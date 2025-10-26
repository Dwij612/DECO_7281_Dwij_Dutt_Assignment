# implement_rules_with_star_power.py
# Build clean, non-leaky features with binary franchise, robust year split, and train-only star/director aggregates.
# Input : movies_partial_enriched.csv
# Output: movies_ml_ready.csv  (features + success_label)

import pandas as pd
import numpy as np

# ------------- CONFIG -------------
INPUT_CSV   = "movies_partial_enriched.csv"
OUTPUT_CSV  = "movies_ml_ready.csv"
CUTOFF_YEAR = 2019          # target split: train < 2019, test >= 2019
BUDGET_LOW  = 30_000_000    # Low < 30M
BUDGET_HIGH = 80_000_000    # High > 80M (else Mid)
RARE_MIN    = 30            # fold rare categories in TRAIN to "Other"
SEED        = 42
# ----------------------------------

def to_int(x):
    try:
        if pd.isna(x): return None
        return int(float(x))
    except Exception:
        return None

def decade(y):
    y = to_int(y)
    if not y or y < 1900: return "Unknown"
    return f"{(y//10)*10}s"

def budget_band(b):
    b = to_int(b) or 0
    if b <= 0:          return "Unknown"
    if b < BUDGET_LOW:  return "Low"
    if b > BUDGET_HIGH: return "High"
    return "Mid"

def language_group(lang):
    if pd.isna(lang): return "Unknown"
    return "English" if str(lang).lower() == "en" else "Non-English"

def fold_rare(series, min_count):
    vc = series.value_counts(dropna=False)
    keep = vc[vc >= min_count].index
    return series.where(series.isin(keep), other="Other")

def franchise_to_binary(val):
    if pd.isna(val): return 0
    s = str(val).strip().lower()
    return 1 if s in {"yes","y","true","1","t"} else 0

def choose_gross(row):
    # Prefer BOM worldwide gross; else TMDb revenue; else generic 'revenue'/'gross'
    for col in ["worldwide_gross_usd", "tmdb_revenue_usd", "revenue", "gross"]:
        if col in row and not pd.isna(row[col]):
            v = to_int(row[col])
            if v and v > 0: return v
    return None

def choose_budget(row):
    for col in ["budget_usd", "budget"]:
        if col in row and not pd.isna(row[col]):
            v = to_int(row[col])
            if v and v > 0: return v
    return None

def derive_year_cols(df):
    """Ensure we have a numeric 'year' column. If missing, try from release_date."""
    if "year" not in df.columns or df["year"].isna().all():
        if "release_date" in df.columns:
            y = pd.to_numeric(df["release_date"].astype(str).str[:4], errors="coerce")
            df["year"] = y
        else:
            df["year"] = np.nan
    else:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
    return df

def build_features(df):
    # 1) Label (if missing): success = ROI >= 2.0
    if "success_label" not in df.columns:
        df["gross_for_roi"]  = df.apply(choose_gross, axis=1)
        df["budget_for_roi"] = df.apply(choose_budget, axis=1)
        df["roi"] = df.apply(lambda r: (r["gross_for_roi"]/r["budget_for_roi"])
                             if (r["gross_for_roi"] and r["budget_for_roi"] and r["budget_for_roi"]>0) else np.nan, axis=1)
        df["success_label"] = (df["roi"] >= 2.0).astype("Int64")
    df["success_label"] = df["success_label"].astype(int)

    # 2) Core features (general only)
    if "genre_primary" in df.columns and df["genre_primary"].notna().any():
        genre_primary = df["genre_primary"].fillna("Unknown").astype(str)
    else:
        raw_genres = df["genres"].fillna("") if "genres" in df.columns else pd.Series([""]*len(df))
        genre_primary = raw_genres.apply(lambda s: s.split("|")[0] if isinstance(s, str) and s else "Unknown")

    orig_lang = df["original_language"] if "original_language" in df.columns else pd.Series(["Unknown"]*len(df))
    vote_avg  = pd.to_numeric(df["vote_average"], errors="coerce") if "vote_average" in df.columns else pd.Series([np.nan]*len(df))
    budget    = pd.to_numeric(df["budget_usd"], errors="coerce") if "budget_usd" in df.columns else (
                pd.to_numeric(df["budget"], errors="coerce") if "budget" in df.columns else pd.Series([np.nan]*len(df)))
    franchise_raw = df["franchise"] if "franchise" in df.columns else pd.Series(["No"]*len(df))
    franchise_bin = franchise_raw.apply(franchise_to_binary).astype(int)

    feats = pd.DataFrame({
        "year": df["year"],
        "genre_primary": genre_primary,
        "original_language": orig_lang,
        "language_group": orig_lang.apply(language_group),
        "vote_average": vote_avg,
        "budget_usd": budget,
        "budget_band": budget.apply(budget_band),
        "decade": df["year"].apply(decade),
        "franchise": franchise_bin,   # <-- strictly 0/1
        # carry for aggregates (will be removed from final save):
        "lead_actor": df["lead_actor"] if "lead_actor" in df.columns else pd.Series([None]*len(df)),
        "director": df["director"] if "director" in df.columns else pd.Series([None]*len(df)),
        "success_label": df["success_label"].astype(int)
    })

    return feats

def add_train_only_aggregates(feats):
    """Compute actor/director aggregates on TRAIN ONLY, then map to all rows (unknowns → global train mean)."""
    yr = pd.to_numeric(feats["year"], errors="coerce").fillna(0).astype(int)
    train_mask = yr < _effective_cutoff(yr)  # temp; will be recomputed in make_split

    train = feats.loc[train_mask].copy()
    global_mean = train["success_label"].mean() if len(train) else 0.5

    # Normalise strings
    train["lead_actor"] = train["lead_actor"].astype(str).replace({"nan": None})
    train["director"]   = train["director"].astype(str).replace({"nan": None})

    # Aggregates
    actor_grp = train.dropna(subset=["lead_actor"]).groupby("lead_actor")["success_label"]
    dir_grp   = train.dropna(subset=["director"]).groupby("director")["success_label"]

    actor_sr = actor_grp.mean().rename("actor_success_rate_train")
    actor_ct = actor_grp.size().rename("actor_appearances_train")
    dir_sr   = dir_grp.mean().rename("director_success_rate_train")
    dir_ct   = dir_grp.size().rename("director_appearances_train")

    def map_with_default(series, stats, default):
        out = series.astype(str).replace({"nan": None}).map(stats)
        return out.fillna(default)

    feats["actor_success_rate_train"]    = map_with_default(feats["lead_actor"], actor_sr, global_mean)
    feats["actor_appearances_train"]     = map_with_default(feats["lead_actor"], actor_ct, 0).astype(int)
    feats["director_success_rate_train"] = map_with_default(feats["director"],   dir_sr,  global_mean)
    feats["director_appearances_train"]  = map_with_default(feats["director"],   dir_ct,  0).astype(int)

    feats["actor_appearances_log1p"]     = np.log1p(feats["actor_appearances_train"])
    feats["director_appearances_log1p"]  = np.log1p(feats["director_appearances_train"])
    return feats

def _effective_cutoff(yr_series):
    """Pick a cutoff that guarantees a non-empty test set if CUTOFF_YEAR fails."""
    valid_years = yr_series.replace(0, np.nan).dropna()
    if valid_years.empty:
        return CUTOFF_YEAR
    return CUTOFF_YEAR

def make_split(features, y):
    """Time-based split with auto-adjust + random fallback."""
    yr = pd.to_numeric(features["year"], errors="coerce")

    # If year missing, try release_date year (handled earlier) — just ensure we have something numeric
    valid_years = yr.dropna()
    cutoff_used = CUTOFF_YEAR

    # First attempt: configured cutoff
    train_mask = (yr.fillna(0).astype(int) < cutoff_used)
    test_mask  = ~train_mask

    # If either side empty → pick 80th percentile year as dynamic cutoff
    if train_mask.sum() == 0 or test_mask.sum() == 0:
        if not valid_years.empty:
            dyn = int(np.nanpercentile(valid_years, 80))
            cutoff_used = dyn
            train_mask = (yr.fillna(0).astype(int) < cutoff_used)
            test_mask  = ~train_mask

    X_train = features.loc[train_mask].copy()
    y_train = y.loc[train_mask].copy()
    X_test  = features.loc[test_mask].copy()
    y_test  = y.loc[test_mask].copy()

    # If still empty → fall back to stratified random split
    if len(X_train) == 0 or len(X_test) == 0:
        from sklearn.model_selection import train_test_split
        print("[warn] Time split empty; falling back to stratified random 80/20.")
        X_train, X_test, y_train, y_test = train_test_split(
            features, y, test_size=0.2, random_state=SEED, stratify=y
        )
        cutoff_used = None  # indicates random fallback

    print(f"Train size: {len(X_train)} | Test size: {len(X_test)} | Cutoff used: {cutoff_used}")
    return X_train, X_test, y_train, y_test, cutoff_used

def main():
    df_raw = pd.read_csv(INPUT_CSV)
    df_raw = derive_year_cols(df_raw)

    feats = build_features(df_raw)
    feats = add_train_only_aggregates(feats)

    # Final feature set (no titles/names/gross/revenue)
    cat_cols = ["genre_primary","budget_band","decade","language_group"]
    num_cols = ["vote_average","franchise",  # franchise is strictly 0/1 numeric
                "actor_success_rate_train","director_success_rate_train",
                "actor_appearances_log1p","director_appearances_log1p","year"]

    # Save ML-ready CSV (features + label) — this is what you'll train your model on
    out = pd.concat([feats[cat_cols + num_cols], feats["success_label"]], axis=1)
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Saved {OUTPUT_CSV} with {len(out)} rows.")

    # Optional baseline if scikit-learn is available
    try:
        from sklearn.preprocessing import OneHotEncoder, StandardScaler
        from sklearn.compose import ColumnTransformer
        from sklearn.pipeline import Pipeline
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

        # Split with robust logic
        X_train, X_test, y_train, y_test, _ = make_split(out.drop(columns=["success_label"]), out["success_label"])

        # Fold rare categories on TRAIN ONLY
        for c in cat_cols:
            X_train[c] = fold_rare(X_train[c].astype(str).fillna("Unknown"), RARE_MIN)
            seen = set(X_train[c].unique())
            X_test[c]  = X_test[c].astype(str).fillna("Unknown").apply(lambda v: v if v in seen else "Other")

        # Numeric NaNs (use train medians)
        for c in [nc for nc in num_cols if nc != "franchise"]:
            med = pd.to_numeric(X_train[c], errors="coerce").median()
            X_train[c] = pd.to_numeric(X_train[c], errors="coerce").fillna(med)
            X_test[c]  = pd.to_numeric(X_test[c], errors="coerce").fillna(med)
        # Franchise is 0/1 already; fill missing with 0
        X_train["franchise"] = pd.to_numeric(X_train["franchise"], errors="coerce").fillna(0).astype(int)
        X_test["franchise"]  = pd.to_numeric(X_test["franchise"], errors="coerce").fillna(0).astype(int)

        pre = ColumnTransformer(
            transformers=[
                ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
                ("num", StandardScaler(with_mean=False), [c for c in num_cols]),  # with_mean=False for sparse safety
            ],
            remainder="drop",
        )

        pipe = Pipeline([
            ("pre", pre),
            ("clf", LogisticRegression(max_iter=1000, random_state=SEED))
        ])

        pipe.fit(X_train, y_train)
        y_pred = pipe.predict(X_test)

        print("\n=== BASELINE (LogReg) ===")
        print("Accuracy:", round(accuracy_score(y_test, y_pred), 3))
        print("Confusion matrix:\n", confusion_matrix(y_test, y_pred))
        print("Report:\n", classification_report(y_test, y_pred, digits=3))

    except ImportError:
        print("[info] scikit-learn not installed; skipping baseline. (pip install scikit-learn)")

if __name__ == "__main__":
    main()
