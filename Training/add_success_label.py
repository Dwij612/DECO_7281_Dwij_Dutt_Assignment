# add_success_label.py
# Adds a binary success_label to movies_demographics.csv.
# 1) Tries to MERGE labels from movies_ml_ready.csv (tmdb_id | imdb_id | title+year).
# 2) If not found, COMPUTES label from budget & revenue (ROI >= 2 -> 1 else 0).
#
# Usage:
#   python3 add_success_label.py
#   # or specify paths:
#   python3 add_success_label.py --demo movies_demographics.csv --source movies_ml_ready.csv --out movies_demographics.csv

import re, argparse, os
import pandas as pd

def to_num(x):
    if pd.isna(x): return None
    s = str(x)
    s = re.sub(r'[^0-9.\-]', '', s)  # drop $ , etc.
    try:
        return float(s)
    except Exception:
        return None

def pick_key_cols(df):
    keys = []
    if "tmdb_id" in df.columns: keys.append("tmdb_id")
    if "imdb_id" in df.columns: keys.append("imdb_id")
    if "title" in df.columns and "year" in df.columns: keys.append(("title","year"))
    return keys

def merge_labels(demo, source):
    # try tmdb_id, then imdb_id, then (title,year)
    merged = demo.copy()
    keys_demo = pick_key_cols(demo)
    keys_src  = pick_key_cols(source)

    def try_key(k):
        nonlocal merged
        if isinstance(k, tuple):
            cols = list(k)
            if all(c in demo.columns for c in cols) and all(c in source.columns for c in cols):
                m = merged.merge(source[cols+["success_label"]].drop_duplicates(cols), on=cols, how="left")
                return m
        else:
            if k in demo.columns and k in source.columns:
                m = merged.merge(source[[k,"success_label"]].drop_duplicates([k]), on=k, how="left")
                return m
        return None

    for k in ["tmdb_id", "imdb_id", ("title","year")]:
        if (k in keys_demo) or (k in keys_src) or (isinstance(k,tuple) and (("title" in keys_demo and "year" in keys_demo) and ("title" in keys_src and "year" in keys_src))):
            m = try_key(k)
            if m is not None:
                merged = m
                if "success_label" in merged.columns and merged["success_label"].notna().any():
                    # got some labels; fill only missing later if needed
                    break

    return merged

def compute_labels(df):
    # find budget & revenue-like columns
    cand_rev = [c for c in df.columns if c.lower() in ["worldwide_gross","worldwide","revenue","gross_worldwide","worldwidegross"]]
    cand_bug = [c for c in df.columns if c.lower() in ["budget","production_budget"]]
    if not cand_rev or not cand_bug:
        return df  # nothing we can compute
    rev_col = cand_rev[0]
    bud_col = cand_bug[0]
    rev = df[rev_col].apply(to_num)
    bud = df[bud_col].apply(to_num)
    roi = []
    for r, b in zip(rev, bud):
        if (r is None) or (b is None) or b <= 0:
            roi.append(None)
        else:
            roi.append(r / b)
    df = df.copy()
    df["roi"] = roi
    df["success_label"] = df["success_label"] if "success_label" in df.columns else None
    df.loc[df["success_label"].isna() & pd.notna(df["roi"]), "success_label"] = (df["roi"] >= 2.0).astype(int)
    df.drop(columns=["roi"], inplace=True)
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo",   default="movies_demographics.csv", help="Demographics CSV to label")
    ap.add_argument("--source", default="movies_ml_ready.csv",     help="Source CSV that may already have success_label")
    ap.add_argument("--out",    default="movies_demographics.csv", help="Output CSV path")
    args = ap.parse_args()

    demo = pd.read_csv(args.demo)
    if os.path.exists(args.source):
        src = pd.read_csv(args.source)
    else:
        src = pd.DataFrame()

    if "success_label" not in demo.columns or demo["success_label"].isna().any():
        if not src.empty and "success_label" in src.columns:
            merged = merge_labels(demo, src)
        else:
            merged = demo.copy()
        # compute for any still-missing rows (needs budget+revenue)
        merged = compute_labels(merged)

        if "success_label" not in merged.columns:
            raise SystemExit("Could not create success_label: no merge match and no usable budget/revenue columns.")
        if merged["success_label"].isna().any():
            # fill any leftover as 0 to keep training simple (you can change this)
            merged["success_label"] = merged["success_label"].fillna(0).astype(int)
    else:
        merged = demo

    merged.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with success_label. Rows: {len(merged)}")

if __name__ == "__main__":
    main()
