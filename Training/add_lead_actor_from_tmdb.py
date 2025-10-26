# add_lead_actor_from_tmdb.py
# Add "lead_actor" and "lead_actor_id" to a movie CSV by looking up TMDb credits.
# It can find the TMDb movie via: tmdb_id / tmdb_movie_id / imdb_id / title(+year).
#
# Usage (simple):
#   python3 add_lead_actor_from_tmdb.py
# or specify files:
#   python3 add_lead_actor_from_tmdb.py --input movies_partial_enriched.csv --output movies_with_lead.csv
#
# Requires env var: TMDB_API_KEY (TMDb v3 key)

import os, time, argparse, requests, pandas as pd

# ----------- CONFIG -----------
DEFAULT_INPUTS = ["movies_partial_enriched.csv",
                  "movies_partial.csv",
                  "movies_ml_ready.csv"]
DEFAULT_OUTPUT = "movies_with_lead.csv"
SLEEP_SEC = 0.25
TIMEOUT   = 20
# ------------------------------

API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    raise SystemExit("Set TMDB_API_KEY first (e.g., export TMDB_API_KEY='YOUR_TMDB_V3_KEY').")

BASE = "https://api.themoviedb.org/3"
session = requests.Session()

def tget(path, params=None, retries=3):
    params = dict(params or {})
    params["api_key"] = API_KEY
    for attempt in range(retries):
        r = session.get(f"{BASE}{path}", params=params, timeout=TIMEOUT)
        if r.status_code in (429,) or 500 <= r.status_code < 600:
            time.sleep(SLEEP_SEC * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"TMDb error on {path}")

def search_movie_by_title(title, year=None):
    if not title:
        return None
    params = {"query": title, "include_adult": "false"}
    if pd.notna(year):
        try:
            params["year"] = int(year)
        except Exception:
            pass
    data = tget("/search/movie", params)
    results = data.get("results", [])
    if not results:
        return None
    # prefer exact year match if provided, else highest popularity
    if "year" in params:
        yr_matches = [r for r in results if str(r.get("release_date",""))[:4] == str(params["year"])]
        if yr_matches:
            return sorted(yr_matches, key=lambda x: x.get("popularity", 0), reverse=True)[0]
    return sorted(results, key=lambda x: x.get("popularity", 0), reverse=True)[0]

def tmdb_id_from_imdb(imdb_id):
    if not imdb_id:
        return None
    data = tget(f"/find/{imdb_id}", {"external_source": "imdb_id"})
    mr = data.get("movie_results") or []
    return mr[0].get("id") if mr else None

def pick_input(path_arg: str | None):
    if path_arg:
        return path_arg
    for p in DEFAULT_INPUTS:
        try:
            pd.read_csv(p, nrows=1)
            return p
        except Exception:
            pass
    raise SystemExit("No input CSV found. Put your CSV next to this script (e.g., movies_partial_enriched.csv) "
                     "or pass --input path/to/file.csv")

def pick_top_cast(cast_list):
    if not cast_list:
        return None
    ordered = [c for c in cast_list if c.get("order") is not None]
    if ordered:
        ordered.sort(key=lambda c: c["order"])
        return ordered[0]
    # fallback by popularity
    return sorted(cast_list, key=lambda c: c.get("popularity", 0), reverse=True)[0]

def get_lead_from_tmdb(tmdb_id: int):
    """Return (name, id) of top-billed actor for a TMDb movie id, or (None, None)."""
    try:
        credits = tget(f"/movie/{tmdb_id}/credits")
        cast = credits.get("cast") or []
        star = pick_top_cast(cast)
        if star:
            return (star.get("name"), star.get("id"))
    except Exception:
        pass
    return (None, None)

def resolve_tmdb_for_row(row, df_cols):
    """Try tmdb_id, imdb_id, title(+year) to find a TMDb movie id."""
    # 1) Direct TMDb id
    for key in ("tmdb_id", "tmdb_movie_id"):
        if key in df_cols:
            v = row.get(key)
            if pd.notna(v):
                try:
                    return int(v)
                except Exception:
                    pass

    # 2) IMDb -> TMDb
    if "imdb_id" in df_cols:
        imdb = row.get("imdb_id")
        if pd.notna(imdb):
            try:
                tid = tmdb_id_from_imdb(str(imdb).strip())
                if tid:
                    return int(tid)
            except Exception:
                pass

    # 3) Title (+ optional year)
    title = str(row.get("title") or "").strip() if "title" in df_cols else ""
    year  = row.get("year") if "year" in df_cols else None
    if title:
        hit = search_movie_by_title(title, year)
        if hit and "id" in hit:
            return int(hit["id"])

    return None

def main():
    ap = argparse.ArgumentParser(description="Add lead_actor and lead_actor_id to a movie CSV using TMDb.")
    ap.add_argument("--input",  default=None, help="Input CSV path (default: first existing of common names).")
    ap.add_argument("--output", default=DEFAULT_OUTPUT, help=f"Output CSV path (default: {DEFAULT_OUTPUT})")
    ap.add_argument("--inplace", action="store_true", help="Write back to the input file instead of a new file.")
    args = ap.parse_args()

    in_csv = pick_input(args.input)
    print(f"Reading: {in_csv}")
    df = pd.read_csv(in_csv)

    # Prepare output columns
    if "lead_actor" not in df.columns:
        df["lead_actor"] = None
    if "lead_actor_id" not in df.columns:
        df["lead_actor_id"] = pd.Series(dtype="Int64")

    df_cols = set(df.columns)
    processed = 0
    skips = 0

    for i, row in df.iterrows():
        # Skip if already filled
        if pd.notna(row.get("lead_actor")) and pd.notna(row.get("lead_actor_id")):
            continue

        tmdb_id = resolve_tmdb_for_row(row, df_cols)
        if tmdb_id is None:
            skips += 1
            if i % 25 == 0:
                print(f"[skip] row {i}: no id/title to resolve TMDb id")
            continue

        name, pid = get_lead_from_tmdb(tmdb_id)
        if name:
            df.at[i, "lead_actor"] = name
        if pid:
            df.at[i, "lead_actor_id"] = int(pid)

        processed += 1
        if processed % 25 == 0:
            print(f"Updated {processed} rows...")

        time.sleep(SLEEP_SEC)

    out_path = in_csv if args.inplace else args.output
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Done. Updated rows: {processed}, skipped: {skips}. Wrote: {out_path}")

if __name__ == "__main__":
    main()
