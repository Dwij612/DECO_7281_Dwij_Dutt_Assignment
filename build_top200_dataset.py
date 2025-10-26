# build_top200_dataset.py.py  (you can keep your filename)
# TMDB harvester with FAST_MODE + progress logs + periodic partial saves.
# Replace "API ASDFG" with your real TMDB v3 API key.

import csv
import random
import time
import requests
import sys
from pathlib import Path

# ------------- AUTH (REQUIRED) -------------
API_KEY = "67b5091d6ee52cdd4a2a687b61df1cc5"  # <= PUT YOUR TMDB v3 KEY HERE

# ------------- MODES & TUNING -------------
FAST_MODE = True        # <- start True to confirm it runs; set False for full harvest
POS_THRESHOLD = 1.5     # ROI >= 1.5 => positive (successful)
NEG_THRESHOLD = 0.8     # ROI <= 0.8 => negative (unsuccessful)
SLEEP_BETWEEN_CALLS = 0.15
TIMEOUT = 30
PAGES_PER_YEAR = 80
YEAR_START, YEAR_END = 1950, 2024
DESIRED_PER_CLASS = 1000
LOG_EVERY_PAGES = 5     # print progress every N pages
SAVE_EVERY_ROWS = 250   # write partial CSV every N rows

if FAST_MODE:
    # Trimmed search so you can SEE progress quickly, then expand.
    YEAR_START, YEAR_END = 2015, 2024
    PAGES_PER_YEAR = 10
    DESIRED_PER_CLASS = 150

FULL_OUT = "movies_full.csv"
BAL_OUT = "movies_balanced.csv"
PARTIAL_OUT = "movies_partial.csv"

# ------------- HTTP HELPERS -------------
BASE = "https://api.themoviedb.org/3"
DISCOVER_URL = f"{BASE}/discover/movie"
DETAIL_URL = f"{BASE}/movie"

session = requests.Session()

def tmdb_get(url, params=None, retries=3, backoff=0.75):
    if not API_KEY or API_KEY == "API ASDFG":
        sys.exit("Missing/placeholder API key. Replace 'API ASDFG' with your TMDB v3 key.")
    if params is None:
        params = {}
    params["api_key"] = API_KEY

    for attempt in range(retries):
        r = session.get(url, params=params, timeout=TIMEOUT)
        if r.status_code == 401:
            sys.exit(f"TMDB 401 Unauthorized. Check your API key. Body: {r.text[:200]}")
        if r.status_code in (429,) or 500 <= r.status_code < 600:
            time.sleep(backoff * (2 ** attempt))
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r

def discover_movies(year, page, sort="popularity.asc"):
    params = {
        "language": "en-US",
        "include_adult": "false",
        "include_video": "false",
        "page": page,
        "primary_release_year": year,
        "sort_by": sort,
        "vote_count.gte": 0
    }
    return tmdb_get(DISCOVER_URL, params=params).json().get("results", [])

def movie_detail(movie_id):
    return tmdb_get(f"{DETAIL_URL}/{movie_id}").json()

def classify_roi(roi):
    if roi is None:
        return None
    if roi >= POS_THRESHOLD:
        return "positive"
    if roi <= NEG_THRESHOLD:
        return "negative"
    return None

def credential_sanity_check():
    r = tmdb_get(f"{BASE}/configuration")
    ok = r.status_code == 200 and "images" in r.json()
    if not ok:
        sys.exit("TMDB credential check failed.")
    print("[ok] TMDB credential check passed.", flush=True)

# ------------- SAVE HELPERS -------------
def write_csv(path, rows, header=None):
    if not rows:
        return
    if header is None:
        header = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)

# ------------- HARVEST -------------
def harvest():
    seen_ids = set()
    rows = []
    pos, neg = [], []
    sorts = ["revenue.asc", "popularity.asc", "vote_count.asc", "release_date.asc"]
    last_partial_save = 0

    for year in range(YEAR_END, YEAR_START - 1, -1):
        random.shuffle(sorts)
        for sort in sorts:
            for page in range(1, PAGES_PER_YEAR + 1):
                if page % LOG_EVERY_PAGES == 1:
                    print(f"[scan] year={year} sort={sort} page={page} "
                          f"pos={len(pos)} neg={len(neg)} total={len(rows)}", flush=True)

                try:
                    results = discover_movies(year, page, sort=sort)
                except requests.HTTPError:
                    time.sleep(SLEEP_BETWEEN_CALLS); continue

                if not results:
                    break

                for m in results:
                    mid = m.get("id")
                    if not mid or mid in seen_ids:
                        continue
                    seen_ids.add(mid)

                    try:
                        d = movie_detail(mid)
                    except requests.HTTPError:
                        time.sleep(SLEEP_BETWEEN_CALLS); continue

                    budget = d.get("budget") or 0
                    revenue = d.get("revenue") or 0
                    if budget <= 0 or revenue <= 0:
                        time.sleep(SLEEP_BETWEEN_CALLS); continue

                    roi_val = revenue / float(budget) if budget else None
                    label = classify_roi(roi_val)
                    if label is None:
                        time.sleep(SLEEP_BETWEEN_CALLS); continue

                    row = {
                        "id": mid,
                        "title": d.get("title") or d.get("original_title"),
                        "original_language": d.get("original_language"),
                        "release_date": d.get("release_date"),
                        "budget": budget,
                        "revenue": revenue,
                        "roi": round(roi_val, 4) if roi_val is not None else None,
                        "vote_average": d.get("vote_average"),
                        "vote_count": d.get("vote_count"),
                        "runtime": d.get("runtime"),
                        "success": 1 if label == "positive" else 0
                    }
                    rows.append(row)
                    (pos if label == "positive" else neg).append(row)

                    # Periodic partial save
                    if len(rows) - last_partial_save >= SAVE_EVERY_ROWS:
                        write_csv(PARTIAL_OUT, rows)
                        last_partial_save = len(rows)
                        print(f"[save] Wrote partial {PARTIAL_OUT} ({len(rows)} rows).", flush=True)

                    # Early exit if target reached
                    if len(pos) >= DESIRED_PER_CLASS and len(neg) >= DESIRED_PER_CLASS:
                        return rows, pos, neg

                    time.sleep(SLEEP_BETWEEN_CALLS)

    return rows, pos, neg

# ------------- MAIN -------------
def main():
    credential_sanity_check()
    print(f"[info] Harvesting {YEAR_START}-{YEAR_END}, {PAGES_PER_YEAR} pages/year…", flush=True)
    rows, pos, neg = harvest()
    print(f"[info] Harvested total={len(rows)} | pos={len(pos)} | neg={len(neg)}", flush=True)

    if not rows:
        print("[warn] No rows harvested. Try widening years/pages.", flush=True)
        return

    header = list(rows[0].keys())
    write_csv(FULL_OUT, rows, header)
    print(f"[done] Wrote {FULL_OUT} with {len(rows)} rows.", flush=True)

    k = min(len(pos), len(neg), DESIRED_PER_CLASS)
    if k == 0:
        print("[warn] Could not create a balanced sample (one class empty).", flush=True)
        return
    random.shuffle(pos); random.shuffle(neg)
    balanced = pos[:k] + neg[:k]
    random.shuffle(balanced)
    write_csv(BAL_OUT, balanced, header)
    print(f"[done] Wrote {BAL_OUT} with {len(balanced)} rows (k={k} per class).", flush=True)

if __name__ == "__main__":
    main()
