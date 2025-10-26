# topup_tmdb_dataset.py
# Read existing CSV, then harvest more (biasing toward negatives) until both classes reach target.
# Replace the API key placeholder below with your TMDB v3 key.

import csv, sys, time, random, requests, os
from pathlib import Path

# ----------------- AUTH (REQUIRED) -----------------
API_KEY = "67b5091d6ee52cdd4a2a687b61df1cc5"   # <= PUT YOUR TMDB v3 KEY HERE

# ----------------- INPUT / OUTPUT ------------------
EXISTING_FULL = "movies_full.csv"        # your current file (55 rows)
OUT_FULL      = "movies_full_topup.csv"  # merged output grows as we harvest
OUT_BAL       = "movies_balanced.csv"    # rewritten after top-up
PARTIAL_OUT   = "movies_partial.csv"     # periodic save while harvesting

# ----------------- TARGET + BIAS -------------------
TARGET_PER_CLASS   = 1000     # stop when pos >= and neg >= this number
POS_THRESHOLD      = 2.0      # successful if ROI >= 2.0 (stricter positives)
NEG_THRESHOLD      = 0.9      # unsuccessful if ROI <= 0.9 (more negatives)
NEGATIVE_HUNT_ONLY = False    # set True if you ONLY want to top up negatives

# Crawl space (go wide!)
YEAR_START, YEAR_END = 1970, 2024
PAGES_PER_YEAR      = 120     # can go up to 500 per TMDB per query
SLEEP_BETWEEN_CALLS = 0.15
TIMEOUT             = 25
LOG_EVERY_PAGES     = 5
SAVE_EVERY_ROWS     = 250

# Discovery sorts; start with revenue.asc to surface likely flops
SORTS = ["revenue.asc", "vote_count.asc", "popularity.asc", "release_date.asc"]

# ----------------- TMDB HELPERS --------------------
BASE = "https://api.themoviedb.org/3"
DISCOVER = f"{BASE}/discover/movie"
DETAIL   = f"{BASE}/movie"
session  = requests.Session()

def tmdb_get(url, params=None, retries=3, backoff=0.75):
    if not API_KEY or API_KEY == "API ASDFG":
        sys.exit("Missing TMDB key: replace 'API ASDFG' with your v3 key.")
    if params is None:
        params = {}
    params["api_key"] = API_KEY
    for attempt in range(retries):
        r = session.get(url, params=params, timeout=TIMEOUT)
        if r.status_code == 401:
            sys.exit(f"TMDB 401 Unauthorized. Check your key. Body: {r.text[:200]}")
        if r.status_code in (429,) or 500 <= r.status_code < 600:
            time.sleep(backoff * (2 ** attempt)); continue
        r.raise_for_status()
        return r
    r.raise_for_status(); return r

def discover_movies(year, page, sort):
    params = {
        "language": "en-US",
        "include_adult": "false",
        "include_video": "false",
        "primary_release_year": year,
        "page": page,
        "sort_by": sort,
        "vote_count.gte": 0
    }
    return tmdb_get(DISCOVER, params=params).json().get("results", [])

def movie_detail(movie_id):
    return tmdb_get(f"{DETAIL}/{movie_id}").json()

def classify_roi(roi):
    if roi is None: return None
    if roi >= POS_THRESHOLD: return "positive"
    if roi <= NEG_THRESHOLD: return "negative"
    return None

# ----------------- CSV HELPERS ---------------------
def read_existing(path):
    if not Path(path).exists(): return [], set()
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = [row for row in r]
    seen = {int(row["id"]) for row in rows if row.get("id")}
    def to_int(x):
        try: return int(x)
        except: return 0
    return rows, seen

def write_csv(path, rows, header=None):
    if not rows: return
    if header is None: header = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow(row)

# ----------------- MAIN HARVEST --------------------
def main():
    # Credential sanity
    tmdb_get(f"{BASE}/configuration")
    print("[ok] TMDB credential check passed.", flush=True)

    # Load existing
    full_rows, seen_ids = read_existing(EXISTING_FULL)
    if not full_rows:
        print(f"[info] No {EXISTING_FULL} found; starting fresh.", flush=True)

    # Count classes
    pos = [r for r in full_rows if str(r.get("success")) == "1"]
    neg = [r for r in full_rows if str(r.get("success")) == "0"]
    print(f"[info] Starting with pos={len(pos)} neg={len(neg)} total={len(full_rows)}", flush=True)

    # Harvest until both classes reach target
    rows = list(full_rows)
    last_partial = len(rows)

    for year in range(YEAR_END, YEAR_START - 1, -1):
        sorts = list(SORTS); random.shuffle(sorts)
        # Keep revenue.asc first to bias for negatives
        if "revenue.asc" in sorts:
            sorts.remove("revenue.asc")
        sorts = ["revenue.asc"] + sorts

        for sort in sorts:
            for page in range(1, PAGES_PER_YEAR + 1):
                if page % LOG_EVERY_PAGES == 1:
                    print(f"[scan] y={year} sort={sort} p={page} pos={len(pos)} neg={len(neg)} total={len(rows)}", flush=True)

                # Early-stop if both targets met
                if len(pos) >= TARGET_PER_CLASS and len(neg) >= TARGET_PER_CLASS:
                    return finish(rows, pos, neg)

                # If only topping up negatives and already enough negatives, skip crawl
                if NEGATIVE_HUNT_ONLY and len(neg) >= TARGET_PER_CLASS and len(pos) >= TARGET_PER_CLASS:
                    return finish(rows, pos, neg)

                try:
                    batch = discover_movies(year, page, sort)
                except requests.HTTPError:
                    time.sleep(SLEEP_BETWEEN_CALLS); continue
                if not batch: break

                for m in batch:
                    mid = m.get("id")
                    if not mid: continue
                    mid = int(mid)
                    if mid in seen_ids: continue

                    try:
                        d = movie_detail(mid)
                    except requests.HTTPError:
                        time.sleep(SLEEP_BETWEEN_CALLS); continue

                    budget  = d.get("budget") or 0
                    revenue = d.get("revenue") or 0
                    if budget <= 0 or revenue <= 0:
                        time.sleep(SLEEP_BETWEEN_CALLS); continue

                    roi = (revenue / float(budget)) if budget else None
                    label = classify_roi(roi)
                    if label is None:
                        time.sleep(SLEEP_BETWEEN_CALLS); continue

                    # If we're hunting negatives and this is positive but we already have many positives, you can skip:
                    if NEGATIVE_HUNT_ONLY and label == "positive" and len(pos) >= TARGET_PER_CLASS:
                        time.sleep(SLEEP_BETWEEN_CALLS); continue

                    row = {
                        "id": mid,
                        "title": d.get("title") or d.get("original_title"),
                        "original_language": d.get("original_language"),
                        "release_date": d.get("release_date"),
                        "budget": budget,
                        "revenue": revenue,
                        "roi": round(roi, 4) if roi is not None else None,
                        "vote_average": d.get("vote_average"),
                        "vote_count": d.get("vote_count"),
                        "runtime": d.get("runtime"),
                        "success": 1 if label == "positive" else 0
                    }

                    rows.append(row)
                    seen_ids.add(mid)
                    if label == "positive": pos.append(row)
                    else: neg.append(row)

                    # Periodic partial save
                    if len(rows) - last_partial >= SAVE_EVERY_ROWS:
                        write_csv(PARTIAL_OUT, rows)
                        last_partial = len(rows)
                        print(f"[save] {PARTIAL_OUT} ({len(rows)} rows).", flush=True)

                    time.sleep(SLEEP_BETWEEN_CALLS)

    # Out of crawl space
    return finish(rows, pos, neg)

def finish(rows, pos, neg):
    print(f"[info] Final counts pos={len(pos)} neg={len(neg)} total={len(rows)}", flush=True)

    # Write expanded full
    header = list(rows[0].keys()) if rows else ["id","title","original_language","release_date","budget","revenue","roi","vote_average","vote_count","runtime","success"]
    write_csv(OUT_FULL, rows, header)
    print(f"[done] Wrote {OUT_FULL} with {len(rows)} rows.", flush=True)

    # Balanced sample
    k = min(len(pos), len(neg), TARGET_PER_CLASS)
    if k > 0:
        random.shuffle(pos); random.shuffle(neg)
        balanced = pos[:k] + neg[:k]
        random.shuffle(balanced)
        write_csv(OUT_BAL, balanced, header)
        print(f"[done] Wrote {OUT_BAL} with {len(balanced)} rows (k={k} per class).", flush=True)
    else:
        print("[warn] Could not create a balanced sample (one class empty).", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())
