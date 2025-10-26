# enrich_partial_tmdb.py
# Enriches movies_partial.csv with franchise (collection), director(s), and top-5 cast.
# Output: movies_partial_enriched.csv (original stays unchanged)

import csv, json, sys, time, requests
from pathlib import Path

# -------- AUTH (REQUIRED) --------
API_KEY = "67b5091d6ee52cdd4a2a687b61df1cc5"  # <= PUT YOUR TMDB v3 KEY HERE

# -------- FILES --------
IN_CSV  = "movies_partial.csv"
OUT_CSV = "movies_partial_enriched.csv"
CACHE_DIR = Path("cache_tmdb"); CACHE_DIR.mkdir(exist_ok=True)

# -------- TMDB --------
BASE = "https://api.themoviedb.org/3"
DETAIL = f"{BASE}/movie"  # /{id}?append_to_response=credits
SLEEP = 0.12
TIMEOUT = 25
session = requests.Session()

def tmdb_get_movie_with_credits(mid: int):
    cache_path = CACHE_DIR / f"{mid}.json"
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    params = {"api_key": API_KEY, "language": "en-US", "append_to_response": "credits"}
    url = f"{DETAIL}/{mid}"
    for attempt in range(4):
        r = session.get(url, params=params, timeout=TIMEOUT)
        if r.status_code == 401:
            sys.exit("TMDB 401 Unauthorized. Replace 'API ASDFG' with your real v3 key.")
        if r.status_code in (429,) or 500 <= r.status_code < 600:
            time.sleep(0.7 * (2 ** attempt)); continue
        r.raise_for_status()
        data = r.json()
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(data, f)
        return data
    r.raise_for_status()

def extract_franchise(d):
    c = d.get("belongs_to_collection")
    return c.get("name") if isinstance(c, dict) else ""

def extract_directors(d):
    crew = (d.get("credits") or {}).get("crew") or []
    directors = [p.get("name") for p in crew if p.get("job") == "Director" and p.get("name")]
    return (directors[0] if directors else ""), directors

def extract_top_cast(d, k=5):
    cast = (d.get("credits") or {}).get("cast") or []
    cast_sorted = sorted([c for c in cast if c.get("name")], key=lambda c: c.get("order", 9999))
    names = [c["name"] for c in cast_sorted[:k]]
    names += [""] * (k - len(names))
    return names

def read_csv_rows(path):
    if not Path(path).exists():
        sys.exit(f"Input not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv_rows(path, rows, header):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def dedup_by_id(rows):
    seen, uniq = set(), []
    for r in rows:
        mid = r.get("id")
        try:
            mid_int = int(str(mid).strip())
        except Exception:
            uniq.append(r); continue
        if mid_int in seen: 
            continue
        seen.add(mid_int)
        uniq.append(r)
    return uniq

def main():
    if not API_KEY or API_KEY == "API ASDFG":
        sys.exit("Missing TMDB key. Replace 'API ASDFG' with your v3 key in the script.")

    rows = read_csv_rows(IN_CSV)
    print(f"[info] Input rows: {len(rows)}", flush=True)
    rows = dedup_by_id(rows)
    print(f"[info] Unique by id: {len(rows)}", flush=True)

    # Add new columns (keeps original order first)
    base_fields = list(rows[0].keys())
    extra = ["franchise","director_primary","directors_all",
             "cast_top_1","cast_top_2","cast_top_3","cast_top_4","cast_top_5"]
    header = base_fields + [c for c in extra if c not in base_fields]

    total = len(rows)
    for i, r in enumerate(rows, 1):
        mid = r.get("id")
        try:
            mid_int = int(str(mid).strip())
        except Exception:
            for k in extra: r.setdefault(k, "")
            continue

        try:
            data = tmdb_get_movie_with_credits(mid_int)
        except requests.HTTPError:
            for k in extra: r.setdefault(k, "")
            time.sleep(SLEEP); continue

        r["franchise"] = extract_franchise(data)
        primary, directors = extract_directors(data)
        r["director_primary"] = primary
        r["directors_all"] = "; ".join(directors) if directors else ""
        c1,c2,c3,c4,c5 = extract_top_cast(data, 5)
        r["cast_top_1"], r["cast_top_2"], r["cast_top_3"], r["cast_top_4"], r["cast_top_5"] = c1,c2,c3,c4,c5

        if i % 25 == 0 or i == total:
            print(f"[enrich:partial] {i}/{total}  dir='{r['director_primary']}'  franchise='{r['franchise']}'", flush=True)
        time.sleep(SLEEP)

    write_csv_rows(OUT_CSV, rows, header)
    print(f"[done] Wrote {OUT_CSV} with {len(rows)} rows.", flush=True)

if __name__ == "__main__":
    main()
