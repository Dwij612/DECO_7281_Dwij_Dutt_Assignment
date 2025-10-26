# make_demographics_from_tmdb.py
# Build demographics columns from TMDb for the lead actor:
# - lead_gender        -> Male / Female / Unknown (TMDb gender field)
# - lead_culture_group -> coarse region from place_of_birth (very rough proxy)
#
# Input  : movies_partial_enriched.csv  (must contain 'lead_actor' column)
# Output : movies_demographics.csv

import os
import time
import json
import requests
import pandas as pd

# ---- CONFIG ----
CANDIDATE_INPUTS = [
    "movies_partial_enriched.csv",
    "movies_balanced.csv",
    "movies_ml_ready.csv",
]
INPUT_CSV = None           # auto-picked below
OUTPUT_CSV = "movies_demographics.csv"
SLEEP_SEC = 0.25           # TMDb polite rate-limit
TIMEOUT   = 20
# ---------------

API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    raise SystemExit("Set TMDB_API_KEY first (e.g., export TMDB_API_KEY='YOUR_TMDB_V3_KEY').")

BASE = "https://api.themoviedb.org/3"
SESSION = requests.Session()

def pick_input():
    for p in CANDIDATE_INPUTS:
        try:
            pd.read_csv(p, nrows=1)
            return p
        except Exception:
            pass
    raise SystemExit("No input CSV found. Put your CSV next to this script and name it one of: "
                     + ", ".join(CANDIDATE_INPUTS))

def tmdb_get(path, params=None, retries=3):
    params = dict(params or {})
    params["api_key"] = API_KEY
    for attempt in range(retries):
        r = SESSION.get(f"{BASE}{path}", params=params, timeout=TIMEOUT)
        if r.status_code in (429,) or 500 <= r.status_code < 600:
            time.sleep(SLEEP_SEC * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"TMDb error on {path} after {retries} tries.")

def search_person(name: str):
    if not name:
        return None
    data = tmdb_get("/search/person", {"query": name, "include_adult": "false"})
    res = data.get("results", [])
    return res[0] if res else None

def person_details(pid: int):
    return tmdb_get(f"/person/{pid}")

def pick_gender(val):
    # TMDb: 0 Unknown, 1 Female, 2 Male, 3 Non-binary (rare)
    return {1: "Female", 2: "Male", 3: "Non-binary"}.get(val, "Unknown")

def country_to_region(place_of_birth: str) -> str:
    if not place_of_birth:
        return "Other/Unknown"
    pob = place_of_birth.lower()

    buckets = [
        ("Europe/North America", ["united states", "usa", "canada", "mexico", "england", "scotland", "wales",
                                  "ireland", "united kingdom", "uk", "france", "germany", "italy", "spain",
                                  "netherlands", "sweden", "norway", "denmark", "belgium", "switzerland",
                                  "austria", "portugal", "greece", "poland", "czech", "hungary", "finland"]),
        ("Latin America", ["brazil", "argentina", "chile", "colombia", "peru", "venezuela", "uruguay",
                           "paraguay", "ecuador", "bolivia", "guatemala", "cuba", "dominican", "puerto rico",
                           "panama", "costa rica", "el salvador", "nicaragua", "honduras"]),
        ("Africa", ["nigeria", "south africa", "egypt", "morocco", "ethiopia", "kenya", "ghana", "algeria",
                    "tanzania", "uganda", "tunisia", "senegal"]),
        ("East Asia", ["china", "japan", "korea", "south korea", "republic of korea", "north korea",
                       "taiwan", "hong kong", "mongolia"]),
        ("South Asia", ["india", "pakistan", "bangladesh", "sri lanka", "nepal", "bhutan", "maldives"]),
        ("SE Asia", ["singapore", "malaysia", "indonesia", "philippines", "thailand", "vietnam",
                     "cambodia", "laos", "myanmar", "brunei", "timor"]),
        ("MENA", ["turkey", "saudi", "united arab emirates", "uae", "iran", "iraq", "syria", "lebanon",
                  "jordan", "yemen", "oman", "kuwait", "qatar", "bahrain", "israel", "palestine", "algeria",
                  "morocco", "tunisia", "libya", "egypt"]),
        ("Oceania", ["australia", "new zealand", "fiji", "papua"]),
    ]
    for region, keys in buckets:
        if any(k in pob for k in keys):
            return region
    return "Other/Unknown"

def main():
    global INPUT_CSV
    INPUT_CSV = pick_input()
    print(f"Reading: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)
    if "lead_actor" not in df.columns:
        raise SystemExit(f"{INPUT_CSV} must contain a 'lead_actor' column.")

    # Prepare output columns
    if "lead_gender" not in df.columns:
        df["lead_gender"] = "Unknown"
    if "lead_culture_group" not in df.columns:
        df["lead_culture_group"] = "Other/Unknown"

    # Cache to avoid repeated lookups of same person
    cache = {}

    for i, row in df.iterrows():
        name = str(row.get("lead_actor") or "").strip()
        if not name:
            continue

        if name in cache:
            g, cg = cache[name]
            df.at[i, "lead_gender"] = g
            df.at[i, "lead_culture_group"] = cg
            continue

        try:
            found = search_person(name)
            if not found:
                cache[name] = ("Unknown", "Other/Unknown")
                continue

            details = person_details(found["id"])
            gender = pick_gender(details.get("gender", 0))
            pob = details.get("place_of_birth") or ""
            culture = country_to_region(pob)

            df.at[i, "lead_gender"] = gender
            df.at[i, "lead_culture_group"] = culture
            cache[name] = (gender, culture)

            # be nice to their servers
            time.sleep(SLEEP_SEC)

            if i % 25 == 0 and i > 0:
                print(f"Processed {i} rows...")

        except requests.HTTPError as e:
            print(f"[HTTP] row {i} name={name}: {e}")
            time.sleep(1)
        except Exception as e:
            print(f"[WARN] row {i} name={name}: {e}")

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Wrote {OUTPUT_CSV} with {len(df)} rows.")

if __name__ == "__main__":
    main()
