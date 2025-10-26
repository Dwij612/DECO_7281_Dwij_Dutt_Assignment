# normalize_culture_groups.py
# Canonicalize lead_culture_group into a fixed set so the model/app don't dump to "Other".
# Input : movies_demographics.csv
# Output: movies_demographics_norm.csv

import pandas as pd
import re

INP = "movies_demographics.csv"
OUT = "movies_demographics_norm.csv"

CANON = {
    "Europe/North America": {
        "europe/north america","europe - north america","ena","europe","north america",
        "usa","united states","american","canada","canadian","mexico","mexican","uk","british","england",
        "france","germany","italy","spain","netherlands","sweden","norway","denmark","belgium","switzerland",
        "austria","portugal","greece","poland","czech","hungary","finland","ireland","scotland","wales"
    },
    "East Asia": {
        "east asia","east-asia","china","chinese","japan","japanese","korea","south korea","north korea","korean",
        "republic of korea","taiwan","hong kong","mongolia"
    },
    "South Asia": {
        "south asia","south-asian","india","indian","pakistan","pakistani","bangladesh","bangladeshi","sri lanka","nepal","bhutan","maldives"
    },
    "SE Asia": {
        "se asia","southeast asia","thailand","thai","vietnam","vietnamese","indonesia","indonesian","malaysia","malaysian",
        "philippines","filipino","cambodia","laos","myanmar","brunei","timor","singapore","singaporean"
    },
    "MENA": {
        "mena","middle east","middle-east","north africa","turkey","turkish","saudi","uae","united arab emirates","iran",
        "iraq","syria","lebanon","jordan","yemen","oman","kuwait","qatar","bahrain","israel","palestine",
        "algeria","morocco","tunisia","libya","egypt","egyptian"
    },
    "Latin America": {
        "latin america","latam","brazil","brazilian","argentina","argentine","chile","chilean","colombia","peru","venezuela",
        "uruguay","paraguay","ecuador","bolivia","guatemala","cuba","dominican","puerto rico","panama",
        "costa rica","el salvador","nicaragua","honduras","latino","latina","latinx"
    },
    "Africa": {
        "africa","nigeria","nigerian","ghana","ghanaian","kenya","kenyan","south africa","south african","egypt","morocco",
        "ethiopia","algeria","tanzania","uganda","tunisia","senegal"
    },
    "Oceania": {
        "oceania","australia","australian","new zealand","new zealander","kiwi","fiji","papua"
    },
    "Other/Unknown": {"other","unknown","", "n/a", "none"}
}

def canonicalize(val: str) -> str:
    if pd.isna(val):
        return "Other/Unknown"
    s = str(val).strip().lower()
    # try exact and substring matches
    for canon, vocab in CANON.items():
        if s in vocab:
            return canon
        for token in vocab:
            if token and token in s:
                return canon
    return "Other/Unknown"

def main():
    df = pd.read_csv(INP)
    if "lead_culture_group" not in df.columns:
        raise SystemExit(f"{INP} missing 'lead_culture_group'.")
    df["lead_culture_group"] = df["lead_culture_group"].apply(canonicalize)
    df.to_csv(OUT, index=False)
    # show a tiny summary so you can verify distribution
    counts = df["lead_culture_group"].value_counts(dropna=False).to_dict()
    print("Saved", OUT, "| distribution:", counts)

if __name__ == "__main__":
    main()
