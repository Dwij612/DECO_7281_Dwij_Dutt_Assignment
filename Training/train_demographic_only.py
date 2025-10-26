# train_demographic_only.py
# Train a model using ONLY lead_gender and lead_culture_group, then save serve_model.joblib

import sys
import joblib
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

INPUT = "movies_demographics_norm.csv"   # make sure this file exists from the previous step
MODEL_OUT = "serve_model.joblib"

try:
    df = pd.read_csv(INPUT)
except Exception:
    sys.exit(f"Missing {INPUT}. Run make_demographics_from_tmdb.py first (or create it with those columns).")

for c in ["lead_gender", "lead_culture_group", "success_label"]:
    if c not in df.columns:
        sys.exit(f"{INPUT} is missing '{c}'.")

X = df[["lead_gender", "lead_culture_group"]].fillna("Unknown").astype(str)
y = df["success_label"].astype(int)

pre = ColumnTransformer([
    ("cat", OneHotEncoder(handle_unknown="ignore"), ["lead_gender", "lead_culture_group"])
])

clf = LogisticRegression(max_iter=1000, random_state=42)
pipe = Pipeline([("pre", pre), ("clf", clf)])

Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
pipe.fit(Xtr, ytr)

acc = accuracy_score(yte, pipe.predict(Xte))
print(f"Holdout accuracy (demographics-only): {acc:.3f}")
print(classification_report(yte, pipe.predict(Xte), digits=3))

joblib.dump(pipe, MODEL_OUT)
print(f"Saved {MODEL_OUT}")
