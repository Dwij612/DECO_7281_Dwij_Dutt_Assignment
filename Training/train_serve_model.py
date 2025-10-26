# train_serve_model.py
# Trains a model WITHOUT genre or budget band and saves serve_model.joblib

import pandas as pd
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib

INPUT = "movies_ml_ready.csv"
MODEL_OUT = "serve_model.joblib"

# No genre, no budget band:
CAT = ["decade", "language_group"]
NUM = ["franchise", "vote_average", "year"]
TARGET = "success_label"

df = pd.read_csv(INPUT)

for c in CAT:
    if c not in df.columns: df[c] = "Unknown"
    df[c] = df[c].astype(str).fillna("Unknown")

for c in NUM:
    if c not in df.columns: df[c] = 0
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(df[c].median())

y = df[TARGET].astype(int)
X = df[CAT + NUM].copy()

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

pre = ColumnTransformer(
    [
        ("cat", OneHotEncoder(handle_unknown="ignore"), CAT),
        ("num", StandardScaler(), NUM),
    ],
    remainder="drop",
)

pipe = Pipeline([("pre", pre), ("clf", LogisticRegression(max_iter=1000, random_state=42))])
pipe.fit(X_train, y_train)

acc = accuracy_score(y_test, pipe.predict(X_test))
print(f"Holdout accuracy (no-genre/no-budget): {acc:.3f}")

joblib.dump(pipe, MODEL_OUT)
print(f"Saved {MODEL_OUT}")
