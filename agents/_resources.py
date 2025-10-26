# agents/_resources.py
import pandas as pd
import joblib
from functools import lru_cache

MODEL_PATH = "serve_model.joblib"
DATA_PATHS = ["movies_demographics_norm.csv", "movies_demographics.csv"]

MIN_N = 30
GENDERS = ["Male", "Female", "Non-binary", "Unknown"]

def _load_first(paths):
    last_err = None
    for p in paths:
        try:
            return pd.read_csv(p), p
        except Exception as e:
            last_err = e
    raise SystemExit(f"Could not load any of: {paths}. Last error: {last_err}")

def _sr_table(df, col):
    g = df.groupby(col)["success_label"].agg(["mean", "count"]).reset_index()
    return {row[col]: (float(row["mean"]), int(row["count"])) for _, row in g.iterrows()}

@lru_cache(maxsize=1)
def get_resources():
    try:
        pipe = joblib.load(MODEL_PATH)
    except Exception as e:
        raise SystemExit(f"Could not load model at {MODEL_PATH}: {e}")

    df_base, used_data_path = _load_first(DATA_PATHS)
    req = ["lead_gender", "lead_culture_group", "success_label"]
    miss = [c for c in req if c not in df_base.columns]
    if miss:
        raise SystemExit(f"{used_data_path} missing columns: {miss}")

    sr_gender = _sr_table(df_base, "lead_gender")
    sr_cult   = _sr_table(df_base, "lead_culture_group")
    global_sr = float(df_base["success_label"].mean())

    return dict(
        pipe=pipe,
        df_base=df_base,
        sr_gender=sr_gender,
        sr_cult=sr_cult,
        global_sr=global_sr,
        MIN_N=MIN_N,
        GENDERS=GENDERS,
    )
