import os
import json
import pickle
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

from preprocessing import run_preproc

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")

NUM_COLS = [
    "amount", "lat", "lon", "population_city",
    "merchant_lat", "merchant_lon",
    "hour", "day_of_week", "month",
]
CAT_COLS = [
    "merch", "cat_id", "gender", "one_city", "us_state", "jobs",
]


def _build_pipeline() -> Pipeline:
    num_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])
    cat_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", max_categories=50)),
    ])
    preprocessor = ColumnTransformer(transformers=[
        ("num", num_pipe, NUM_COLS),
        ("cat", cat_pipe, CAT_COLS),
    ])
    return Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", LogisticRegression(
            max_iter=2000, class_weight="balanced", random_state=42,
        )),
    ])


def train_model() -> dict:
    train_df = pd.read_parquet(os.path.join(DATA_DIR, "train.parquet"))
    test_df = pd.read_parquet(os.path.join(DATA_DIR, "test.parquet"))

    target_col = "target"

    X_train = run_preproc(train_df.drop(columns=[target_col], errors="ignore"))
    y_train = train_df[target_col]
    X_test = run_preproc(test_df.drop(columns=[target_col], errors="ignore"))
    y_test = test_df[target_col]

    pipeline = _build_pipeline()
    pipeline.fit(X_train, y_train)

    proba = pipeline.predict_proba(X_test)[:, 1]
    auc = float(roc_auc_score(y_test, proba))

    with open(os.path.join(DATA_DIR, "model_candidate.pkl"), "wb") as f:
        pickle.dump(pipeline, f)
    with open(os.path.join(DATA_DIR, "model_candidate.metrics.json"), "w") as f:
        json.dump({"roc_auc": auc}, f)

    return {"roc_auc": auc}