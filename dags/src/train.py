import os
import json
import pickle
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import SimpleImputer

from preprocessing import run_preproc

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")


def train_model() -> dict:
    train_df = pd.read_parquet(os.path.join(DATA_DIR, "train.parquet"))
    test_df = pd.read_parquet(os.path.join(DATA_DIR, "test.parquet"))

    target_col = "target"

    X_train = run_preproc(train_df.drop(columns=[target_col], errors="ignore"))
    y_train = train_df[target_col]
    X_test = run_preproc(test_df.drop(columns=[target_col], errors="ignore"))
    y_test = test_df[target_col]

    cat_cols = X_train.select_dtypes(include=['object', 'category']).columns
    for col in cat_cols:
        le = LabelEncoder()
        X_train[col] = le.fit_transform(X_train[col].astype(str))

        le_classes = set(le.classes_)
        X_test[col] = X_test[col].astype(str).map(
            lambda x: le.transform([x])[0] if x in le_classes else le.transform([le.classes_[0]])[0]
        )

    imputer = SimpleImputer(strategy="mean")
    X_train = pd.DataFrame(imputer.fit_transform(X_train), columns=X_train.columns)
    X_test = pd.DataFrame(imputer.transform(X_test), columns=X_test.columns)


    model = LogisticRegression(
        max_iter=2000, class_weight="balanced", random_state=42
    )
    model.fit(X_train, y_train)

    proba = model.predict_proba(X_test)[:, 1]
    auc = float(roc_auc_score(y_test, proba))

    with open(os.path.join(DATA_DIR, "model_candidate.pkl"), "wb") as f:
        pickle.dump(model, f)
    with open(os.path.join(DATA_DIR, "model_candidate.metrics.json"), "w") as f:
        json.dump({"roc_auc": auc}, f)

    return {"roc_auc": auc}