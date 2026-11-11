import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")


def prepare_train_test(logical_month, window_months=3):
    warehouse_path = os.path.join(DATA_DIR, "warehouse.csv")
    if not os.path.exists(warehouse_path):
        raise FileNotFoundError(f"Warehouse not found: {warehouse_path}")

    df = pd.read_csv(warehouse_path)
    df["transaction_time"] = pd.to_datetime(df["transaction_time"], errors="coerce")
    df = df.dropna(subset=["transaction_time"])
    df["_month"] = df["transaction_time"].dt.to_period("M").astype(str)

    all_months = sorted(df["_month"].unique())

    if logical_month in all_months:
        target_test_month = logical_month
    else:
        target_test_month = all_months[-1]
        logger.warning(f"Month {logical_month} not found. Using {target_test_month} instead.")

    idx = all_months.index(target_test_month)
    start_idx = max(0, idx - window_months + 1)
    months = all_months[start_idx : idx + 1]
    
    window_df = df[df["_month"].isin(months)].copy()
    
    test_df = window_df[window_df["_month"] == target_test_month].copy()
    train_df = window_df[window_df["_month"] != target_test_month].copy()

    if train_df.empty:
        logger.warning("No data left for train! Using a copy of the test month.")
        train_df = test_df.copy()

    train_df = train_df.drop(columns=["_month"])
    test_df = test_df.drop(columns=["_month"])

    logger.info("Prepared: train=%d rows, test=%d rows", len(train_df), len(test_df))

    train_path = os.path.join(DATA_DIR, "train.parquet")
    test_path = os.path.join(DATA_DIR, "test.parquet")
    train_df.to_parquet(train_path, index=False)
    test_df.to_parquet(test_path, index=False)

    return {
        "train_rows": len(train_df),
        "test_rows": len(test_df),
        "months": months,
    }