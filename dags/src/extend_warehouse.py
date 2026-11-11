import os
import json
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100000"))
HISTORY_YEARS = int(os.getenv("HISTORY_YEARS", "3"))


def _read_last_month():
    state_path = os.path.join(DATA_DIR, "state.json")
    if not os.path.exists(state_path):
        return None

    with open(state_path) as f:
        return json.load(f).get("last_month")


def extend_warehouse():
    wh_path = os.path.join(DATA_DIR, "warehouse.csv")
    if not os.path.exists(wh_path):
        raise FileNotFoundError(f"Warehouse not found: {wh_path}")

    df = pd.read_csv(wh_path)
    df["transaction_time"] = pd.to_datetime(df["transaction_time"], errors="coerce")
    df = df.dropna(subset=["transaction_time"])

    last_month = _read_last_month()
    if last_month:
        next_month = (pd.Period(last_month, freq="M") + 1).strftime("%Y-%m")
    else:
        next_month = (
            df["transaction_time"].dt.to_period("M").max() + 1
        ).strftime("%Y-%m")

    existing_months = set(df["transaction_time"].dt.to_period("M").astype(str))
    if next_month in existing_months:
        logger.info("Month %s already exists in warehouse, skipping", next_month)
        return {
            "added": 0,
            "total": len(df),
            "next_month": next_month,
            "skipped": "month already exists",
        }


    batch = df.sample(n=BATCH_SIZE, replace=True).copy()
    
    batch["transaction_time"] = batch["transaction_time"] + pd.DateOffset(months=1)
    
    if "transaction_id" in batch.columns:
        batch["transaction_id"] = [
            f"tx_{next_month}_{np.random.randint(1000000, 9999999)}_{i}" 
            for i in range(len(batch))
        ]
    
    logger.info("Generated %d new rows for month %s", len(batch), next_month)

    df_new = pd.concat([df, batch], ignore_index=True)

    cutoff = df_new["transaction_time"].max() - pd.DateOffset(years=HISTORY_YEARS)
    before = len(df_new)
    df_new = df_new[df_new["transaction_time"] >= cutoff]
    dropped = before - len(df_new)
    if dropped:
        logger.info("Dropped %d old rows (older than %d years)", dropped, HISTORY_YEARS)

    df_new.to_csv(wh_path, index=False)
    logger.info("Warehouse now has %d rows", len(df_new))

    return {"added": len(batch), "total": len(df_new), "next_month": next_month}