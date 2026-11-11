import logging
import pandas as pd

logger = logging.getLogger(__name__)
DROP_COLS = ["name_1", "name_2", "street", "post_code"]


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "transaction_time" in df.columns:
        df["transaction_time"] = pd.to_datetime(df["transaction_time"], errors="coerce")
        df["hour"] = df["transaction_time"].dt.hour
        df["day_of_week"] = df["transaction_time"].dt.dayofweek
        df["month"] = df["transaction_time"].dt.month
        df = df.drop(columns=["transaction_time"])
    return df


def drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=[c for c in DROP_COLS if c in df.columns])


def run_preproc(input_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting preprocessing, input shape: %s", input_df.shape)

    df = drop_unused_columns(input_df)
    df = add_time_features(df)

    logger.info("Preprocessing completed, output shape: %s", df.shape)
    return df