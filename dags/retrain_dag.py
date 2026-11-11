import os
import json
import sys
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.append("/opt/airflow/dags/src")

from prepare import prepare_train_test
from train import train_model
from evaluate import is_candidate_better
from publish import publish_model

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")


def next_logical_month():
    state_path = os.path.join(DATA_DIR, "state.json")
    if os.path.exists(state_path):
        with open(state_path) as f:
            last = json.load(f).get("last_month")
        if last:
            return (pd.Period(last, freq="M") + 1).strftime("%Y-%m")

    warehouse_path = os.path.join(DATA_DIR, "warehouse.csv")
    if not os.path.exists(warehouse_path):
        raise FileNotFoundError(f"Warehouse not found: {warehouse_path}")

    df = pd.read_csv(warehouse_path, usecols=["transaction_time"])
    latest = pd.to_datetime(df["transaction_time"], errors="coerce").dt.to_period("M").max()
    if pd.isna(latest):
        raise ValueError("Warehouse has no valid transaction_time values")

    return latest.strftime("%Y-%m")


def _task_prepare(**context):
    month = next_logical_month()
    context["ti"].xcom_push(key="logical_month", value=month)
    return prepare_train_test(month, window_months=3)


def _task_evaluate(**context):
    return "publish" if is_candidate_better() else "skip_publish"


def _task_publish(**context):
    month = context["ti"].xcom_pull(task_ids="prepare", key="logical_month")
    return publish_model(month)


with DAG(
    dag_id="retrain_fraud_model",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["mlops", "fraud"],
) as dag:
    prepare = PythonOperator(task_id="prepare", python_callable=_task_prepare)
    train = PythonOperator(task_id="train", python_callable=train_model)
    evaluate = BranchPythonOperator(task_id="evaluate", python_callable=_task_evaluate)
    publish = PythonOperator(task_id="publish", python_callable=_task_publish)
    skip = EmptyOperator(task_id="skip_publish")

    prepare >> train >> evaluate >> [publish, skip]