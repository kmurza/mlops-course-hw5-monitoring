import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/dags/src")
from extend_warehouse import extend_warehouse

with DAG(
    dag_id="extend_warehouse",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["mlops", "warehouse"],
) as dag:
    extend = PythonOperator(
        task_id="extend_warehouse",
        python_callable=extend_warehouse,
    )