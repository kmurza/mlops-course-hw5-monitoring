import os
import logging
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
REPORTS_DIR = os.getenv("REPORTS_DIR", "/opt/airflow/reports")

# В поле amount не должно быть пропусков
def _check_missing_amount(df: pd.DataFrame) -> dict:
    if "amount" not in df.columns:
        return {"name": "No missing amount", "status": "SKIP", "details": "Column not found"}
    missing = df["amount"].isna().sum()
    return {
        "name": "No missing amount",
        "status": "PASS" if missing == 0 else "FAIL",
        "details": f"Missing: {missing}",
    }

# Дата транзакции не может быть в будущем
def _check_future_dates(df: pd.DataFrame) -> dict:
    if "transaction_time" not in df.columns:
        return {"name": "No future transaction_time", "status": "SKIP", "details": "Column not found"}
    
    dates = pd.to_datetime(df["transaction_time"], errors="coerce")
    future = (dates > datetime.now()).sum()
    return {
        "name": "No future transaction_time",
        "status": "PASS" if future == 0 else "FAIL",
        "details": f"Future dates: {future}",
    }

# Сумма транзакции должна быть больше нуля
def _check_positive_amount(df: pd.DataFrame) -> dict:
    if "amount" not in df.columns:
        return {"name": "Amount must be positive", "status": "SKIP", "details": "Column not found"}
    
    negative = (df["amount"] <= 0).sum()
    return {
        "name": "Amount must be positive",
        "status": "PASS" if negative == 0 else "FAIL",
        "details": f"Negative/Zero amounts: {negative}",
    }

# У каждой транзакции должен быть указан мерчант
def _check_merchant_present(df: pd.DataFrame) -> dict:
    if "merch" not in df.columns:
        return {"name": "Merchant is present", "status": "SKIP", "details": "Column not found"}
    
    missing_merchant = df["merch"].isna().sum()
    return {
        "name": "Merchant is present",
        "status": "PASS" if missing_merchant == 0 else "FAIL",
        "details": f"Missing merchants: {missing_merchant}",
    }

# Целевая переменная должна быть 0 или 1
def _check_binary_target(df: pd.DataFrame) -> dict:
    if "target" not in df.columns:
        return {"name": "Target is binary (0/1)", "status": "SKIP", "details": "Column not found"}
    
    invalid = (~df["target"].isin([0, 1])).sum()
    return {
        "name": "Target is binary (0/1)",
        "status": "PASS" if invalid == 0 else "FAIL",
        "details": f"Invalid targets: {invalid}",
    }


def _generate_html_report(results: list, output_path: str) -> None:
    rows = ""
    for res in results:
        color = {"PASS": "green", "FAIL": "red", "SKIP": "orange"}.get(res["status"], "black")
        rows += f"""
        <tr>
            <td>{res['name']}</td>
            <td style="color: {color}; font-weight: bold;">{res['status']}</td>
            <td>{res['details']}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Data Quality Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <h1>Data Quality Report</h1>
    <p>Generated at: <b>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</b></p>
    <table>
        <tr><th>Rule</th><th>Status</th><th>Details</th></tr>
        {rows}
    </table>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


def run_data_quality_checks():
    warehouse_path = os.path.join(DATA_DIR, "warehouse.csv")
    if not os.path.exists(warehouse_path):
        raise FileNotFoundError(f"Warehouse not found: {warehouse_path}")

    logger.info("Loading warehouse for DQ checks...")
    df = pd.read_csv(warehouse_path)

    rules = [
        _check_missing_amount(df),
        _check_future_dates(df),
        _check_positive_amount(df),
        _check_merchant_present(df),
        _check_binary_target(df),
    ]

    os.makedirs(REPORTS_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"dq_report_{timestamp}.html"
    report_path = os.path.join(REPORTS_DIR, report_filename)
    
    _generate_html_report(rules, report_path)
    logger.info("DQ Report successfully saved to %s", report_path)

    failed_rules = [r["name"] for r in rules if r["status"] == "FAIL"]
    if failed_rules:
        raise ValueError(f"Data Quality checks failed: {', '.join(failed_rules)}")


with DAG(
    dag_id="data_quality_check",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["mlops", "dq"],
    doc_md="""
    ### Data Quality Check
    Этот DAG проверяет `warehouse.csv` на 5 правил:
    1. Нет пропусков в `amount`.
    2. Нет дат из будущего.
    3. Сумма больше нуля.
    4. У каждой транзакции есть мерчант (`merch`).
    5. `target` принимает только 0 или 1.
    
    Результаты сохраняются в `reports/dq_report_<timestamp>.html`.
    """
) as dag:
    check_dq = PythonOperator(
        task_id="check_data_quality",
        python_callable=run_data_quality_checks,
    )