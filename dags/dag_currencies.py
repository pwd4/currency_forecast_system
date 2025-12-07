# dags/dag_currencies.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.data_ingestion.cbr_client import CBRClient
from src.data_ingestion.storage import save_json_to_minio


def load_currencies():
    cbr = CBRClient()

    today_display = datetime.now().strftime("%d/%m/%Y")     # формат API ЦБ
    today_iso = datetime.now().strftime("%Y-%m-%d")         # для MinIO

    rates = cbr.get_currency_rates(today_display)

    save_json_to_minio(rates, f"currencies/{today_iso}.json")


with DAG(
    dag_id="currencies_cbr.ru",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 9 * * *",
    catchup=False,
) as dag:

    PythonOperator(
        task_id="load_daily_currencies",
        python_callable=load_currencies,
    )
