# dags/dag_metals.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.data_ingestion.cbr_client import CBRClient
from src.data_ingestion.storage import save_json_to_minio


def load_metals():
    yesterday = (datetime.now() - timedelta(days=1))

    date_api = yesterday.strftime("%d/%m/%Y")      # формат API
    date_iso = yesterday.strftime("%Y-%m-%d")      # для MinIO

    cbr = CBRClient()
    metals = cbr.get_metals(date_api, date_api)

    save_json_to_minio(metals, f"metals/{date_iso}.json")


with DAG(
    dag_id="dag_metals",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 12 * * *",
    catchup=False,
) as dag:

    PythonOperator(
        task_id="load_daily_metals",
        python_callable=load_metals,
    )
