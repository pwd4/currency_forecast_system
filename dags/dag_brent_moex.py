from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.data_ingestion.moex_client import MoexClient
from src.data_ingestion.storage import save_json_to_minio


def load_latest_brent():
    """
    Загрузка ближайшей доступной котировки Brent с MOEX.
    Берём последние 14 дней и выбираем максимальную TRADEDATE.
    """

    end_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")

    moex = MoexClient()
    rows = moex.get_brent_history(start=start_date)

    if not rows:
        print("MOEX: нет данных за последние 14 дней")
        save_json_to_minio({"error": "no data"}, f"brent_moex/{end_date}.json")
        return

    # выбираем последнюю доступную дату
    rows_sorted = sorted(rows, key=lambda r: r["date"])
    latest = rows_sorted[-1]         # последний элемент = свежая дата
    latest_date = latest["date"]

    result = {
        "date": latest_date,
        "records": [r for r in rows if r["date"] == latest_date]
    }

    key = f"brent_moex/{latest_date}.json"
    save_json_to_minio(result, key)

    print(f"[MOEX Brent] saved → {key}")
    print(result)


with DAG(
    dag_id="brent_moex_daily",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 10 * * *",   # ежедневно 10:00
    catchup=False,
    tags=["brent", "moex"]
) as dag:

    PythonOperator(
        task_id="load_latest_brent_from_moex",
        python_callable=load_latest_brent,
    )
