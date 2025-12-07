from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

from src.data_ingestion.storage import save_json_to_minio

EIA_API_KEY = "igQPBkvoO0cUVZWGzP32EzK4I36Cg0te88QLLlaB"
EIA_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"


def fetch_brent_latest():
    """Берём последнюю доступную цену Brent через EIA."""

    end_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")

    params = {
        "frequency": "daily",
        "data[0]": "value",
        "facets[series][]": "RBRTE",
        "start": start_date,
        "end": end_date,
        "api_key": EIA_API_KEY
    }

    response = requests.get(EIA_URL, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    rows = data.get("response", {}).get("data", [])

    if not rows:
        print("EIA: нет данных в диапазоне")
        save_json_to_minio({"error": "no data"}, f"brent_eia/{end_date}.json")
        return

    # сортировка по дате
    rows_sorted = sorted(rows, key=lambda x: x["period"])

    # последняя доступная дата
    latest = rows_sorted[-1]
    latest_date = latest["period"]

    result = {
        "date": latest_date,
        "records": [r for r in rows if r["period"] == latest_date]
    }

    key = f"brent_eia/{latest_date}.json"
    save_json_to_minio(result, key)

    print(f"[EIA Brent] saved → {key}")
    print(result)


with DAG(
    dag_id="brent_eia_daily",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 10 * * *",
    catchup=False,
    tags=["brent", "eia"]
) as dag:

    PythonOperator(
        task_id="fetch_brent_latest",
        python_callable=fetch_brent_latest,
    )
