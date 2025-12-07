from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import requests
import boto3
from botocore.exceptions import ClientError

from src.data_ingestion.cbr_client import CBRClient
from src.data_ingestion.moex_client import MoexClient
from src.data_ingestion.storage import save_json_to_minio

EIA_API_KEY = "igQPBkvoO0cUVZWGzP32EzK4I36Cg0te88QLLlaB"
EIA_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"

START_DATE = "2025-01-01"
END_DATE = "2025-12-31"

BUCKET = "currency-data"

# -------------------------------------------------------
# MinIO helper
# -------------------------------------------------------

def minio_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
    )


def minio_has_key(prefix, date_iso):
    """
    Проверяет, существует ли файл historical/<prefix>/<date>.json в MinIO.
    """
    client = minio_client()
    key = f"historical/{prefix}/{date_iso}.json"

    try:
        client.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError:
        return False


# -------------------------------------------------------
# UNIVERSAL FALLBACK LOGIC
# -------------------------------------------------------

def fetch_with_fallback(fetch_fn, target_date, max_back_days=10):
    dt = datetime.strptime(target_date, "%Y-%m-%d")

    for back in range(max_back_days + 1):
        d = (dt - timedelta(days=back)).strftime("%Y-%m-%d")
        result = fetch_fn(d)
        if result:
            return result, d

    return None, None


# -------------------------------------------------------
# FETCH FUNCTIONS FOR EACH SOURCE
# -------------------------------------------------------

def fetch_currencies(date_iso):
    cbr = CBRClient()
    d = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    return cbr.get_currency_rates(d)


def fetch_metals(date_iso):
    cbr = CBRClient()
    d = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    return cbr.get_metals(d, d)


def fetch_brent_moex(date_iso):
    moex = MoexClient()
    return moex.get_brent_history(start=date_iso)


def fetch_brent_eia(date_iso):
    params = {
        "frequency": "daily",
        "data[0]": "value",
        "facets[series][]": "RBRTE",
        "start": date_iso,
        "end": date_iso,
        "api_key": EIA_API_KEY
    }

    try:
        r = requests.get(EIA_URL, params=params, timeout=10)
        r.raise_for_status()
        j = r.json()
        return j.get("response", {}).get("data", [])
    except:
        return []


# -------------------------------------------------------
# MAIN HISTORICAL TASK
# -------------------------------------------------------

def load_historical():
    """
    Загружает исторические данные за год.
    Теперь **не делает запросы по API**, если файл уже есть в MinIO.
    """

    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    day = start
    while day <= end:
        date_iso = day.strftime("%Y-%m-%d")

        print(f"\n=== DATE {date_iso} ===")

        # ----------------------------------------------------
        # 1. Пропускаем дату, если все четыре источника уже есть
        # ----------------------------------------------------

        skip_cur = minio_has_key("currencies", date_iso)
        skip_met = minio_has_key("metals", date_iso)
        skip_bmoex = minio_has_key("brent_moex", date_iso)
        skip_beia = minio_has_key("brent_eia", date_iso)

        if skip_cur and skip_met and skip_bmoex and skip_beia:
            print(f"Дата {date_iso}: уже есть все данные → пропускаем")
            day += timedelta(days=1)
            continue

        # ----------------------------------------------------
        # 2. Догружаем только отсутствующие источники
        # ----------------------------------------------------

        # --- CURRENCIES ---
        if not skip_cur:
            cur_rows, cur_actual = fetch_with_fallback(fetch_currencies, date_iso)
            save_json_to_minio(
                {"target": date_iso, "actual": cur_actual, "records": cur_rows or []},
                f"historical/currencies/{date_iso}.json"
            )

        # --- METALS ---
        if not skip_met:
            met_rows, met_actual = fetch_with_fallback(fetch_metals, date_iso)
            save_json_to_minio(
                {"target": date_iso, "actual": met_actual, "records": met_rows or []},
                f"historical/metals/{date_iso}.json"
            )

        # --- BRENT MOEX ---
        if not skip_bmoex:
            br_moex_rows, br_moex_actual = fetch_with_fallback(fetch_brent_moex, date_iso)
            save_json_to_minio(
                {"target": date_iso, "actual": br_moex_actual, "records": br_moex_rows or []},
                f"historical/brent_moex/{date_iso}.json"
            )

        # --- BRENT EIA ---
        if not skip_beia:
            br_eia_rows, br_eia_actual = fetch_with_fallback(fetch_brent_eia, date_iso)
            save_json_to_minio(
                {"target": date_iso, "actual": br_eia_actual, "records": br_eia_rows or []},
                f"historical/brent_eia/{date_iso}.json"
            )

        day += timedelta(days=1)

    print("=== Historical load completed ===")


# -------------------------------------------------------
# DAG
# -------------------------------------------------------

with DAG(
    dag_id="historical_load",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # вручную
    catchup=False,
    tags=["historical-1", "etl"]
) as dag:

    PythonOperator(
        task_id="run_historical_load",
        python_callable=load_historical,
    )
