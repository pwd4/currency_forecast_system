from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import json
import hashlib

import psycopg2
import boto3

# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "currency-data"


# ----------------------------------------------------------
# DB & S3 HELPERS
# ----------------------------------------------------------

def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def md5_key(*parts) -> str:
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def parse_date_ddmmyyyy_slash(s):  # "01/01/2025"
    return datetime.strptime(s, "%d/%m/%Y").date()


def parse_date_ddmmyyyy_dot(s):  # "29.12.2024"
    return datetime.strptime(s, "%d.%m.%Y").date()


def parse_date_iso(s):  # "2025-01-01"
    return datetime.strptime(s, "%Y-%m-%d").date()


# ----------------------------------------------------------
# TABLE / SCHEMA CREATION
# ----------------------------------------------------------

def create_vault_tables(cur):
    """
    Создаёт схему vault и все hub/link/sat таблицы, если они отсутствуют.
    Структура соответствует методологии Data Vault 2.0 + record_source и уникальности по бизнес-ключу + дате.
    """

    # Безопасное создание схемы vault (избегаем UniqueViolation)
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_namespace WHERE nspname = 'vault'
            ) THEN
                EXECUTE 'CREATE SCHEMA vault';
            END IF;
        END$$;
    """)

    # ----------------------
    # HUBS
    # ----------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_currency (
            currency_hkey TEXT PRIMARY KEY,
            char_code     TEXT NOT NULL,
            load_date     TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_metal (
            metal_hkey    TEXT PRIMARY KEY,
            metal_code    INT NOT NULL,
            load_date     TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_brent (
            brent_hkey    TEXT PRIMARY KEY,
            source        TEXT NOT NULL,          -- eia / moex
            load_date     TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_date (
            date_hkey     TEXT PRIMARY KEY,
            date_value    DATE NOT NULL,
            load_date     TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL
        );
    """)

    # ----------------------
    # LINKS
    # ----------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.link_currency_date (
            link_currency_date_hkey TEXT PRIMARY KEY,
            currency_hkey           TEXT NOT NULL REFERENCES vault.hub_currency,
            date_hkey               TEXT NOT NULL REFERENCES vault.hub_date,
            load_date               TIMESTAMP NOT NULL,
            record_source           TEXT NOT NULL,
            UNIQUE(currency_hkey, date_hkey)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.link_metal_date (
            link_metal_date_hkey TEXT PRIMARY KEY,
            metal_hkey           TEXT NOT NULL REFERENCES vault.hub_metal,
            date_hkey            TEXT NOT NULL REFERENCES vault.hub_date,
            load_date            TIMESTAMP NOT NULL,
            record_source        TEXT NOT NULL,
            UNIQUE(metal_hkey, date_hkey)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.link_brent_date (
            link_brent_date_hkey TEXT PRIMARY KEY,
            brent_hkey           TEXT NOT NULL REFERENCES vault.hub_brent,
            date_hkey            TEXT NOT NULL REFERENCES vault.hub_date,
            load_date            TIMESTAMP NOT NULL,
            record_source        TEXT NOT NULL,
            UNIQUE(brent_hkey, date_hkey)
        );
    """)

    # ----------------------
    # SATELLITES
    # ----------------------

    # Валюты
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_currency_rate (
            currency_hkey TEXT NOT NULL REFERENCES vault.hub_currency,
            rate_date     DATE NOT NULL,
            nominal       NUMERIC,
            value         NUMERIC,
            load_date     TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL,
            UNIQUE(currency_hkey, rate_date)
        );
    """)

    # Металлы
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_metal_price (
            metal_hkey    TEXT NOT NULL REFERENCES vault.hub_metal,
            price_date    DATE NOT NULL,
            buy           NUMERIC,
            sell          NUMERIC,
            load_date     TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL,
            UNIQUE(metal_hkey, price_date)
        );
    """)

    # Brent EIA
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_brent_eia_price (
            brent_hkey    TEXT NOT NULL REFERENCES vault.hub_brent,
            price_date    DATE NOT NULL,
            value         NUMERIC,
            load_date     TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL,
            UNIQUE(brent_hkey, price_date)
        );
    """)

    # Brent MOEX (та же структура, что и EIA, только другой record_source и hub.source = 'moex')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_brent_moex_price (
            brent_hkey    TEXT NOT NULL REFERENCES vault.hub_brent,
            price_date    DATE NOT NULL,
            value         NUMERIC,
            load_date     TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL,
            UNIQUE(brent_hkey, price_date)
        );
    """)


# ----------------------------------------------------------
# HUB + LINK HELPERS
# ----------------------------------------------------------

def ensure_date_hub(cur, d, load_ts, record_source):
    d_iso = d.isoformat()
    h = md5_key("DATE", d_iso)
    cur.execute("""
        INSERT INTO vault.hub_date(date_hkey, date_value, load_date, record_source)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date_hkey) DO NOTHING;
    """, (h, d, load_ts, record_source))
    return h


def ensure_currency_hub(cur, code, load_ts, record_source):
    code = code.upper()
    h = md5_key("CURR", code)
    cur.execute("""
        INSERT INTO vault.hub_currency(currency_hkey, char_code, load_date, record_source)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (currency_hkey) DO NOTHING;
    """, (h, code, load_ts, record_source))
    return h


def ensure_metal_hub(cur, metal_code, load_ts, record_source):
    h = md5_key("METAL", str(metal_code))
    cur.execute("""
        INSERT INTO vault.hub_metal(metal_hkey, metal_code, load_date, record_source)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (metal_hkey) DO NOTHING;
    """, (h, metal_code, load_ts, record_source))
    return h


def ensure_brent_hub(cur, source, load_ts, record_source):
    s = source.lower()  # 'eia' / 'moex'
    h = md5_key("BRENT", s)
    cur.execute("""
        INSERT INTO vault.hub_brent(brent_hkey, source, load_date, record_source)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (brent_hkey) DO NOTHING;
    """, (h, s, load_ts, record_source))
    return h


def ensure_link_currency_date(cur, currency_hkey, date_hkey, load_ts, record_source):
    link_hkey = md5_key("L_CURR_DATE", currency_hkey, date_hkey)
    cur.execute("""
        INSERT INTO vault.link_currency_date(
            link_currency_date_hkey, currency_hkey, date_hkey, load_date, record_source
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (currency_hkey, date_hkey) DO NOTHING;
    """, (link_hkey, currency_hkey, date_hkey, load_ts, record_source))


def ensure_link_metal_date(cur, metal_hkey, date_hkey, load_ts, record_source):
    link_hkey = md5_key("L_METAL_DATE", metal_hkey, date_hkey)
    cur.execute("""
        INSERT INTO vault.link_metal_date(
            link_metal_date_hkey, metal_hkey, date_hkey, load_date, record_source
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (metal_hkey, date_hkey) DO NOTHING;
    """, (link_hkey, metal_hkey, date_hkey, load_ts, record_source))


def ensure_link_brent_date(cur, brent_hkey, date_hkey, load_ts, record_source):
    link_hkey = md5_key("L_BRENT_DATE", brent_hkey, date_hkey)
    cur.execute("""
        INSERT INTO vault.link_brent_date(
            link_brent_date_hkey, brent_hkey, date_hkey, load_date, record_source
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (brent_hkey, date_hkey) DO NOTHING;
    """, (link_hkey, brent_hkey, date_hkey, load_ts, record_source))


# ----------------------------------------------------------
# LOADERS
# ----------------------------------------------------------

def load_currency(cur, data):
    """
    Ожидаемый формат historical/currencies/*.json:
    {
      "target": "2025-01-01",
      "actual": "2025-01-01",
      "records": [
        {"date": "01/01/2025", "char_code": "USD", "nominal": 1, "value": 101.67},
        ...
      ]
    }
    """
    record_source = "cbr_currency_api"
    load_ts = datetime.utcnow()

    if isinstance(data, list):
        rows = data
    else:
        rows = data.get("records") or []

    for r in rows:
        try:
            d = parse_date_ddmmyyyy_slash(r["date"])
        except Exception:
            continue

        code = r.get("char_code")
        if not code:
            continue

        nominal = r.get("nominal")
        value = r.get("value")

        h_curr = ensure_currency_hub(cur, code, load_ts, record_source)
        h_date = ensure_date_hub(cur, d, load_ts, record_source)
        ensure_link_currency_date(cur, h_curr, h_date, load_ts, record_source)

        # Защита от дубликатов по (currency_hkey, rate_date)
        cur.execute("""
            INSERT INTO vault.sat_currency_rate(
                currency_hkey, rate_date, nominal, value, load_date, record_source
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (currency_hkey, rate_date) DO NOTHING;
        """, (h_curr, d, nominal, value, load_ts, record_source))


def load_metals(cur, data):
    """
    Ожидаемый формат historical/metals/*.json:
    {
      "target": "2025-01-01",
      "actual": "2024-12-29",
      "records": [
        {"date": "29.12.2024", "metal_code": 1, "buy": 8551.74, "sell": 8551.74},
        ...
      ]
    }
    """
    record_source = "cbr_metals_api"
    load_ts = datetime.utcnow()

    if isinstance(data, list):
        rows = data
    else:
        rows = data.get("records") or []

    for r in rows:
        try:
            d = parse_date_ddmmyyyy_dot(r["date"])
        except Exception:
            continue

        try:
            metal_code = int(r["metal_code"])
        except Exception:
            continue

        buy = r.get("buy")
        sell = r.get("sell")

        h_metal = ensure_metal_hub(cur, metal_code, load_ts, record_source)
        h_date = ensure_date_hub(cur, d, load_ts, record_source)
        ensure_link_metal_date(cur, h_metal, h_date, load_ts, record_source)

        cur.execute("""
            INSERT INTO vault.sat_metal_price(
                metal_hkey, price_date, buy, sell, load_date, record_source
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (metal_hkey, price_date) DO NOTHING;
        """, (h_metal, d, buy, sell, load_ts, record_source))


def load_brent_eia(cur, data):
    """
    Формат historical/brent_eia/*.json:
    {
      "target": "2025-01-01",
      "actual": "2024-12-31",
      "records": [
        {
          "period": "2024-12-31",
          ...,
          "value": "74.58",
          ...
        }
      ]
    }
    """
    record_source = "eia_api"
    load_ts = datetime.utcnow()
    h_brent = ensure_brent_hub(cur, "eia", load_ts, record_source)

    records = data.get("records") or []
    if not records:
        return

    base_date_str = data.get("actual") or data.get("date") or data.get("target")

    for rec in records:
        d_str = rec.get("period") or base_date_str
        if not d_str:
            continue
        try:
            d = parse_date_iso(d_str)
        except Exception:
            continue

        h_date = ensure_date_hub(cur, d, load_ts, record_source)
        ensure_link_brent_date(cur, h_brent, h_date, load_ts, record_source)

        raw_value = rec.get("value")
        try:
            value = float(raw_value) if raw_value is not None else None
        except Exception:
            value = None

        cur.execute("""
            INSERT INTO vault.sat_brent_eia_price(
                brent_hkey, price_date, value, load_date, record_source
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (brent_hkey, price_date) DO NOTHING;
        """, (h_brent, d, value, load_ts, record_source))


def load_brent_moex(cur, data):
    """
    Формат historical/brent_moex/*.json:
    {
      "target": "2025-01-01",
      "actual": "2025-01-01",
      "records": [
        {
          "date": "2025-01-03",
          "settle_price": 77.25,
          ... (open/high/low/volume...)
        },
        ...
      ]
    }

    Мы используем только settle_price как value.
    """
    record_source = "moex_api"
    load_ts = datetime.utcnow()
    h_brent = ensure_brent_hub(cur, "moex", load_ts, record_source)

    records = data.get("records") or []
    if not records:
        return

    for rec in records:
        d_str = rec.get("date")
        if not d_str:
            continue
        try:
            d = parse_date_iso(d_str)
        except Exception:
            continue

        h_date = ensure_date_hub(cur, d, load_ts, record_source)
        ensure_link_brent_date(cur, h_brent, h_date, load_ts, record_source)

        settle = rec.get("settle_price")
        try:
            value = float(settle) if settle is not None else None
        except Exception:
            value = None

        cur.execute("""
            INSERT INTO vault.sat_brent_moex_price(
                brent_hkey, price_date, value, load_date, record_source
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (brent_hkey, price_date) DO NOTHING;
        """, (h_brent, d, value, load_ts, record_source))


# ----------------------------------------------------------
# MAIN LOAD FUNCTION
# ----------------------------------------------------------

def load_vault():
    print("Starting DV load...")

    s3 = get_s3()
    conn = get_pg_conn()
    cur = conn.cursor()

    create_vault_tables(cur)
    conn.commit()

    # Собираем все ключи из MinIO
    keys = []
    token = None

    while True:
        args = {"Bucket": MINIO_BUCKET}
        if token:
            args["ContinuationToken"] = token

        resp = s3.list_objects_v2(**args)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])

        if not resp.get("IsTruncated"):
            break

        token = resp.get("NextContinuationToken")

    print(f"Found {len(keys)} objects in MinIO")
    print("Showing first 30 keys:")
    for k in keys[:30]:
        print("   ", k)

    # Обрабатываем только historical/*
    for key in sorted(keys):

        if not key.endswith(".json"):
            continue

        if not (
            key.startswith("historical/currencies/")
            or key.startswith("historical/metals/")
            or key.startswith("historical/brent_eia/")
            or key.startswith("historical/brent_moex/")
        ):
            # всё остальное игнорируем
            continue

        print(f"\nProcessing {key}")

        raw = s3.get_object(Bucket=MINIO_BUCKET, Key=key)["Body"].read().decode()
        try:
            data = json.loads(raw)
        except Exception:
            print("JSON parse error")
            continue

        try:
            if key.startswith("historical/currencies/"):
                load_currency(cur, data)

            elif key.startswith("historical/metals/"):
                load_metals(cur, data)

            elif key.startswith("historical/brent_eia/"):
                load_brent_eia(cur, data)

            elif key.startswith("historical/brent_moex/"):
                load_brent_moex(cur, data)

        except Exception as e:
            print(f"ERROR processing {key}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print("DV load finished.")


# ----------------------------------------------------------
# DAG
# ----------------------------------------------------------

with DAG(
    "dag_load_vault",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 10 * * *",
    catchup=False,
    tags=["vault"],
) as dag:

    load_task = PythonOperator(
        task_id="load_vault_from_minio",
        python_callable=load_vault,
    )
