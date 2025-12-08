# dags/dag_load_vault.py

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
# HELPERS
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


def parse_date_ddmmyyyy_slash(s):  # "07/12/2025"
    return datetime.strptime(s, "%d/%m/%Y").date()


def parse_date_ddmmyyyy_dot(s):  # "06.12.2025"
    return datetime.strptime(s, "%d.%m.%Y").date()


def parse_date_iso(s):  # "2025-12-01"
    return datetime.strptime(s, "%Y-%m-%d").date()


# ----------------------------------------------------------
# TABLE CREATION (unchanged)
# ----------------------------------------------------------

def create_vault_tables(cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS vault;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_currency (
            currency_hkey text PRIMARY KEY,
            char_code text NOT NULL,
            load_date timestamp NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_metal (
            metal_hkey text PRIMARY KEY,
            metal_code int NOT NULL,
            load_date timestamp NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_brent (
            brent_hkey text PRIMARY KEY,
            source text NOT NULL,
            load_date timestamp NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_date (
            date_hkey text PRIMARY KEY,
            date_value date NOT NULL,
            load_date timestamp NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.link_currency_date (
            link_currency_date_hkey text PRIMARY KEY,
            currency_hkey text NOT NULL REFERENCES vault.hub_currency,
            date_hkey text NOT NULL REFERENCES vault.hub_date,
            load_date timestamp NOT NULL,
            UNIQUE(currency_hkey, date_hkey)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.link_metal_date (
            link_metal_date_hkey text PRIMARY KEY,
            metal_hkey text NOT NULL REFERENCES vault.hub_metal,
            date_hkey text NOT NULL REFERENCES vault.hub_date,
            load_date timestamp NOT NULL,
            UNIQUE(metal_hkey, date_hkey)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.link_brent_date (
            link_brent_date_hkey text PRIMARY KEY,
            brent_hkey text NOT NULL REFERENCES vault.hub_brent,
            date_hkey text NOT NULL REFERENCES vault.hub_date,
            load_date timestamp NOT NULL,
            UNIQUE(brent_hkey, date_hkey)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_currency_rate (
            currency_hkey text NOT NULL,
            rate_date date NOT NULL,
            nominal numeric,
            value numeric,
            load_date timestamp NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_metal_price (
            metal_hkey text NOT NULL,
            price_date date NOT NULL,
            buy numeric,
            sell numeric,
            load_date timestamp NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_brent_price (
            brent_hkey text NOT NULL,
            price_date date NOT NULL,
            value numeric,
            open numeric,
            high numeric,
            low numeric,
            volume numeric,
            load_date timestamp NOT NULL
        );
    """)


# ----------------------------------------------------------
# HUB + LINK HELPERS
# ----------------------------------------------------------

def ensure_date_hub(cur, d, load_ts):
    d_iso = d.isoformat()
    h = md5_key("DATE", d_iso)
    cur.execute("""
        INSERT INTO vault.hub_date(date_hkey, date_value, load_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (date_hkey) DO NOTHING;
    """, (h, d, load_ts))
    return h


def ensure_currency_hub(cur, code, load_ts):
    code = code.upper()
    h = md5_key("CURR", code)
    cur.execute("""
        INSERT INTO vault.hub_currency(currency_hkey, char_code, load_date)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (h, code, load_ts))
    return h


def ensure_metal_hub(cur, metal_code, load_ts):
    h = md5_key("METAL", str(metal_code))
    cur.execute("""
        INSERT INTO vault.hub_metal(metal_hkey, metal_code, load_date)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (h, metal_code, load_ts))
    return h


def ensure_brent_hub(cur, source, load_ts):
    s = source.lower()
    h = md5_key("BRENT", s)
    cur.execute("""
        INSERT INTO vault.hub_brent(brent_hkey, source, load_date)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (h, s, load_ts))
    return h


def ensure_link(cur, table, link_hkey, h1, h2, load_ts):
    cur.execute(f"""
        INSERT INTO vault.{table}(link_{table}_hkey, {table.split('_')[1]}_hkey, date_hkey, load_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT ({table.split('_')[1]}_hkey, date_hkey) DO NOTHING;
    """, (link_hkey, h1, h2, load_ts))


# ----------------------------------------------------------
# LOADERS
# ----------------------------------------------------------

def load_currency(cur, rows):
    load_ts = datetime.utcnow()
    for r in rows:
        try:
            d = parse_date_ddmmyyyy_slash(r["date"])
        except:
            continue

        code = r["char_code"]
        nominal = r.get("nominal")
        value = r.get("value")

        h_curr = ensure_currency_hub(cur, code, load_ts)
        h_date = ensure_date_hub(cur, d, load_ts)

        link_hkey = md5_key("L_CURR_DATE", h_curr, h_date)
        cur.execute("""
            INSERT INTO vault.link_currency_date(
                link_currency_date_hkey, currency_hkey, date_hkey, load_date
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (currency_hkey, date_hkey) DO NOTHING;
        """, (link_hkey, h_curr, h_date, load_ts))

        cur.execute("""
            INSERT INTO vault.sat_currency_rate(currency_hkey, rate_date, nominal, value, load_date)
            VALUES (%s, %s, %s, %s, %s);
        """, (h_curr, d, nominal, value, load_ts))


def load_metals(cur, rows):
    load_ts = datetime.utcnow()
    for r in rows:
        try:
            d = parse_date_ddmmyyyy_dot(r["date"])
        except:
            continue

        metal_code = int(r["metal_code"])
        buy = r.get("buy")
        sell = r.get("sell")

        h_m = ensure_metal_hub(cur, metal_code, load_ts)
        h_date = ensure_date_hub(cur, d, load_ts)

        link_hkey = md5_key("L_METAL_DATE", h_m, h_date)
        cur.execute("""
            INSERT INTO vault.link_metal_date(
                link_metal_date_hkey, metal_hkey, date_hkey, load_date
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (link_hkey, h_m, h_date, load_ts))

        cur.execute("""
            INSERT INTO vault.sat_metal_price(metal_hkey, price_date, buy, sell, load_date)
            VALUES (%s, %s, %s, %s, %s);
        """, (h_m, d, buy, sell, load_ts))


# ----------------------------------------------------------
# BRENT EIA — NOW SUPPORTS historical/*
# ----------------------------------------------------------

def load_brent_eia(cur, data, source_name):
    """
    Supports:
      {"date": "...", "records":[...]}
      {"target":"...", "actual":"...", "records":[...]}
    """
    load_ts = datetime.utcnow()
    h_brent = ensure_brent_hub(cur, source_name, load_ts)

    records = data.get("records") or []
    if not records:
        return

    # choose real date
    base_date_str = data.get("actual") or data.get("date") or data.get("target")

    for rec in records:

        d_str = rec.get("period", base_date_str)
        try:
            d = parse_date_iso(d_str)
        except:
            continue

        h_date = ensure_date_hub(cur, d, load_ts)

        link_hkey = md5_key("L_BRENT_DATE", h_brent, h_date)
        cur.execute("""
            INSERT INTO vault.link_brent_date(
                link_brent_date_hkey, brent_hkey, date_hkey, load_date
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (link_hkey, h_brent, h_date, load_ts))

        try:
            value = float(rec.get("value")) if rec.get("value") else None
        except:
            value = None

        cur.execute("""
            INSERT INTO vault.sat_brent_price(
                brent_hkey, price_date, value, open, high, low, volume, load_date
            )
            VALUES (%s, %s, %s, NULL, NULL, NULL, NULL, %s);
        """, (h_brent, d, value, load_ts))


# ----------------------------------------------------------
# BRENT MOEX (unchanged, works)
# ----------------------------------------------------------

def load_brent_moex(cur, data):
    records = data.get("records") or []
    if not records:
        return

    load_ts = datetime.utcnow()
    h_brent = ensure_brent_hub(cur, "moex", load_ts)

    for rec in records:
        d_str = rec.get("date") or data.get("date")
        try:
            d = parse_date_iso(d_str)
        except:
            continue

        h_date = ensure_date_hub(cur, d, load_ts)

        link_hkey = md5_key("L_BRENT_DATE", h_brent, h_date)
        cur.execute("""
            INSERT INTO vault.link_brent_date(
                link_brent_date_hkey, brent_hkey, date_hkey, load_date
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (link_hkey, h_brent, h_date, load_ts))

        settle = rec.get("settle_price") or rec.get("close")

        try:
            value = float(settle)
        except:
            value = None

        cur.execute("""
            INSERT INTO vault.sat_brent_price(
                brent_hkey, price_date, value, open, high, low, volume, load_date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            h_brent,
            d,
            value,
            rec.get("open"),
            rec.get("high"),
            rec.get("low"),
            rec.get("volume"),
            load_ts
        ))


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

    # collect all keys
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

    # process sorted keys
    for key in sorted(keys):

        if not key.endswith(".json"):
            continue

        print(f"\nProcessing {key}")

        raw = s3.get_object(Bucket=MINIO_BUCKET, Key=key)["Body"].read().decode()
        try:
            data = json.loads(raw)
        except Exception:
            print("JSON parse error")
            continue

        # dispatch
        try:
            if key.startswith("currencies/"):
                if isinstance(data, list):
                    load_currency(cur, data)

            elif key.startswith("metals/"):
                if isinstance(data, list):
                    load_metals(cur, data)

            elif key.startswith("brent_eia/"):
                load_brent_eia(cur, data, "eia")

            elif key.startswith("historical/brent_eia/"):
                load_brent_eia(cur, data, "eia")

            elif key.startswith("brent_moex/"):
                load_brent_moex(cur, data)

            else:
                print(f"Ignored key: {key}")

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
