# dags/dag_build_mart.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2


PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )


# ================================================================
# CREATE MART SCHEMA + TABLES
# ================================================================
def create_mart_tables(cur):

    cur.execute("CREATE SCHEMA IF NOT EXISTS mart;")

    # ----------------------
    # DIM DATE
    # ----------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_date (
            date_hkey TEXT PRIMARY KEY,
            date_value DATE NOT NULL
        );
    """)

    # ----------------------
    # DIM CURRENCY
    # ----------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_currency (
            currency_hkey TEXT PRIMARY KEY,
            char_code TEXT NOT NULL
        );
    """)

    # ----------------------
    # DIM METAL
    # ----------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_metal (
            metal_hkey TEXT PRIMARY KEY,
            metal_code INT NOT NULL
        );
    """)

    # ----------------------
    # DIM BRENT
    # ----------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_brent (
            brent_hkey TEXT PRIMARY KEY,
            source TEXT NOT NULL
        );
    """)

    # ----------------------
    # FACT — единая аналитическая таблица
    # ----------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.fact_market_prices (
            date_hkey TEXT NOT NULL,
            entity_type TEXT NOT NULL,        -- currency / metal / brent
            entity_code TEXT NOT NULL,        -- AUD, XAU, MOEX, etc.
            source TEXT,                       -- eia / moex / null for metals
            value NUMERIC,                     -- main value
            nominal NUMERIC,                   -- only for currency
            buy NUMERIC,                       -- only for metals
            sell NUMERIC,                      -- only for metals
            open NUMERIC,                      -- for brent moex
            high NUMERIC,
            low NUMERIC,
            volume NUMERIC,                    -- brent moex
            load_date TIMESTAMP NOT NULL
        );
    """)


# ================================================================
# LOAD DIMENSIONS
# ================================================================
def load_dimensions(cur):

    # dim_date
    cur.execute("""
        INSERT INTO mart.dim_date(date_hkey, date_value)
        SELECT date_hkey, date_value
        FROM vault.hub_date
        ON CONFLICT (date_hkey) DO NOTHING;
    """)

    # dim_currency
    cur.execute("""
        INSERT INTO mart.dim_currency(currency_hkey, char_code)
        SELECT currency_hkey, char_code
        FROM vault.hub_currency
        ON CONFLICT (currency_hkey) DO NOTHING;
    """)

    # dim_metal
    cur.execute("""
        INSERT INTO mart.dim_metal(metal_hkey, metal_code)
        SELECT metal_hkey, metal_code
        FROM vault.hub_metal
        ON CONFLICT (metal_hkey) DO NOTHING;
    """)

    # dim_brent
    cur.execute("""
        INSERT INTO mart.dim_brent(brent_hkey, source)
        SELECT brent_hkey, source
        FROM vault.hub_brent
        ON CONFLICT (brent_hkey) DO NOTHING;
    """)


# ================================================================
# LOAD FACT TABLE
# ================================================================
def load_fact(cur):

    # Полная очистка факта (пересоздаём витрину каждый раз)
    cur.execute("TRUNCATE mart.fact_market_prices;")

    now = datetime.utcnow()

    # ============================================================
    # INSERT CURRENCY RATES → fact
    # ============================================================
    cur.execute("""
        INSERT INTO mart.fact_market_prices(
            date_hkey, entity_type, entity_code,
            value, nominal, load_date
        )
        SELECT
            d.date_hkey,
            'currency' AS entity_type,
            c.char_code AS entity_code,
            s.value,
            s.nominal,
            s.load_date
        FROM vault.sat_currency_rate s
        JOIN vault.hub_currency c USING(currency_hkey)
        JOIN vault.hub_date d ON d.date_value = s.rate_date;
    """)

    # ============================================================
    # INSERT METAL PRICES → fact
    # ============================================================
    cur.execute("""
        INSERT INTO mart.fact_market_prices(
            date_hkey, entity_type, entity_code,
            buy, sell, load_date
        )
        SELECT
            d.date_hkey,
            'metal' AS entity_type,
            m.metal_code::text AS entity_code,
            s.buy,
            s.sell,
            s.load_date
        FROM vault.sat_metal_price s
        JOIN vault.hub_metal m USING(metal_hkey)
        JOIN vault.hub_date d ON d.date_value = s.price_date;
    """)

    # ============================================================
    # INSERT BRENT PRICES (EIA + MOEX) → fact
    # ============================================================
    cur.execute("""
        INSERT INTO mart.fact_market_prices(
            date_hkey, entity_type, entity_code, source,
            value, open, high, low, volume, load_date
        )
        SELECT
            d.date_hkey,
            'brent' AS entity_type,
            b.source AS entity_code,
            b.source AS source,
            s.value,
            s.open,
            s.high,
            s.low,
            s.volume,
            s.load_date
        FROM vault.sat_brent_price s
        JOIN vault.hub_brent b USING(brent_hkey)
        JOIN vault.hub_date d ON d.date_value = s.price_date;
    """)


# ================================================================
# MAIN
# ================================================================
def build_mart():
    print("Building Data Mart...")

    conn = get_conn()
    cur = conn.cursor()

    create_mart_tables(cur)
    conn.commit()

    load_dimensions(cur)
    conn.commit()

    load_fact(cur)
    conn.commit()

    cur.close()
    conn.close()

    print("Data Mart completed!")


# ================================================================
# DAG DEFINITION
# ================================================================
with DAG(
    dag_id="dag_build_mart",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 11 * * *",   # после vault
    catchup=False,
    tags=["mart", "kimball"]
) as dag:

    task = PythonOperator(
        task_id="build_mart",
        python_callable=build_mart
    )
