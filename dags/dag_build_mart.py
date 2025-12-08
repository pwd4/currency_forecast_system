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


# =====================================================================
# CREATE MART TABLES
# =====================================================================
def create_mart_tables(cur):

    cur.execute("CREATE SCHEMA IF NOT EXISTS mart;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_date (
            date_hkey TEXT PRIMARY KEY,
            date_value DATE NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_currency (
            currency_hkey TEXT PRIMARY KEY,
            char_code TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_metal (
            metal_hkey TEXT PRIMARY KEY,
            metal_code INT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_brent (
            brent_hkey TEXT PRIMARY KEY,
            source TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.fact_market_prices (
            date_hkey TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_code TEXT NOT NULL,
            source TEXT,
            value NUMERIC,
            nominal NUMERIC,
            buy NUMERIC,
            sell NUMERIC,
            load_date TIMESTAMP NOT NULL
        );
    """)


# =====================================================================
# LOAD DIMENSIONS
# =====================================================================
def load_dimensions(cur):

    cur.execute("""
        INSERT INTO mart.dim_date(date_hkey, date_value)
        SELECT date_hkey, date_value
        FROM vault.hub_date
        ON CONFLICT (date_hkey) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO mart.dim_currency(currency_hkey, char_code)
        SELECT currency_hkey, char_code
        FROM vault.hub_currency
        ON CONFLICT (currency_hkey) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO mart.dim_metal(metal_hkey, metal_code)
        SELECT metal_hkey, metal_code
        FROM vault.hub_metal
        ON CONFLICT (metal_hkey) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO mart.dim_brent(brent_hkey, source)
        SELECT brent_hkey, source
        FROM vault.hub_brent
        ON CONFLICT (brent_hkey) DO NOTHING;
    """)


# =====================================================================
# LOAD FACT WITH DENSE FF + BF
# =====================================================================
def load_fact(cur):

    cur.execute("TRUNCATE mart.fact_market_prices;")

    # -----------------------------------------------------------------
    # 1) RAW FACT (sparse)
    # -----------------------------------------------------------------
    cur.execute("""
        WITH raw AS (

            -- currencies
            SELECT
                d.date_value,
                'currency' AS entity_type,
                c.char_code AS entity_code,
                NULL AS source,
                s.value,
                s.nominal,
                NULL AS buy,
                NULL AS sell,
                s.load_date
            FROM vault.sat_currency_rate s
            JOIN vault.hub_currency c USING(currency_hkey)
            JOIN vault.hub_date d ON d.date_value = s.rate_date

            UNION ALL

            -- metals
            SELECT
                d.date_value,
                'metal',
                m.metal_code::text,
                NULL AS source,
                NULL AS value,
                NULL AS nominal,
                s.buy,
                s.sell,
                s.load_date
            FROM vault.sat_metal_price s
            JOIN vault.hub_metal m USING(metal_hkey)
            JOIN vault.hub_date d ON d.date_value = s.price_date

            UNION ALL

            -- Brent EIA
            SELECT
                d.date_value,
                'brent',
                'eia',
                'eia' AS source,
                s.value,
                NULL AS nominal,
                NULL AS buy,
                NULL AS sell,
                s.load_date
            FROM vault.sat_brent_eia_price s
            JOIN vault.hub_brent b USING(brent_hkey)
            JOIN vault.hub_date d ON d.date_value = s.price_date
            WHERE b.source = 'eia'

            UNION ALL

            -- Brent MOEX
            SELECT
                d.date_value,
                'brent',
                'moex',
                'moex' AS source,
                s.value,
                NULL AS nominal,
                NULL AS buy,
                NULL AS sell,
                s.load_date
            FROM vault.sat_brent_moex_price s
            JOIN vault.hub_brent b USING(brent_hkey)
            JOIN vault.hub_date d ON d.date_value = s.price_date
            WHERE b.source = 'moex'
        ),

        -- -----------------------------------------------------------------
        -- 2) Create dense date–entity grid
        -- -----------------------------------------------------------------
        entities AS (
            SELECT DISTINCT entity_type, entity_code, source FROM raw
        ),
        dense AS (
            SELECT
                dd.date_value,
                e.entity_type,
                e.entity_code,
                e.source,
                r.value,
                r.nominal,
                r.buy,
                r.sell,
                r.load_date
            FROM mart.dim_date dd
            CROSS JOIN entities e
            LEFT JOIN raw r
              ON r.date_value = dd.date_value
             AND r.entity_type = e.entity_type
             AND r.entity_code = e.entity_code
             AND COALESCE(r.source,'x') = COALESCE(e.source,'x')
        ),

        -- -----------------------------------------------------------------
        -- 3) Forward-fill using MAX window
        -- -----------------------------------------------------------------
        ff AS (
            SELECT
                date_value,
                entity_type,
                entity_code,
                source,

                MAX(value)   OVER w AS value_ff,
                MAX(nominal) OVER w AS nominal_ff,
                MAX(buy)     OVER w AS buy_ff,
                MAX(sell)    OVER w AS sell_ff,
                MAX(load_date) OVER w AS load_date_ff

            FROM dense
            WINDOW w AS (
                PARTITION BY entity_type, entity_code, source
                ORDER BY date_value
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ),

        -- -----------------------------------------------------------------
        -- 4) Back-fill using MAX over reversed window
        -- -----------------------------------------------------------------
        bf AS (
            SELECT
                date_value,
                entity_type,
                entity_code,
                source,

                MAX(value_ff)     OVER w AS value_final,
                MAX(nominal_ff)   OVER w AS nominal_final,
                MAX(buy_ff)       OVER w AS buy_final,
                MAX(sell_ff)      OVER w AS sell_final,
                MAX(load_date_ff) OVER w AS load_date_final

            FROM ff
            WINDOW w AS (
                PARTITION BY entity_type, entity_code, source
                ORDER BY date_value DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        )

        INSERT INTO mart.fact_market_prices
        SELECT
            d.date_hkey,
            bf.entity_type,
            bf.entity_code,
            bf.source,
            bf.value_final,
            bf.nominal_final,
            bf.buy_final,
            bf.sell_final,
            bf.load_date_final
        FROM bf
        JOIN mart.dim_date d
          ON d.date_value = bf.date_value
        ORDER BY d.date_value, entity_type, entity_code;
    """)


# =====================================================================
# MAIN
# =====================================================================
def build_mart():
    print("Building dense Data Mart with FF/BF...")

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

    print("Dense Mart built successfully.")


# =====================================================================
# DAG
# =====================================================================
with DAG(
    dag_id="dag_build_mart",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 11 * * *",
    catchup=False,
    tags=["mart", "dense", "ffbf"]
) as dag:

    task = PythonOperator(
        task_id="build_mart",
        python_callable=build_mart
    )
