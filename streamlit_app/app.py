import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go

# -------------------------------------------------------
# CONFIG
# -------------------------------------------------------

st.set_page_config(
    page_title="Market Factors & RUB Exchange Dashboard",
    page_icon="📊",
    layout="wide"
)

DATABASE_URL = os.getenv("DATABASE_URL")  # "postgresql://airflow:airflow@postgres:5432/airflow"
engine = create_engine(DATABASE_URL)


# -------------------------------------------------------
# DATA LOADERS
# -------------------------------------------------------

@st.cache_data
def load_fact():
    query = """
        SELECT
            f.*,
            d.date_value AS date
        FROM mart.fact_market_prices f
        JOIN mart.dim_date d ON d.date_hkey = f.date_hkey
        ORDER BY d.date_value;
    """
    return pd.read_sql(query, engine)


@st.cache_data
def load_dimensions():
    dims = {}
    dims["currency"] = pd.read_sql("SELECT * FROM mart.dim_currency", engine)
    dims["metal"] = pd.read_sql("SELECT * FROM mart.dim_metal", engine)
    dims["brent"] = pd.read_sql("SELECT * FROM mart.dim_brent", engine)
    return dims


# -------------------------------------------------------
# LOAD DATA
# -------------------------------------------------------
fact = load_fact()
dims = load_dimensions()

st.title("📈 Market Factors & RUB Exchange Dashboard")
st.markdown("Визуализация данных из **Data Mart**, созданного по методологии Kimball.")


# -------------------------------------------------------
# SIDEBAR FILTERS
# -------------------------------------------------------
st.sidebar.header("🔍 Фильтры")

entity_type = st.sidebar.selectbox(
    "Тип рынка",
    ["currency", "metal", "brent"]
)

if entity_type == "currency":
    codes = dims["currency"]["char_code"].unique().tolist()
elif entity_type == "metal":
    codes = dims["metal"]["metal_code"].astype(str).unique().tolist()
else:
    codes = dims["brent"]["source"].unique().tolist()

entity_code = st.sidebar.selectbox("Выберите инструмент", codes)


# -------------------------------------------------------
# FILTER FACT
# -------------------------------------------------------
df = fact[
    (fact["entity_type"] == entity_type) &
    (fact["entity_code"] == str(entity_code))
].copy()

if df.empty:
    st.warning("Нет данных для выбранных параметров.")
    st.stop()


# -------------------------------------------------------
# MAIN GRAPH
# -------------------------------------------------------

if entity_type == "currency":
    y_column = "value"
    title = f"Курс валюты {entity_code} к RUB"
elif entity_type == "metal":
    y_column = "buy"
    title = f"Цена металла {entity_code}"
else:
    y_column = "value"
    title = f"Цена нефти Brent ({entity_code.upper()})"


fig = px.line(
    df,
    x="date",
    y=y_column,
    title=title,
    markers=True
)

fig.update_layout(height=450)

st.plotly_chart(fig, use_container_width=True)


# -------------------------------------------------------
# SECONDARY PANEL (DETAILS)
# -------------------------------------------------------
st.subheader("📄 Детали данных")

st.dataframe(df[["date", "entity_type", "entity_code", "value", "buy", "sell", "nominal"]])

# -------------------------------------------------------
# STATISTICS
# -------------------------------------------------------
st.subheader("📊 Статистика по ряду")

col1, col2, col3, col4 = st.columns(4)

col1.metric("📌 Последнее значение", round(df[y_column].iloc[-1], 4))
col2.metric("📈 Изменение за 7 дней", round(df[y_column].iloc[-1] - df[y_column].iloc[-7], 4) if len(df) > 7 else 0)
col3.metric("📉 Минимум", round(df[y_column].min(), 4))
col4.metric("📈 Максимум", round(df[y_column].max(), 4))

# -------------------------------------------------------
# CORRELATION WITH USD
# -------------------------------------------------------

if entity_type != "currency" and "USD" in dims["currency"]["char_code"].values:

    df_usd = fact[
        (fact["entity_type"] == "currency") &
        (fact["entity_code"] == "USD")
    ][["date", "value"]].rename(columns={"value": "usd_value"})

    merged = df.merge(df_usd, on="date", how="inner")

    corr = merged[y_column].corr(merged["usd_value"])

    st.subheader("🔗 Корреляция с USD/RUB")
    st.metric("Корреляция", round(corr, 4))


st.success("Данные загружены успешно ✔")
