import os
import math
import pandas as pd
import streamlit as st
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

DATABASE_URL = os.getenv("DATABASE_URL")
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
st.markdown(
    "Визуализация данных из **Data Mart**, построенного по методологии Kimball "
    "на основе слоя **Data Vault**."
)

# гарантируем сортировку по дате
fact = fact.sort_values("date")
fact["date"] = pd.to_datetime(fact["date"])


# -------------------------------------------------------
# SIDEBAR FILTERS
# -------------------------------------------------------

st.sidebar.header("🔍 Фильтры")

entity_type = st.sidebar.selectbox(
    "Тип рынка",
    ["currency", "metal", "brent"],
    format_func=lambda x: {
        "currency": "Валюты",
        "metal": "Драгоценные металлы",
        "brent": "Нефть Brent"
    }[x]
)

if entity_type == "currency":
    codes = sorted(dims["currency"]["char_code"].unique().tolist())
    entity_code = st.sidebar.selectbox("Валюта", codes)

elif entity_type == "brent":
    codes = dims["brent"]["source"].unique().tolist()
    entity_code = st.sidebar.selectbox("Источник Brent", codes)
else:
    entity_code = None  # металлы выбираются все сразу

# Параметры анализа
st.sidebar.header("⚙ Параметры анализа")

ema_window = None
log_scale = False

if entity_type != "metal":
    ema_window = st.sidebar.selectbox(
        "Скользящая средняя (EMA, дней)",
        [None, 7, 14, 30],
        format_func=lambda x: "Нет" if x is None else str(x)
    )
    log_scale = st.sidebar.checkbox("Логарифмическая шкала Y", value=False)


# -------------------------------------------------------
# FILTER FACT
# -------------------------------------------------------

if entity_type == "metal":
    df = fact[fact["entity_type"] == "metal"].copy()
else:
    df = fact[
        (fact["entity_type"] == entity_type) &
        (fact["entity_code"] == str(entity_code))
    ].copy()

if df.empty:
    st.warning("Нет данных для выбранных параметров.")
    st.stop()

metal_names = {
    "1": "Gold",
    "2": "Silver",
    "3": "Platinum",
    "4": "Palladium"
}


# -------------------------------------------------------
# LAYOUT TABS (теперь 2 вкладки)
# -------------------------------------------------------

tab_main, tab_table = st.tabs(["📉 Графики", "📄 Таблица"])


# -------------------------------------------------------
# MAIN GRAPH + SMALL MULTIPLES
# -------------------------------------------------------

with tab_main:
    if entity_type == "currency":
        y_col = "value"
        title = f"Курс валюты {entity_code} к RUB"

        fig = px.line(
            df,
            x="date",
            y=y_col,
            title=title,
            labels={"date": "Дата", y_col: "Курс (RUB)"},
            markers=True
        )

        # EMA
        if ema_window is not None and len(df) > ema_window:
            df["ema"] = df[y_col].ewm(span=ema_window, adjust=False).mean()
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df["ema"],
                    mode="lines",
                    name=f"EMA {ema_window}",
                    line=dict(width=2, dash="dash")
                )
            )

        if log_scale:
            fig.update_yaxes(type="log")

        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

    elif entity_type == "brent":
        y_col = "value"
        title = f"Цена нефти Brent ({entity_code.upper()})"

        fig = px.line(
            df,
            x="date",
            y=y_col,
            title=title,
            labels={"date": "Дата", y_col: "Цена (USD за баррель)"},
            markers=True
        )

        # EMA
        if ema_window is not None and len(df) > ema_window:
            df["ema"] = df[y_col].ewm(span=ema_window, adjust=False).mean()
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df["ema"],
                    mode="lines",
                    name=f"EMA {ema_window}",
                    line=dict(width=2, dash="dash")
                )
            )

        if log_scale:
            fig.update_yaxes(type="log")

        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

    else:
        # -------- METALS: общий график + 4 мини-графика --------
        st.subheader("💰 Цены драгоценных металлов (SELL, RUB) — общий график")

        fig = go.Figure()

        big_metals = ["1", "3", "4"]
        small_metals = ["2"]

        for code in big_metals:
            df_m = df[df["entity_code"] == code]
            if not df_m.empty:
                fig.add_trace(go.Scatter(
                    x=df_m["date"],
                    y=df_m["sell"],
                    mode="lines",
                    name=f"{metal_names[code]} (SELL)",
                    line=dict(width=2)
                ))

        for code in small_metals:
            df_m = df[df["entity_code"] == code]
            if not df_m.empty:
                fig.add_trace(go.Scatter(
                    x=df_m["date"],
                    y=df_m["sell"],
                    mode="lines",
                    name=f"{metal_names[code]} (SELL)",
                    line=dict(width=2, dash="dot"),
                    yaxis="y2"
                ))

        fig.update_layout(
            xaxis_title="Дата",
            yaxis_title="Цена (RUB)",
            yaxis2=dict(
                title="Silver (RUB)",
                overlaying="y",
                side="right",
                showgrid=False
            ),
            height=450,
            legend=dict(x=1.02, y=1)
        )

        st.plotly_chart(fig, use_container_width=True)

        # small multiples
        st.markdown("#### Отдельные графики по каждому металлу")

        cols = st.columns(4)
        for idx, code in enumerate(["1", "2", "3", "4"]):
            df_m = df[df["entity_code"] == code]
            if df_m.empty:
                continue

            with cols[idx]:
                sub_fig = px.line(
                    df_m,
                    x="date",
                    y="sell",
                    title=metal_names[code],
                    labels={"date": "Дата", "sell": "SELL (RUB)"},
                    height=250
                )
                sub_fig.update_layout(margin=dict(l=10, r=10, t=40, b=10))
                st.plotly_chart(sub_fig, use_container_width=True)


# -------------------------------------------------------
# TABLE TAB
# -------------------------------------------------------

with tab_table:
    st.subheader("📄 Детали данных")
    st.dataframe(
        df[["date", "entity_type", "entity_code", "value", "buy", "sell", "nominal"]],
        use_container_width=True
    )


# -------------------------------------------------------
# STATISTICS
# -------------------------------------------------------

st.subheader("📊 Статистика")

col1, col2, col3, col4 = st.columns(4)

if entity_type == "metal":
    available_codes = sorted(df["entity_code"].unique().tolist())
    stat_code = st.selectbox(
        "Металл для статистики",
        available_codes,
        format_func=lambda c: metal_names.get(c, c)
    )
    df_stat = df[df["entity_code"] == stat_code].sort_values("date")
    price_series = df_stat["sell"]
    last_value = price_series.iloc[-1]
else:
    df_stat = df.sort_values("date")
    price_series = df_stat["value"]
    last_value = price_series.iloc[-1]

if len(price_series) > 7:
    change_7 = last_value - price_series.iloc[-7]
else:
    change_7 = 0.0

col1.metric("📌 Последнее значение", round(last_value, 4))
col2.metric("📈 Изменение за 7 точек", round(change_7, 4))
col3.metric("📉 Минимум", round(price_series.min(), 4))
col4.metric("📈 Максимум", round(price_series.max(), 4))


# -------------------------------------------------------
# CORRELATION WITH USD
# -------------------------------------------------------

if entity_type != "currency" and "USD" in dims["currency"]["char_code"].values:

    df_usd = fact[
        (fact["entity_type"] == "currency") &
        (fact["entity_code"] == "USD")
    ][["date", "value"]].rename(columns={"value": "usd_value"})

    if entity_type == "metal":
        merge_base = df_stat[["date", "sell"]]
        price_col = "sell"
    else:
        merge_base = df_stat[["date", "value"]]
        price_col = "value"

    merged = merge_base.merge(df_usd, on="date", how="inner")

    if len(merged) > 1:
        corr = merged[price_col].corr(merged["usd_value"])
        st.subheader("🔗 Корреляция с USD/RUB")
        st.metric("Корреляция", round(corr, 4))


st.success("Данные загружены успешно ✔")
