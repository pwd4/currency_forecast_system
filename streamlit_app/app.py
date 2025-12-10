# ===============================================================
# Панель анализа факторов рынка и курса рубля
# ===============================================================

import os
import io
import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor


# -------------------------------------------------------
# КОНФИГ
# -------------------------------------------------------

st.set_page_config(
    page_title="Анализ рынка и курса рубля (учебный проект)",
    page_icon="📊",
    layout="wide"
)

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# Подписи металлов
METAL_LABELS = {
    "gold": "Золото — цена за 1 грамм (рубли)",
    "silver": "Серебро — цена за 1 грамм (рубли)",
    "platinum": "Платина — цена за 1 грамм (рубли)",
    "palladium": "Палладий — цена за 1 грамм (рубли)"
}


# -------------------------------------------------------
# ЗАГРУЗКА ДАННЫХ
# -------------------------------------------------------

@st.cache_data
def load_fact():
    """Загружает факт-таблицу и создаёт поле price — универсальная цена."""
    q = """
        SELECT f.*, d.date_value AS date
        FROM mart.fact_market_prices f
        JOIN mart.dim_date d ON d.date_hkey = f.date_hkey
        ORDER BY d.date_value;
    """
    df = pd.read_sql(q, engine)
    df["date"] = pd.to_datetime(df["date"])

    # Унифицированная цена:
    df["price"] = df["sell"].fillna(df["buy"]).fillna(df["value"])

    return df


@st.cache_data
def load_dims():
    return {
        "currency": pd.read_sql("SELECT * FROM mart.dim_currency", engine),
        "brent": pd.read_sql("SELECT * FROM mart.dim_brent", engine),
        "metal": pd.read_sql("SELECT * FROM mart.dim_metal", engine),
    }


fact = load_fact()
dims = load_dims()

st.title("📈 Панель анализа факторов рынка и курса рубля")


# -------------------------------------------------------
# БОКОВОЕ МЕНЮ
# -------------------------------------------------------

st.sidebar.header("🔍 Параметры анализа")

entity_type = st.sidebar.selectbox(
    "Тип инструмента:",
    ["currency", "metal", "brent"],
    format_func=lambda x: {
        "currency": "Валюты",
        "metal": "Драгоценные металлы",
        "brent": "Нефть Brent"
    }[x]
)

if entity_type == "currency":
    entity_code = st.sidebar.selectbox(
        "Валюта:",
        sorted(dims["currency"]["char_code"].unique())
    )

elif entity_type == "brent":
    entity_code = st.sidebar.selectbox(
        "Источник данных Brent:",
        dims["brent"]["source"].unique()
    )

elif entity_type == "metal":
    entity_code = st.sidebar.selectbox(
        "Металл:",
        ["gold", "silver", "platinum", "palladium"],
        format_func=lambda x: METAL_LABELS[x]
    )


# -------------------------------------------------------
# ФИЛЬТРАЦИЯ ДАННЫХ
# -------------------------------------------------------

if entity_type == "metal":
    df = fact[(fact["entity_type"] == "metal") &
              (fact["entity_code"] == entity_code)].copy()
else:
    df = fact[(fact["entity_type"] == entity_type) &
              (fact["entity_code"] == str(entity_code))].copy()

# Агрегация по дате (на случай дублей)
df = df.groupby("date", as_index=False).agg({"price": "mean"})

if df.empty:
    st.error("Нет данных для выбранного инструмента.")
    st.stop()


# -------------------------------------------------------
# ТАБЫ
# -------------------------------------------------------

tab_overview, tab_detail, tab_table, tab_ml = st.tabs([
    "📊 Обзор рынка",
    "📉 Детализация инструмента",
    "📄 Таблица",
    "🤖 Прогноз (ML)"
])


# =======================================================
# ОБЗОР РЫНКА — 3 обучающих графика
# =======================================================

with tab_overview:

    st.header("📊 Обзор ключевых рыночных индикаторов")

    # ------------------------
    # 1️ Корреляции валют
    # ------------------------

    st.subheader("1️⃣ Корреляционная матрица валют")
    st.caption(
        "График показывает, насколько изменения различных валют связаны между собой. "
        "Красный — сильная положительная связь, синий — отрицательная."
    )

    df_curr = fact[fact["entity_type"] == "currency"][["date", "entity_code", "price"]]
    pivot = df_curr.pivot_table(index="date", columns="entity_code", values="price").dropna()

    if pivot.shape[1] >= 2:
        fig_corr = px.imshow(
            pivot.corr(),
            text_auto=True,
            color_continuous_scale="RdBu",
            zmin=-1,
            zmax=1,
            labels={"color": "Коэффициент корреляции"}
        )
        st.plotly_chart(fig_corr, use_container_width=True)

    # ------------------------
    # 2️ Средние цены металлов
    # ------------------------

    st.subheader("2️⃣ Средние цены драгоценных металлов")
    st.caption("Средние значения цен по каждому металлу за весь период наблюдений.")

    df_m = fact[fact["entity_type"] == "metal"].copy()
    df_m["Название"] = df_m["entity_code"].map(METAL_LABELS)
    df_m = df_m.groupby("Название", as_index=False)["price"].mean()

    fig_bar = px.bar(
        df_m,
        x="Название",
        y="price",
        labels={"price": "Средняя цена (руб)", "Название": "Металл"},
        text_auto=".2f"
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # ------------------------
    # 3 Brent vs USD/RUB
    # ------------------------

    st.subheader("3️⃣ Динамика цены Brent и курса USD/RUB")
    st.caption(
        "На графике видно, как цена нефти Brent связана с курсом доллара. "
        "Это классический учебный пример макроэкономической зависимости."
    )

    br = fact[fact["entity_type"] == "brent"].groupby("date")["price"].mean().reset_index()
    usd = fact[(fact["entity_type"] == "currency") & (fact["entity_code"] == "USD")] \
        .groupby("date")["price"].mean().reset_index()

    merged = br.merge(usd, on="date", suffixes=("_brent", "_usd"))

    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(
        x=merged["date"], y=merged["price_brent"], mode="lines",
        name="Цена Brent (USD/баррель)"
    ))
    fig_line.add_trace(go.Scatter(
        x=merged["date"], y=merged["price_usd"], mode="lines",
        name="Курс USD/RUB"
    ))
    fig_line.update_layout(
        xaxis_title="Дата",
        yaxis_title="Значения показателей"
    )

    st.plotly_chart(fig_line, use_container_width=True)


# =======================================================
# 4 ДЕТАЛИЗАЦИЯ ИНСТРУМЕНТА
# =======================================================

with tab_detail:

    st.header("📉 Детализация выбранного инструмента")

    if entity_type == "metal":

        st.subheader("График цен всех четырёх драгоценных металлов")
        st.caption(
            "Общий график помогает сравнить динамику золота, серебра, платины и палладия. "
            "Серебро вынесено на отдельную ось из-за меньшего масштаба цен."
        )

        df_all = fact[fact["entity_type"] == "metal"].copy()
        df_all["price"] = df_all["sell"].fillna(df_all["buy"]).fillna(df_all["value"])
        df_all["Название"] = df_all["entity_code"].map(METAL_LABELS)

        df_all = df_all.groupby(["date", "Название"], as_index=False)["price"].mean()

        fig = go.Figure()

        for code in ["gold", "platinum", "palladium"]:
            name = METAL_LABELS[code]
            d = df_all[df_all["Название"] == name]
            fig.add_trace(go.Scatter(x=d["date"], y=d["price"], mode="lines", name=name, yaxis="y1"))

        # Серебро — отдельная ось
        d_s = df_all[df_all["Название"] == METAL_LABELS["silver"]]
        fig.add_trace(go.Scatter(
            x=d_s["date"], y=d_s["price"], mode="lines",
            name=METAL_LABELS["silver"], line=dict(dash="dot"), yaxis="y2"
        ))

        fig.update_layout(
            height=500,
            xaxis=dict(title="Дата"),
            yaxis=dict(title="Цена (руб) — золото / платина / палладий"),
            yaxis2=dict(
                title="Цена (руб) — серебро (масштаб другой)",
                overlaying="y",
                side="right",
                showgrid=False,
            )
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        fig = px.line(df, x="date", y="price",
                      labels={"date": "Дата", "price": "Значение показателя"})
        st.plotly_chart(fig, use_container_width=True)


# =======================================================
# ТАБЛИЦА ДАННЫХ
# =======================================================

with tab_table:

    st.header("📄 Таблица исходных данных")

    d = df.copy()
    d = d.rename(columns={
        "date": "Дата",
        "price": "Цена"
    })

    st.dataframe(d.sort_values("Дата"), use_container_width=True)


# =======================================================
# МАШИННОЕ ОБУЧЕНИЕ — ПРОГНОЗ
# =======================================================

with tab_ml:

    st.header("🤖 Прогноз временного ряда")

    st.caption(
        "Здесь можно построить простой учебный прогноз с помощью линейной регрессии "
        "или случайного леса. Модель обучается только по историческим данным "
        "выбранного инструмента."
    )

    ml_items = {
        "USD/RUB": ("currency", "USD"),
        "EUR/RUB": ("currency", "EUR"),
        "CNY/RUB": ("currency", "CNY"),
        "Золото": ("metal", "gold"),
        "Серебро": ("metal", "silver"),
        "Платина": ("metal", "platinum"),
        "Палладий": ("metal", "palladium"),
        "Brent": ("brent", None)
    }

    selected = st.selectbox("Выберите инструмент:", list(ml_items.keys()))
    model_type = st.selectbox("Тип модели:", ["Линейная регрессия", "Случайный лес"])
    horizon = st.slider("Горизонт прогноза (дней):", 7, 60, 14)

    if st.button("Построить прогноз 🚀"):

        etype, code = ml_items[selected]

        if etype == "brent":
            df_ml = fact[fact["entity_type"] == "brent"].groupby("date")["price"].mean().reset_index()
        else:
            df_ml = fact[(fact["entity_type"] == etype) &
                         (fact["entity_code"] == str(code))] \
                         .groupby("date")["price"].mean().reset_index()

        df_ml = df_ml.sort_values("date")
        df_ml["t"] = np.arange(len(df_ml))

        X = df_ml[["t"]].values
        y = df_ml["price"].values

        if model_type == "Линейная регрессия":
            model = LinearRegression()
        else:
            model = RandomForestRegressor(n_estimators=300)

        model.fit(X, y)

        future_t = np.arange(len(df_ml), len(df_ml) + horizon)
        pred = model.predict(future_t.reshape(-1, 1))

        future_dates = pd.date_range(df_ml["date"].iloc[-1] + pd.Timedelta(days=1), periods=horizon)

        df_pred = pd.DataFrame({
            "Дата": future_dates,
            "Прогноз": pred
        })

        fig_ml = go.Figure()
        fig_ml.add_trace(go.Scatter(x=df_ml["date"], y=df_ml["price"], mode="lines", name="Фактические данные"))
        fig_ml.add_trace(go.Scatter(x=df_pred["Дата"], y=df_pred["Прогноз"],
                                    mode="lines+markers", name="Прогноз"))

        fig_ml.update_layout(xaxis_title="Дата", yaxis_title="Цена / значение")

        st.plotly_chart(fig_ml, use_container_width=True)

        st.dataframe(df_pred, use_container_width=True)

        out = io.BytesIO()
        with pd.ExcelWriter(out) as writer:
            df_pred.to_excel(writer, index=False)
        out.seek(0)

        st.download_button("Скачать прогноз в Excel", out, "forecast.xlsx")
