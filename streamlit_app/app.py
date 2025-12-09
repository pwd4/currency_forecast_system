import os
import math
import io
import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# -------------------------------------------------------
# КОНФИГ
# -------------------------------------------------------

st.set_page_config(
    page_title="Панель анализа факторов рынка и курса рубля",
    page_icon="📊",
    layout="wide"
)

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


# -------------------------------------------------------
# ЗАГРУЗКА
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
# ПОДГОТОВКА
# -------------------------------------------------------

fact = load_fact()
dims = load_dimensions()

st.title("📈 Панель анализа факторов рынка и курса рубля")
st.markdown(
    "Данные загружаются из слоя **Data Mart**, построенного по методологии Kimball "
    "на основе слоя **Data Vault**."
)

fact = fact.sort_values("date")
fact["date"] = pd.to_datetime(fact["date"])

metal_names = {
    "1": "Золото — цена за 1 грамм (RUB)",
    "2": "Серебро — цена за 1 грамм (RUB)",
    "3": "Платина — цена за 1 грамм (RUB)",
    "4": "Палладий — цена за 1 грамм (RUB)"
}


# -------------------------------------------------------
# БОКОВОЕ МЕНЮ
# -------------------------------------------------------

st.sidebar.header("🔍 Фильтры")

entity_type = st.sidebar.selectbox(
    "Тип рынка",
    ["currency", "metal", "brent"],
    format_func=lambda x: {
        "currency": "Валюты к рублю",
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
    entity_code = None

st.sidebar.header("⚙ Параметры анализа")
ema_window = None
log_scale = False

if entity_type != "metal":
    ema_window = st.sidebar.selectbox(
        "Скользящая средняя (EMA, дней)",
        [None, 7, 14, 30],
        format_func=lambda x: "Нет" if x is None else str(x)
    )
    log_scale = st.sidebar.checkbox("Логарифмическая шкала по оси Y", value=False)


# -------------------------------------------------------
# ФИЛЬТР ДАННЫХ
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


# -------------------------------------------------------
# ВКЛАДКИ
# -------------------------------------------------------

tab_overview, tab_detail, tab_table, tab_ml = st.tabs(
    ["📊 Обзор рынка", "📉 Детализация по инструменту", "📄 Таблица", "📈 Прогноз (ML)"]
)


# =======================================================
# 📊 ВКЛАДКА — ОБЗОР РЫНКА
# =======================================================

with tab_overview:
    st.subheader("📊 Обзор ключевых рыночных показателей")

    # Brent vs USD
    st.markdown("#### 🛢 Связь средней цены Brent и курса USD/RUB")
    st.caption(
        "Диаграмма рассеяния показывает, как связаны средние цены нефти Brent и курс USD/RUB. "
        "Рост облака точек вправо-вверх означает одновременное удорожание доллара и нефти."
    )

    brent_all = fact[fact["entity_type"] == "brent"][["date", "entity_code", "value"]].copy()
    usd_all = fact[
        (fact["entity_type"] == "currency") &
        (fact["entity_code"] == "USD")
    ][["date", "value"]].rename(columns={"value": "usd_value"})

    if not brent_all.empty and not usd_all.empty:
        brent_pivot = brent_all.pivot_table(
            index="date", columns="entity_code", values="value", aggfunc="mean"
        )
        brent_pivot["brent_avg"] = brent_pivot.mean(axis=1, skipna=True)
        merged_scatter = brent_pivot[["brent_avg"]].reset_index().merge(
            usd_all, on="date", how="inner"
        )

        if len(merged_scatter) > 1:
            fig_scatter = px.scatter(
                merged_scatter,
                x="usd_value",
                y="brent_avg",
                labels={
                    "usd_value": "Курс USD/RUB",
                    "brent_avg": "Средняя цена Brent (USD/баррель)"
                },
            )
            fig_scatter.update_traces(marker=dict(size=8))
            fig_scatter.update_layout(height=400)
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.info("Недостаточно совпадающих дат для построения графика.")
    else:
        st.info("Недостаточно данных по Brent или USD.")

    # Boxplot + Heatmap
    col_bottom_left, col_bottom_right = st.columns(2)

    with col_bottom_left:
        st.markdown("#### 📦 Распределение цен драгоценных металлов")
        st.caption(
            "Диаграмма boxplot показывает статистическое распределение цен металлов. "
            "Граница коробки отражает диапазон от 25% до 75% значений, горизонтальная линия — медиана. "
            "Выбросы отображаются точками и позволяют оценить волатильность."
        )

        metals_all = fact[fact["entity_type"] == "metal"].copy()
        if not metals_all.empty:
            metals_all["metal_name"] = metals_all["entity_code"].map(metal_names)
            fig_box = px.box(
                metals_all,
                x="metal_name",
                y="sell",
                labels={"metal_name": "Металл", "sell": "Цена продажи (RUB)"},
            )
            fig_box.update_layout(height=400)
            st.plotly_chart(fig_box, use_container_width=True)
        else:
            st.info("Нет данных по металлам.")

    with col_bottom_right:
        st.markdown("#### 🔥 Корреляции основных валют к рублю")
        st.caption(
            "Тепловая карта показывает степень связи динамики курсов основных мировых валют "
            "по отношению к рублю. Значения ближе к +1 отражают сильную положительную корреляцию, "
            "ближе к −1 — противоположную динамику."
        )

        corr_df = fact[
            (fact["entity_type"] == "currency") &
            (fact["entity_code"].isin(["USD", "EUR", "GBP", "CNY", "JPY"]))
        ][["date", "entity_code", "value"]].copy()

        if not corr_df.empty:
            pivot_corr = corr_df.pivot_table(index="date", columns="entity_code", values="value") \
                .dropna(axis=0, how="any")

            if pivot_corr.shape[1] > 1:
                corr_matrix = pivot_corr.corr()
                fig_heat = px.imshow(
                    corr_matrix,
                    text_auto=True,
                    color_continuous_scale="RdBu",
                    zmin=-1, zmax=1,
                )
                fig_heat.update_layout(height=400)
                st.plotly_chart(fig_heat, use_container_width=True)
            else:
                st.info("Недостаточно данных для тепловой карты.")
        else:
            st.info("Недостаточно данных по валютам.")


# =======================================================
# 📉 ВКЛАДКА — ДЕТАЛИЗАЦИЯ
# =======================================================

with tab_detail:

    # Валюты
    if entity_type == "currency":
        y_col = "value"
        title = f"Курс валюты {entity_code} к рублю"

        fig = px.line(
            df, x="date", y=y_col,
            title=title,
            labels={"date": "Дата", y_col: "Курс (RUB)"},
            markers=True
        )

        if ema_window is not None and len(df) > ema_window:
            df["ema"] = df[y_col].ewm(span=ema_window, adjust=False).mean()
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["ema"],
                mode="lines", name=f"EMA {ema_window}",
                line=dict(width=2, dash="dash")
            ))

        if log_scale:
            fig.update_yaxes(type="log")

        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            "Линейный график показывает динамику курса выбранной валюты к рублю. "
            "Скользящая средняя (если выбрана) сглаживает колебания и подчеркивает общий тренд."
        )

    # Brent
    elif entity_type == "brent":
        y_col = "value"
        title = f"Цена нефти Brent ({entity_code.upper()})"

        fig = px.line(
            df, x="date", y=y_col,
            title=title,
            labels={"date": "Дата", y_col: "Цена (USD/баррель)"},
            markers=True
        )

        if ema_window is not None and len(df) > ema_window:
            df["ema"] = df[y_col].ewm(span=ema_window, adjust=False).mean()
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["ema"],
                mode="lines", name=f"EMA {ema_window}",
                line=dict(width=2, dash="dash")
            ))

        if log_scale:
            fig.update_yaxes(type="log")

        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            "График отражает динамику цены нефти Brent по выбранному источнику. "
            "Скользящая средняя помогает визуально оценить общий тренд без краткосрочного шума."
        )

    # Металлы
    else:
        st.subheader("💰 Цены драгоценных металлов (SELL, RUB)")

        fig = go.Figure()

        # Левая ось: золото, платина, палладий
        for code in ["1", "3", "4"]:
            df_m = df[df["entity_code"] == code]
            if not df_m.empty:
                fig.add_trace(go.Scatter(
                    x=df_m["date"],
                    y=df_m["sell"],
                    mode="lines",
                    name=metal_names[code],
                    line=dict(width=2),
                    yaxis="y1"
                ))

        # Правая ось: серебро
        df_silver = df[df["entity_code"] == "2"]
        if not df_silver.empty:
            fig.add_trace(go.Scatter(
                x=df_silver["date"],
                y=df_silver["sell"],
                mode="lines",
                name=metal_names["2"],
                line=dict(width=2, dash="dot"),
                yaxis="y2"
            ))

        fig.update_layout(
            height=500,
            xaxis=dict(title="Дата"),
            yaxis=dict(
                title="Цена (RUB) — Золото, Платина, Палладий",
                side="left"
            ),
            yaxis2=dict(
                title="Цена (RUB) — Серебро",
                overlaying="y",
                side="right",
                showgrid=False
            ),
            legend=dict(
                orientation="h",
                x=0,
                y=-0.2
            )
        )

        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            "Общий график показывает динамику всех драгоценных металлов. "
            "Золото, платина и палладий отображены по левой оси Y, серебро — по правой оси, "
            "чтобы его шкала была визуально сравнима с более дорогими металлами."
        )

    # Статистика
    st.subheader("📊 Статистика")
    st.caption(
        "Блок статистики позволяет оценить базовые характеристики временного ряда: "
        "последнее значение, изменение за несколько точек, минимальное и максимальное значения."
    )

    df_stat = df.sort_values("date")
    price_series = df_stat["sell"] if entity_type == "metal" else df_stat["value"]

    last_value = price_series.iloc[-1]
    change_7 = last_value - price_series.iloc[-7] if len(price_series) > 7 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("📌 Последнее значение", round(last_value, 4))
    col2.metric("📈 Изменение за 7 точек", round(change_7, 4))
    col3.metric("📉 Минимум", round(price_series.min(), 4))
    col4.metric("📈 Максимум", round(price_series.max(), 4))

    # Корреляция с USD
    if entity_type != "currency":
        df_usd = fact[
            (fact["entity_type"] == "currency") &
            (fact["entity_code"] == "USD")
        ][["date", "value"]].rename(columns={"value": "usd_value"})

        merge_base = df_stat[["date", "sell" if entity_type == "metal" else "value"]]
        merge_base.columns = ["date", "price"]

        merged_corr = merge_base.merge(df_usd, on="date", how="inner")

        if len(merged_corr) > 2:
            corr = merged_corr["price"].corr(merged_corr["usd_value"])
            st.subheader("🔗 Корреляция с USD/RUB")
            st.metric("Коэффициент", round(corr, 4))
            st.caption(
                "Коэффициент корреляции показывает, насколько изменения выбранного показателя "
                "согласованы с изменениями курса USD/RUB. Значения ближе к +1 означают сильную "
                "положительную связь."
            )


# =======================================================
# 📄 ВКЛАДКА — ТАБЛИЦА
# =======================================================

with tab_table:
    st.subheader("📄 Детали данных (факт)")
    st.caption(
        "Табличное представление исходных данных после загрузки в слой Data Mart. "
        "Используется для детальной проверки значений и сверки с графиками."
    )

    df_display = df.copy()

    if entity_type == "metal":
        df_display["entity_code"] = df_display["entity_code"].map(metal_names)

    df_display = df_display.rename(columns={
        "date": "Дата",
        "entity_type": "Тип сущности",
        "entity_code": "Название инструмента",
        "value": "Значение",
        "buy": "Покупка (BUY)",
        "sell": "Продажа (SELL)",
        "nominal": "Номинал"
    })

    st.dataframe(df_display, use_container_width=True)


# =======================================================
# 📈 ВКЛАДКА — ПРОГНОЗ (ML)
# =======================================================

with tab_ml:
    st.subheader("📈 Прогноз значений с помощью моделей машинного обучения")
    st.caption(
        "Этот модуль использует простые интерпретируемые модели (линейная регрессия и случайный лес) "
        "для прогнозирования будущих значений выбранного показателя на основе исторических данных."
    )

    st.markdown("### 📘 Как работает модуль прогнозирования")
    st.markdown(
        "- Исторические данные по выбранному инструменту (валюта, металл или нефть Brent) "
        "загружаются из слоя Data Mart.\n"
        "- Дата преобразуется во временной индекс (номер точки во временном ряду).\n"
        "- Модель обучается на историческом ряде и выявляет общий тренд зависимости значения от времени.\n"
        "- На основе этого тренда рассчитываются прогнозные значения на заданный горизонт (количество дней).\n"
        "- Для оценки качества прогноза рассчитываются метрики (MAE, RMSE, R²) на отложенной выборке.\n"
        "- Доверительный интервал строится как диапазон вокруг прогноза на основе разброса ошибок модели."
    )

    st.markdown("### 🔧 Настройки прогнозирования")
    st.caption(
        "Выберите инструмент, тип модели и горизонт прогноза. "
        "После нажатия кнопки модель будет обучена и построит прогноз."
    )

    ml_options = {
        "USD/RUB": ("currency", "USD"),
        "EUR/RUB": ("currency", "EUR"),
        "GBP/RUB": ("currency", "GBP"),
        "CNY/RUB": ("currency", "CNY"),
        "Золото (SELL)": ("metal", "1"),
        "Серебро (SELL)": ("metal", "2"),
        "Платина (SELL)": ("metal", "3"),
        "Палладий (SELL)": ("metal", "4"),
        "Brent (средняя цена)": ("brent", None)
    }

    selected_ml = st.selectbox(
        "Инструмент для прогнозирования:",
        list(ml_options.keys()),
        help="Выберите валюту, металл или нефть Brent, для которых требуется построить прогноз."
    )

    model_choice = st.selectbox(
        "Тип модели:",
        ["Линейная регрессия", "Случайный лес (RandomForest)"],
        help="Линейная регрессия строит прямолинейный тренд. Случайный лес позволяет учитывать более сложные зависимости."
    )

    horizon = st.slider(
        "Горизонт прогноза (дней):",
        min_value=7,
        max_value=60,
        value=14,
        help="Укажите, на сколько дней вперёд необходимо построить прогноз."
    )

    st.caption(
        "После настройки параметров нажмите кнопку ниже, чтобы обучить модель и построить прогноз."
    )
    run_button = st.button("🚀 Построить прогноз")

    if run_button:
        entity_type_ml, code_ml = ml_options[selected_ml]

        # Подготовка данных
        if entity_type_ml == "brent":
            df_ml = fact[fact["entity_type"] == "brent"].copy()
            df_ml = df_ml.groupby("date")["value"].mean().reset_index()
            df_ml.rename(columns={"value": "y"}, inplace=True)
        else:
            df_ml = fact[
                (fact["entity_type"] == entity_type_ml) &
                (fact["entity_code"] == str(code_ml))
            ].copy()

            if df_ml.empty:
                st.warning("Нет данных для выбранного инструмента.")
                st.stop()

            if entity_type_ml == "metal":
                df_ml = df_ml.rename(columns={"sell": "y"})
            else:
                df_ml = df_ml.rename(columns={"value": "y"})

        if df_ml.empty:
            st.warning("Нет данных для выбранного инструмента.")
        else:
            df_ml = df_ml.sort_values("date")
            df_ml["date_num"] = np.arange(len(df_ml))

            X_all = df_ml[["date_num"]].values
            y_all = df_ml["y"].values

            # Разделяем на обучающую и тестовую выборки
            if len(df_ml) >= 10:
                split_idx = int(len(df_ml) * 0.8)
                X_train, y_train = X_all[:split_idx], y_all[:split_idx]
                X_test, y_test = X_all[split_idx:], y_all[split_idx:]
            else:
                X_train, y_train = X_all, y_all
                X_test, y_test = None, None

            # Выбор модели
            if model_choice == "Линейная регрессия":
                model = LinearRegression()
            else:
                model = RandomForestRegressor(
                    n_estimators=200,
                    random_state=42,
                    n_jobs=-1
                )

            model.fit(X_train, y_train)

            # Оценка качества
            if X_test is not None and len(X_test) > 1:
                y_test_pred = model.predict(X_test)
                mae = mean_absolute_error(y_test, y_test_pred)
                mse = mean_squared_error(y_test, y_test_pred)
                rmse = math.sqrt(mse)
                r2 = r2_score(y_test, y_test_pred)
            else:
                # если мало точек, считаем метрики по обучающей выборке
                y_train_pred = model.predict(X_train)
                mae = mean_absolute_error(y_train, y_train_pred)
                mse = mean_squared_error(y_train, y_train_pred)
                rmse = math.sqrt(mse)
                r2 = r2_score(y_train, y_train_pred)

            st.markdown("### 📊 Качество модели")
            st.caption(
                "**MAE (Mean Absolute Error, средняя абсолютная ошибка)** показывает, насколько в среднем "
                "модель отклоняется от фактических значений в тех же единицах измерения, что и исходный ряд. "
                "Чем меньше MAE, тем точнее прогноз.\n\n"
                "**RMSE (Root Mean Squared Error, корень среднеквадратичной ошибки)** сильнее штрафует большие "
                "ошибки и отражает стабильность модели: крупные выбросы увеличивают RMSE. Чем меньше RMSE, "
                "тем более устойчивы прогнозы.\n\n"
                "**R² (коэффициент детерминации)** показывает, какую долю вариации данных модель смогла объяснить. "
                "Значение R², близкое к 1, означает хорошее качество, около 0 — модель не лучше простого среднего, "
                "отрицательные значения говорят о том, что модель хуже константного предсказания."
            )
            m1, m2, m3 = st.columns(3)
            m1.metric("MAE", f"{mae:.4f}")
            m2.metric("RMSE", f"{rmse:.4f}")
            m3.metric("R²", f"{r2:.4f}")

            # Переобучаем модель на всех данных для анализа остатков и прогноза
            model.fit(X_all, y_all)
            y_all_pred_full = model.predict(X_all)
            residuals = y_all - y_all_pred_full

            st.markdown("### 📉 График остатков (ошибок модели)")
            st.caption(
                "Остатки — это разница между фактическим значением показателя и прогнозом модели "
                "на той же дате. На графике по оси X отложены даты, по оси Y — величина ошибки. "
                "Если модель адекватно описывает данные, точки располагаются хаотично вокруг нулевой линии, "
                "без выраженного тренда или структуры. Наличие наклона, «воронки» или кластеров может говорить о том, "
                "что модель не учитывает какую-то закономерность (например, сезонность или нелинейность)."
            )

            fig_resid = go.Figure()
            fig_resid.add_trace(go.Scatter(
                x=df_ml["date"],
                y=residuals,
                mode="markers+lines",
                name="Остатки (факт − прогноз)",
            ))
            fig_resid.add_hline(y=0, line=dict(color="red", dash="dash"), name="Нулевая линия")
            fig_resid.update_layout(
                height=400,
                xaxis_title="Дата",
                yaxis_title="Ошибка (остаток)",
            )
            st.plotly_chart(fig_resid, use_container_width=True)

            st.markdown("### 📊 Распределение остатков (гистограмма ошибок)")
            st.caption(
                "Гистограмма показывает распределение ошибок модели. В идеальном случае остатки имеют "
                "симметричное распределение вокруг нуля, без ярко выраженного смещения в плюс или минус. "
                "Сильный перекос или длинные хвосты могут указывать на систематические ошибки модели "
                "(например, постоянное недооценивание или переоценивание показателя). Анализ распределения "
                "остатков помогает оценить, насколько выбранная модель подходит для данного временного ряда."
            )

            fig_resid_hist = px.histogram(
                x=residuals,
                nbins=20,
                labels={"x": "Ошибка (остаток)"},
            )
            fig_resid_hist.update_layout(
                height=400,
                xaxis_title="Ошибка (остаток)",
                yaxis_title="Частота",
                showlegend=False
            )
            st.plotly_chart(fig_resid_hist, use_container_width=True)

            # Простая оценка доверительного интервала по стандартному отклонению остатков
            if len(residuals) > 2:
                resid_std = np.std(residuals, ddof=1)
            else:
                resid_std = 0.0

            ci_k = 1.96

            # Прогноз
            future_nums = np.arange(len(df_ml), len(df_ml) + horizon)
            y_pred = model.predict(future_nums.reshape(-1, 1))

            lower = y_pred - ci_k * resid_std
            upper = y_pred + ci_k * resid_std

            future_dates = pd.date_range(
                df_ml["date"].iloc[-1] + pd.Timedelta(days=1),
                periods=horizon
            )

            df_pred = pd.DataFrame({
                "Дата": future_dates,
                "Прогноз": y_pred,
                "Нижняя граница (≈95%)": lower,
                "Верхняя граница (≈95%)": upper
            })

            st.markdown("### 📈 График фактических данных и прогноза")
            st.caption(
                "Синяя линия отражает исторические значения показателя. "
                "Красная пунктирная линия — прогноз модели на выбранный горизонт. "
                "Заштрихованная область вокруг прогноза соответствует приблизительному доверительному интервалу "
                "на уровне примерно 95%, рассчитанному на основе разброса ошибок модели на исторических данных. "
                "Следует учитывать, что при резких рыночных изменениях фактическая траектория может выйти за пределы "
                "этого интервала."
            )

            fig_ml = go.Figure()

            # Факт
            fig_ml.add_trace(go.Scatter(
                x=df_ml["date"],
                y=df_ml["y"],
                mode="lines+markers",
                name="Фактические данные",
                line=dict(color="blue")
            ))

            # Прогноз
            fig_ml.add_trace(go.Scatter(
                x=df_pred["Дата"],
                y=df_pred["Прогноз"],
                mode="lines+markers",
                name="Прогноз",
                line=dict(color="red", dash="dash")
            ))

            # Доверительный интервал
            if resid_std > 0:
                fig_ml.add_trace(go.Scatter(
                    x=df_pred["Дата"].tolist() + df_pred["Дата"].tolist()[::-1],
                    y=df_pred["Верхняя граница (≈95%)"].tolist() +
                      df_pred["Нижняя граница (≈95%)"].tolist()[::-1],
                    fill="toself",
                    fillcolor="rgba(255,0,0,0.1)",
                    line=dict(color="rgba(255,0,0,0)"),
                    hoverinfo="skip",
                    showlegend=True,
                    name="Доверительный интервал (≈95%)"
                ))

            fig_ml.update_layout(
                height=500,
                xaxis_title="Дата",
                yaxis_title="Значение"
            )

            st.plotly_chart(fig_ml, use_container_width=True)

            st.markdown("### 📄 Таблица прогнозируемых значений")
            st.caption(
                "Таблица содержит прогнозные значения показателя по датам, а также "
                "границы доверительного интервала. Эти данные можно использовать для включения в отчёт по ВКР "
                "и для дополнительного анализа."
            )
            st.dataframe(df_pred, use_container_width=True)

            st.markdown("### 📥 Экспорт прогноза в Excel")
            st.caption(
                "Кнопка ниже позволяет выгрузить исторические и прогнозные значения, а также границы "
                "доверительного интервала в файл Excel. Это удобно для документирования результатов, "
                "передачи данных и построения дополнительных отчётных диаграмм."
            )

            # Подготовка данных для экспорта
            df_hist_export = df_ml[["date", "y"]].rename(
                columns={"date": "Дата", "y": "Значение"}
            )
            df_hist_export["Тип"] = "Факт"

            df_pred_export = df_pred.copy()
            df_pred_export = df_pred_export.rename(columns={"Прогноз": "Значение"})
            df_pred_export["Тип"] = "Прогноз"

            df_export = pd.concat([df_hist_export, df_pred_export], ignore_index=True)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df_export.to_excel(writer, index=False, sheet_name="Прогноз")
            output.seek(0)

            st.download_button(
                label="📥 Скачать прогноз в Excel",
                data=output,
                file_name="forecast_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Скачать файл Excel с историческими и прогнозными значениями, "
                     "а также границами доверительного интервала."
            )

st.success("Данные загружены успешно ✔")
