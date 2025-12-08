import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine
import plotly.express as px

st.title("📈 Currency Forecast Dashboard")

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

@st.cache_data
def load_df(query):
    return pd.read_sql(query, engine)

st.header("USD/RUB rate")
try:
    df = load_df("SELECT date, value FROM currency_rates WHERE code = 'USD' ORDER BY date")
    fig = px.line(df, x="date", y="value")
    st.plotly_chart(fig)
except Exception as e:
    st.error(f"Error: {e}")
