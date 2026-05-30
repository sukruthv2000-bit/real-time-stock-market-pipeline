import pandas as pd
import streamlit as st
import plotly.express as px
from pyspark.sql import SparkSession


GOLD_PATH = "data/gold/daily_stock_summary"


st.set_page_config(
    page_title="Real-Time Stock Market Analytics",
    layout="wide"
)

st.title("Real-Time Stock Market Analytics Dashboard")

spark = (
    SparkSession.builder
    .appName("StockDashboard")
    .getOrCreate()
)

df = spark.read.parquet(GOLD_PATH).toPandas()
spark.stop()

st.sidebar.header("Filters")
tickers = sorted(df["ticker"].unique())
selected_tickers = st.sidebar.multiselect(
    "Select Tickers",
    tickers,
    default=tickers
)

filtered_df = df[df["ticker"].isin(selected_tickers)]

total_volume = int(filtered_df["total_volume"].sum())
avg_return = round(filtered_df["daily_return_pct"].mean(), 4)
best_stock = filtered_df.sort_values("daily_return_pct", ascending=False).iloc[0]["ticker"]

col1, col2, col3 = st.columns(3)
col1.metric("Total Volume", f"{total_volume:,}")
col2.metric("Average Daily Return %", avg_return)
col3.metric("Top Performer", best_stock)

st.subheader("Daily Return by Ticker")
fig_return = px.bar(
    filtered_df,
    x="ticker",
    y="daily_return_pct",
    color="ticker",
    title="Daily Return %"
)
st.plotly_chart(fig_return, use_container_width=True)

st.subheader("Volume by Ticker")
fig_volume = px.bar(
    filtered_df,
    x="ticker",
    y="total_volume",
    color="ticker",
    title="Total Volume"
)
st.plotly_chart(fig_volume, use_container_width=True)

st.subheader("OHLC Summary")
st.dataframe(filtered_df)
