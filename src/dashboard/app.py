import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
from src.utils.config_loader import load_config
import pandas as pd
import streamlit as st
import plotly.express as px
from pyspark.sql import SparkSession


config = load_config()

GOLD_PATH = config["paths"]["gold"]
SILVER_PATH = config["paths"]["silver"]

st.set_page_config(
    page_title="Real-Time Stock Market Analytics",
    layout="wide"
)

st.title("Real-Time Stock Market Analytics Dashboard")
st.caption("Kafka → Spark Structured Streaming → Bronze/Silver/Gold Parquet → Streamlit")


@st.cache_data
def load_gold_data():
    spark = SparkSession.builder.appName("StockDashboardGold").getOrCreate()
    pdf = spark.read.parquet(GOLD_PATH).toPandas()
    spark.stop()
    return pdf


@st.cache_data
def load_silver_data():
    spark = SparkSession.builder.appName("StockDashboardSilver").getOrCreate()
    pdf = spark.read.parquet(SILVER_PATH).toPandas()
    spark.stop()
    return pdf


gold_df = load_gold_data()
silver_df = load_silver_data()

gold_df["event_date"] = pd.to_datetime(gold_df["event_date"])
silver_df["event_timestamp"] = pd.to_datetime(silver_df["event_timestamp"])

st.sidebar.header("Filters")

tickers = sorted(gold_df["ticker"].unique())

selected_tickers = st.sidebar.multiselect(
    "Select Tickers",
    tickers,
    default=tickers
)

filtered_gold = gold_df[gold_df["ticker"].isin(selected_tickers)]
filtered_silver = silver_df[silver_df["ticker"].isin(selected_tickers)]

if filtered_gold.empty or filtered_silver.empty:
    st.warning("Please select at least one ticker.")
    st.stop()

top_gainer_row = filtered_gold.sort_values("daily_return_pct", ascending=False).iloc[0]
top_loser_row = filtered_gold.sort_values("daily_return_pct", ascending=True).iloc[0]
top_volume_row = filtered_gold.sort_values("total_volume", ascending=False).iloc[0]

total_volume = int(filtered_gold["total_volume"].sum())
avg_return = round(filtered_gold["daily_return_pct"].mean(), 4)

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Volume", f"{total_volume:,}")
col2.metric("Average Return %", f"{avg_return}%")
col3.metric(
    "Top Gainer",
    top_gainer_row["ticker"],
    f'{round(top_gainer_row["daily_return_pct"], 4)}%'
)
col4.metric(
    "Most Traded",
    top_volume_row["ticker"],
    f'{int(top_volume_row["total_volume"]):,} volume'
)

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Daily Return by Ticker")
    fig_return = px.bar(
        filtered_gold.sort_values("daily_return_pct", ascending=False),
        x="ticker",
        y="daily_return_pct",
        text="daily_return_pct",
        title="Daily Return %",
        color="daily_return_pct",
        color_continuous_scale=["red", "lightgray", "green"],
    )
    fig_return.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig_return.update_layout(showlegend=False)
    st.plotly_chart(fig_return, use_container_width=True)

with right:
    st.subheader("Volume Leaderboard")
    fig_volume = px.bar(
        filtered_gold.sort_values("total_volume", ascending=False),
        x="ticker",
        y="total_volume",
        text="total_volume",
        title="Total Volume by Ticker",
        color="ticker",
    )
    fig_volume.update_traces(texttemplate="%{text:,}", textposition="outside")
    st.plotly_chart(fig_volume, use_container_width=True)

st.subheader("Stock Price Trend")

fig_price = px.line(
    filtered_silver.sort_values("event_timestamp"),
    x="event_timestamp",
    y="close",
    color="ticker",
    markers=True,
    title="Close Price Trend by Ticker"
)

st.plotly_chart(fig_price, use_container_width=True)

left2, right2 = st.columns(2)

with left2:
    st.subheader("OHLC Comparison")
    ohlc_melted = filtered_gold.melt(
        id_vars=["ticker"],
        value_vars=["open_price", "high_price", "low_price", "close_price"],
        var_name="metric",
        value_name="price"
    )

    fig_ohlc = px.bar(
        ohlc_melted,
        x="ticker",
        y="price",
        color="metric",
        barmode="group",
        title="Open, High, Low, Close by Ticker"
    )
    st.plotly_chart(fig_ohlc, use_container_width=True)

with right2:
    st.subheader("Top Gainer / Loser Summary")

    summary_df = pd.DataFrame({
        "Metric": ["Top Gainer", "Top Loser", "Most Traded"],
        "Ticker": [
            top_gainer_row["ticker"],
            top_loser_row["ticker"],
            top_volume_row["ticker"],
        ],
        "Value": [
            f'{round(top_gainer_row["daily_return_pct"], 4)}%',
            f'{round(top_loser_row["daily_return_pct"], 4)}%',
            f'{int(top_volume_row["total_volume"]):,}',
        ],
    })

    st.dataframe(summary_df, use_container_width=True, hide_index=True)

st.subheader("Gold Layer Dataset")
st.dataframe(
    filtered_gold.sort_values(["event_date", "ticker"]),
    use_container_width=True,
    hide_index=True
)
