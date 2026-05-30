mport sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
from src.utils.config_loader import load_config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, round, when
from pyspark.sql.window import Window


config = load_config()

BRONZE_PATH = config["paths"]["bronze"]
SILVER_PATH = config["paths"]["silver"]

spark = (
    SparkSession.builder
    .appName("BuildSilverStockMetrics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


df = spark.read.parquet(BRONZE_PATH)

window_by_ticker = Window.partitionBy("ticker").orderBy("event_timestamp")
window_5 = Window.partitionBy("ticker").orderBy("event_timestamp").rowsBetween(-4, 0)

silver_df = (
    df
    .withColumn("previous_close", lag("close").over(window_by_ticker))
    .withColumn(
        "price_change_pct",
        round(((col("close") - col("previous_close")) / col("previous_close")) * 100, 4)
    )
    .withColumn("moving_avg_5_ticks", round(avg("close").over(window_5), 4))
    .withColumn(
        "price_alert",
        when(col("price_change_pct") >= 2, "PRICE_JUMP")
        .when(col("price_change_pct") <= -2, "PRICE_DROP")
        .otherwise("NORMAL")
    )
    .select(
        "ticker",
        "event_time",
        "event_timestamp",
        "event_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "previous_close",
        "price_change_pct",
        "moving_avg_5_ticks",
        "price_alert",
    )
)

silver_df.write.mode("overwrite").partitionBy("event_date", "ticker").parquet(SILVER_PATH)

print("Silver stock metrics written successfully.")
silver_df.show(20, truncate=False)

spark.stop()
