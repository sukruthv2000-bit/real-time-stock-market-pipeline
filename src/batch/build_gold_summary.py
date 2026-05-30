from pyspark.sql import SparkSession
from pyspark.sql.functions import first, max, min, last, sum, avg, round, col


SILVER_PATH = "data/silver/stock_metrics"
GOLD_PATH = "data/gold/daily_stock_summary"


spark = (
    SparkSession.builder
    .appName("BuildGoldDailyStockSummary")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(SILVER_PATH)

gold_df = (
    df.groupBy("event_date", "ticker")
    .agg(
        first("open").alias("open_price"),
        max("high").alias("high_price"),
        min("low").alias("low_price"),
        last("close").alias("close_price"),
        sum("volume").alias("total_volume"),
        round(avg("close"), 4).alias("avg_close_price"),
        round(avg("moving_avg_5_ticks"), 4).alias("avg_moving_price"),
    )
    .withColumn(
        "daily_return_pct",
        round(((col("close_price") - col("open_price")) / col("open_price")) * 100, 4)
    )
)

gold_df.write.mode("overwrite").partitionBy("event_date").parquet(GOLD_PATH)

print("Gold daily stock summary written successfully.")
gold_df.show(20, truncate=False)

spark.stop()
