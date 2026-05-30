import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
from src.utils.config_loader import load_config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


config = load_config()

KAFKA_BOOTSTRAP_SERVER = config["kafka"]["bootstrap_server"]
KAFKA_TOPIC = config["kafka"]["topic"]

BRONZE_OUTPUT_PATH = config["paths"]["bronze"]
CHECKPOINT_PATH = config["paths"]["checkpoint"]

schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
])


spark = (
    SparkSession.builder
    .appName("RealTimeStockMarketStreaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_timestamp", to_timestamp(col("event_time")))
    .withColumn("event_date", current_date())
)

query = (
    parsed_df.writeStream
    .format("parquet")
    .option("path", BRONZE_OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("event_date", "ticker")
    .outputMode("append")
    .start()
)

print("Spark streaming job started. Writing Kafka data to bronze Parquet layer...")

query.awaitTermination()
