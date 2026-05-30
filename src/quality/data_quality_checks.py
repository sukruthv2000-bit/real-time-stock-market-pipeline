from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("DataQualityChecks")
    .getOrCreate()
)

BRONZE_PATH = "data/bronze/stock_prices"

df = spark.read.parquet(BRONZE_PATH)

print("\n===== DATA QUALITY REPORT =====")

total_records = df.count()
print(f"Total Records: {total_records}")

required_columns = [
    "ticker",
    "event_time",
    "open",
    "high",
    "low",
    "close",
    "volume"
]

for column_name in required_columns:
    null_count = df.filter(col(column_name).isNull()).count()
    print(f"Null Count [{column_name}]: {null_count}")

duplicate_count = (
    total_records -
    df.dropDuplicates().count()
)

print(f"Duplicate Records: {duplicate_count}")

print("\nSchema Validation")
df.printSchema()

print("\n===== QUALITY CHECK COMPLETE =====")

spark.stop()
