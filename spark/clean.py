from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    dayofweek,
    hour,
    monotonically_increasing_id,
    sha2,
    to_timestamp,
    trim,
)


spark = SparkSession.builder.appName("taxi-hotspot-etl").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    raw = spark.read.parquet("hdfs://namenode:9000/taxi/raw/parquet")
    print("[INFO] Reading NYC TLC Parquet from HDFS", flush=True)
except Exception:
    raw = spark.read.option("header", True).option("inferSchema", True).csv(
        "hdfs://namenode:9000/taxi/raw/taxi_trips.csv"
    )
    print("[INFO] Reading synthetic CSV fallback from HDFS", flush=True)
if "tpep_pickup_datetime" in raw.columns:
    if "event_id" not in raw.columns:
        raw = raw.withColumn(
            "event_id",
            sha2(
                concat_ws(
                    "|",
                    col("tpep_pickup_datetime").cast("string"),
                    col("PULocationID").cast("string"),
                    col("DOLocationID").cast("string"),
                    monotonically_increasing_id().cast("string"),
                ),
                256,
            ),
        )
    normalized = raw.select(
        col("event_id").cast("string").alias("event_id"),
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("PULocationID").cast("string").alias("pickup_zone"),
        col("DOLocationID").cast("string").alias("dropoff_zone"),
    )
else:
    normalized = raw.select("event_id", "pickup_datetime", "pickup_zone", "dropoff_zone")

cleaned = (
    normalized
    .withColumn("event_time", to_timestamp(col("pickup_datetime")))
    .withColumn("pickup_zone", trim(col("pickup_zone")))
    .withColumn("dropoff_zone", trim(col("dropoff_zone")))
    .dropna(subset=["event_id", "event_time", "pickup_zone"])
    .dropDuplicates(["event_id"])
    .withColumn("pickup_hour", hour("event_time"))
    .withColumn("pickup_dow", dayofweek("event_time") - 1)
)

cleaned.write.mode("overwrite").parquet("hdfs://namenode:9000/taxi/curated/trips")
cleaned.write.mode("overwrite").parquet("file:///data/curated/trips")
open("/data/results/spark_etl_SUCCESS", "w", encoding="utf-8").close()
print("[PASS] Spark ETL completed", flush=True)
print("[PASS] Parquet generated", flush=True)
spark.stop()
