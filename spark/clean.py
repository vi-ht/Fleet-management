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
from pyspark.sql.functions import broadcast, count, sum as spark_sum, when


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

lookup_path = "file:///data/reference/taxi_zone_lookup.csv"
lookup = (
    spark.read.option("header", True).option("inferSchema", True).csv(lookup_path)
    .select(
        col("LocationID").cast("string").alias("lookup_zone_id"),
        col("Borough").alias("zone_borough"),
        col("Zone").alias("zone_name"),
        col("service_zone").alias("zone_service_area"),
    )
)
duplicate_lookup_ids = lookup.groupBy("lookup_zone_id").count().where(col("count") > 1).count()
if duplicate_lookup_ids:
    raise ValueError(f"Taxi Zone Lookup has {duplicate_lookup_ids} duplicate LocationIDs")

pickup_lookup = lookup.select(
    col("lookup_zone_id").alias("pickup_zone"),
    col("zone_borough").alias("pickup_borough"),
    col("zone_name").alias("pickup_zone_name"),
    col("zone_service_area").alias("pickup_service_zone"),
)
dropoff_lookup = lookup.select(
    col("lookup_zone_id").alias("dropoff_zone"),
    col("zone_borough").alias("dropoff_borough"),
    col("zone_name").alias("dropoff_zone_name"),
    col("zone_service_area").alias("dropoff_service_zone"),
)
enriched = (
    cleaned.join(broadcast(pickup_lookup), on="pickup_zone", how="left")
    .join(broadcast(dropoff_lookup), on="dropoff_zone", how="left")
)
quality = enriched.agg(
    count("*").alias("after_enrichment"),
    spark_sum(when(col("pickup_zone_name").isNull(), 1).otherwise(0)).alias(
        "unmatched_pickup"
    ),
    spark_sum(when(col("dropoff_zone_name").isNull(), 1).otherwise(0)).alias(
        "unmatched_dropoff"
    ),
).first()
after_enrichment = quality["after_enrichment"]
before_enrichment = cleaned.count()
if before_enrichment != after_enrichment:
    raise ValueError(
        f"Taxi Zone Lookup changed row count: {before_enrichment} -> {after_enrichment}"
    )
print(
    "[PASS] Taxi Zone Lookup enrichment: "
    f"lookup_rows={lookup.count()}, rows_before={before_enrichment}, "
    f"rows_after={after_enrichment}, "
    f"unmatched_pickup={quality['unmatched_pickup'] or 0}, "
    f"unmatched_dropoff={quality['unmatched_dropoff'] or 0}; broadcast joins",
    flush=True,
)

enriched.write.mode("overwrite").parquet("hdfs://namenode:9000/taxi/curated/trips")
enriched.write.mode("overwrite").parquet("file:///data/curated/trips")
open("/data/results/spark_etl_SUCCESS", "w", encoding="utf-8").close()
print("[PASS] Spark ETL completed", flush=True)
print("[PASS] Parquet generated", flush=True)
spark.stop()
