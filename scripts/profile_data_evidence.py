import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, max as spark_max, min as spark_min, sum as spark_sum, when, year


spark = SparkSession.builder.master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")).appName("taxi-evidence-data-profile").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

raw_path = "hdfs://namenode:9000/taxi/raw/parquet"
curated_path = "hdfs://namenode:9000/taxi/curated/trips"
raw = spark.read.parquet(raw_path)
curated = spark.read.parquet(curated_path)

raw_required = [name for name in ("tpep_pickup_datetime", "PULocationID", "DOLocationID") if name in raw.columns]
curated_required = [name for name in ("event_id", "pickup_datetime", "pickup_zone") if name in curated.columns]

raw_summary = raw.agg(
    count("*").alias("row_count"),
    *[spark_sum(when(col(name).isNull(), 1).otherwise(0)).alias(f"null_{name}") for name in raw_required],
    spark_min("tpep_pickup_datetime").cast("string").alias("pickup_min"),
    spark_max("tpep_pickup_datetime").cast("string").alias("pickup_max"),
).first().asDict()
raw_year_counts = [
    {"year": row["pickup_year"], "row_count": row["count"]}
    for row in raw.groupBy(year("tpep_pickup_datetime").alias("pickup_year")).count().orderBy("pickup_year").collect()
]
curated_summary = curated.agg(
    count("*").alias("row_count"),
    *[spark_sum(when(col(name).isNull(), 1).otherwise(0)).alias(f"null_{name}") for name in curated_required],
    spark_min("pickup_datetime").cast("string").alias("pickup_min"),
    spark_max("pickup_datetime").cast("string").alias("pickup_max"),
    countDistinct("pickup_zone").alias("distinct_pickup_zones"),
).first().asDict()

profile = {
    "source": "NYC TLC Yellow Taxi Trip Records",
    "raw": {
        "hdfs_path": raw_path,
        "format": "Parquet",
        "columns": raw.columns,
        "schema": json.loads(raw.schema.json()),
        "row_count": raw_summary.pop("row_count"),
        "pickup_year_distribution": raw_year_counts,
        "pickup_datetime_min": raw_summary.pop("pickup_min"),
        "pickup_datetime_max": raw_summary.pop("pickup_max"),
        "null_counts_on_pipeline_keys": raw_summary,
    },
    "curated": {
        "hdfs_path": curated_path,
        "format": "Parquet",
        "columns": curated.columns,
        "schema": json.loads(curated.schema.json()),
        "row_count": curated_summary.pop("row_count"),
        "pickup_datetime_min": curated_summary.pop("pickup_min"),
        "pickup_datetime_max": curated_summary.pop("pickup_max"),
        "distinct_pickup_zones": curated_summary.pop("distinct_pickup_zones"),
        "null_counts_on_pipeline_keys": curated_summary,
    },
}
print(json.dumps(profile, ensure_ascii=False, indent=2))
spark.stop()
