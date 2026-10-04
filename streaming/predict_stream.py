import os

from pymongo import MongoClient, ReplaceOne
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, dayofweek, from_json, greatest, hour, least, lit, to_timestamp, when
from pyspark.sql.types import StringType, StructField, StructType


spark = (
    SparkSession.builder.appName("taxi-hotspot-streaming-predictor")
    .config("spark.sql.session.timeZone", "America/New_York")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
model = PipelineModel.load(os.getenv("HOTSPOT_MODEL_PATH", "file:///models/hotspot_model"))
demand_model = PipelineModel.load(os.getenv("DEMAND_MODEL_PATH", "file:///models/demand_model"))
baseline_path = os.getenv("HISTORICAL_BASELINE_TABLE_PATH", "file:///models/historical_baseline_table")
historical_baseline = spark.read.parquet(baseline_path).select(
    "pickup_zone",
    "pickup_hour",
    "pickup_dow",
    "mean_trip_count",
    "stddev_trip_count",
    "anomaly_threshold",
    "sample_count",
)

schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("pickup_datetime", StringType()),
        StructField("pickup_zone", StringType()),
        StructField("dropoff_zone", StringType()),
    ]
)


def write_predictions(batch, batch_id):
    if batch.rdd.isEmpty():
        return

    hotspot_predictions = (
        model.transform(batch)
        .select(
            "event_id",
            "pickup_datetime",
            "event_time",
            "pickup_zone",
            "dropoff_zone",
            "pickup_hour",
            "pickup_dow",
            least(greatest(col("predicted_hotspot_score"), lit(0.0)), lit(100.0))
            .cast("double")
            .alias("hotspot_score"),
        )
    )
    demand_predictions = demand_model.transform(batch).select(
        "event_id", greatest(col("predicted_demand"), lit(0.0)).cast("double").alias("predicted_demand")
    )

    predictions = (
        hotspot_predictions.join(demand_predictions, "event_id")
        .join(
            broadcast(historical_baseline),
            ["pickup_zone", "pickup_hour", "pickup_dow"],
            "left",
        )
        .withColumn(
            "hotspot_level",
            when(col("hotspot_score") >= 75, "Rất nóng")
            .when(col("hotspot_score") >= 50, "Nóng")
            .when(col("hotspot_score") >= 25, "Trung bình")
            .otherwise("Thấp"),
        )
        .withColumn(
            "alert_flag",
            when(col("sample_count").isNull() | (col("sample_count") < 5), lit("INSUFFICIENT_BASELINE"))
            .when(col("predicted_demand") > col("anomaly_threshold"), lit("CRITICAL_ANOMALY"))
            .otherwise(lit("NORMAL")),
        )
        .withColumn("baseline_unit", lit("trips_per_hour_same_weekday"))
        .withColumn("prediction_unit", lit("trips_per_hour_same_weekday"))
        .withColumn("batch_id", lit(batch_id))
        .collect()
    )
    documents = [row.asDict(recursive=True) for row in predictions]
    if not documents:
        return

    client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
    collection = client[os.getenv("MONGO_DATABASE", "taxi")][
        os.getenv("MONGO_COLLECTION", "hotspot_predictions")
    ]
    collection.bulk_write(
        [ReplaceOne({"event_id": document["event_id"]}, document, upsert=True) for document in documents],
        ordered=False,
    )
    client.close()
    alerts = sum(document["alert_flag"] == "CRITICAL_ANOMALY" for document in documents)
    insufficient = sum(document["alert_flag"] == "INSUFFICIENT_BASELINE" for document in documents)
    print(
        f"[PASS] MongoDB predictions={len(documents)}; critical_demand_alerts={alerts}; "
        f"insufficient_baseline={insufficient}; batch={batch_id}",
        flush=True,
    )


events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    .option("subscribe", os.getenv("KAFKA_TOPIC", "taxi_trips"))
    .option("startingOffsets", os.getenv("STREAM_STARTING_OFFSETS", "latest"))
    .option("failOnDataLoss", "false")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("event"))
    .select("event.*")
    .withColumn("event_time", to_timestamp("pickup_datetime"))
    .filter(col("event_time").isNotNull())
    .withColumn("pickup_hour", hour("event_time"))
    .withColumn("pickup_dow", dayofweek("event_time") - 1)
)

query = (
    events.writeStream.foreachBatch(write_predictions)
    .option("checkpointLocation", os.getenv("CHECKPOINT_LOCATION", "/checkpoints/taxi-hotspot"))
    .trigger(processingTime="2 seconds")
    .start()
)
print("[PASS] Streaming query started with demand baseline broadcast join", flush=True)
query.awaitTermination()
