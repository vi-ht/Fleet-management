import os
import json
import sys
from pathlib import Path

from pymongo import MongoClient, ReplaceOne
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    concat,
    col,
    date_format,
    dayofweek,
    from_json,
    greatest,
    hour,
    least,
    lit,
    lpad,
    to_timestamp,
    when,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from weather import WEATHER_FIELDS, get_forecast, historical_baseline_threshold, weather_values_for_hour


spark = (
    SparkSession.builder.appName("taxi-hotspot-streaming-predictor")
    .config("spark.sql.session.timeZone", "America/New_York")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
model_path = os.getenv("HOTSPOT_MODEL_PATH", "file:///models/hotspot_model")
model = PipelineModel.load(model_path)
results_path = Path(os.getenv("RESULTS_DIR", "/data/results"))
try:
    model_metrics = json.loads((results_path / "model_metrics.json").read_text(encoding="utf-8"))
except (OSError, ValueError):
    model_metrics = {}
try:
    historical_baseline = json.loads(
        Path(os.getenv("HISTORICAL_BASELINE_PATH", "/models/historical_baseline.json")).read_text(encoding="utf-8")
    )
except (OSError, ValueError):
    historical_baseline = {}
weather_fallback = model_metrics.get("weather_medians_for_fallback", {})
weather_schema = StructType(
    [StructField("weather_hour", StringType(), False)]
    + [StructField(name, DoubleType(), True) for name in WEATHER_FIELDS]
    + [StructField("weather_missing", DoubleType(), False), StructField("weather_source", StringType(), False)]
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
    hourly = batch.withColumn(
        "weather_hour",
        concat(
            date_format("event_time", "yyyy-MM-dd'T'"),
            lpad(hour("event_time").cast("string"), 2, "0"),
        ),
    )
    hour_keys = [row.weather_hour for row in hourly.select("weather_hour").distinct().collect()]
    try:
        forecast = get_forecast()
    except Exception as error:
        print(f"[WARN] Weather forecast unavailable; using training medians: {error}", flush=True)
        forecast = {}
    weather_rows = [
        {"weather_hour": hour_key, **weather_values_for_hour(hour_key, forecast, weather_fallback)}
        for hour_key in hour_keys
    ]
    hourly = hourly.join(spark.createDataFrame(weather_rows, weather_schema), "weather_hour", "left")
    hourly = hourly.withColumn("weather_code", col("weather_code").cast("int").cast("string"))
    predictions = (
        model.transform(hourly)
        .select(
            "event_id",
            "pickup_datetime",
            "pickup_zone",
            "dropoff_zone",
            "pickup_hour",
            "pickup_dow",
            "temperature_2m",
            "precipitation",
            "snowfall",
            "wind_speed_10m",
            "relative_humidity_2m",
            "weather_code",
            "weather_missing",
            "weather_source",
            least(
                greatest(col("predicted_hotspot_score"), lit(0.0)),
                lit(100.0),
            ).cast("double").alias("hotspot_score"),
        )
        .withColumn(
            "hotspot_level",
            when(col("hotspot_score") >= 75, "Rất nóng")
            .when(col("hotspot_score") >= 50, "Nóng")
            .when(col("hotspot_score") >= 25, "Trung bình")
            .otherwise("Thấp"),
        )
        .collect()
    )
    documents = [row.asDict() for row in predictions]
    if not documents:
        return
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
    collection = client[os.getenv("MONGO_DATABASE", "taxi")][
        os.getenv("MONGO_COLLECTION", "hotspot_predictions")
    ]
    for document in documents:
        mean, stddev, threshold, baseline_count = historical_baseline_threshold(
            historical_baseline,
            str(document["pickup_zone"]),
            int(document["pickup_hour"]),
            int(document["pickup_dow"]),
        )
        score = float(document["hotspot_score"] or 0.0)
        alert = score > threshold
        document["alert_flag"] = "CRITICAL_ANOMALY" if alert else "NORMAL"
        document["anomaly_threshold"] = round(threshold, 2)
        document["historical_mean_score"] = round(mean, 2)
        document["historical_stddev_score"] = round(stddev, 2)
        document["historical_baseline_samples"] = baseline_count
        document["event_time"] = document.pop("pickup_datetime")
        document["batch_id"] = batch_id
    collection.bulk_write(
        [ReplaceOne({"event_id": document["event_id"]}, document, upsert=True) for document in documents],
        ordered=False,
    )
    client.close()
    alerts = sum(document["alert_flag"] == "CRITICAL_ANOMALY" for document in documents)
    missing_weather = sum(float(document.get("weather_missing") or 0) > 0 for document in documents)
    print(
        f"[PASS] MongoDB receiving hotspot scores: {len(documents)}; "
        f"weather fallback={missing_weather}; anomaly alerts={alerts}",
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
print("[PASS] Streaming query started", flush=True)
query.awaitTermination()
