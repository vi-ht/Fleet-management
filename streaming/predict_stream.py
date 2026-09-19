import os

from pymongo import MongoClient, ReplaceOne
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, from_json, hour, to_timestamp
from pyspark.sql.types import StringType, StructField, StructType


spark = SparkSession.builder.appName("taxi-demand-streaming-predictor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
model = PipelineModel.load("file:///models/gbt_demand_model")
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
    predictions = (
        model.transform(batch)
        .select(
            "event_id",
            "pickup_datetime",
            "pickup_zone",
            "dropoff_zone",
            col("prediction").cast("double"),
        )
        .collect()
    )
    documents = [row.asDict() for row in predictions]
    if not documents:
        return
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
    collection = client[os.getenv("MONGO_DATABASE", "taxi")][
        os.getenv("MONGO_COLLECTION", "demand_predictions")
    ]
    for document in documents:
        document["event_time"] = document.pop("pickup_datetime")
        document["batch_id"] = batch_id
    collection.bulk_write(
        [ReplaceOne({"event_id": document["event_id"]}, document, upsert=True) for document in documents],
        ordered=False,
    )
    client.close()
    print(f"[PASS] MongoDB receiving predictions: {len(documents)}", flush=True)


events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    .option("subscribe", os.getenv("KAFKA_TOPIC", "taxi_trips"))
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("event"))
    .select("event.*")
    .withColumn("event_time", to_timestamp("pickup_datetime"))
    .withColumn("pickup_hour", hour("event_time"))
    .withColumn("pickup_dow", dayofweek("event_time") - 1)
)

query = (
    events.writeStream.foreachBatch(write_predictions)
    .option("checkpointLocation", os.getenv("CHECKPOINT_LOCATION", "/checkpoints/taxi-demand"))
    .trigger(processingTime="2 seconds")
    .start()
)
print("[PASS] Streaming query started", flush=True)
query.awaitTermination()
