import json
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, greatest, lit, max as spark_max, round as spark_round
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("taxi-hotspot-model-trainer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
trips = spark.read.parquet("hdfs://namenode:9000/taxi/curated/trips")
hourly = trips.groupBy("pickup_zone", "pickup_hour", "pickup_dow").agg(
    count("*").cast("double").alias("trip_count")
)

# The model predicts a relative hotspot score, not a fare or a trip total.
# 100 means the hottest zone for the same hour/day pattern.
bucket_window = Window.partitionBy("pickup_hour", "pickup_dow")
features = hourly.withColumn("bucket_max", spark_max("trip_count").over(bucket_window)).withColumn(
    "hotspot_score",
    spark_round(
        col("trip_count") / greatest(col("bucket_max"), lit(1.0)) * lit(100.0),
        2,
    ),
).drop("bucket_max")

zone_indexer = StringIndexer(
    inputCol="pickup_zone", outputCol="zone_index", handleInvalid="keep"
)

cluster_features = VectorAssembler(
    inputCols=["pickup_hour", "pickup_dow", "zone_index"], outputCol="cluster_features"
)
kmeans = KMeans(k=2, seed=42, featuresCol="cluster_features", predictionCol="cluster")
hotspot_features = VectorAssembler(
    inputCols=["pickup_hour", "pickup_dow", "zone_index", "cluster"], outputCol="features"
)
regressor = GBTRegressor(
    featuresCol="features",
    labelCol="hotspot_score",
    predictionCol="predicted_hotspot_score",
    maxIter=10,
    maxBins=300,
    seed=42,
)
pipeline = Pipeline(stages=[zone_indexer, cluster_features, kmeans, hotspot_features, regressor])
training_features, evaluation_features = features.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(training_features)
evaluation_predictions = model.transform(evaluation_features).cache()
rmse = RegressionEvaluator(
    labelCol="hotspot_score",
    predictionCol="predicted_hotspot_score",
    metricName="rmse",
).evaluate(evaluation_predictions)
mae = RegressionEvaluator(
    labelCol="hotspot_score",
    predictionCol="predicted_hotspot_score",
    metricName="mae",
).evaluate(evaluation_predictions)
evaluation_rows = evaluation_predictions.count()
metrics = {
    "model": "KMeans + GBTRegressor",
    "evaluation_method": "random holdout (80/20, seed=42)",
    "training_rows": training_features.count(),
    "evaluation_rows": evaluation_rows,
    "rmse": float(rmse),
    "mae": float(mae),
    "label": "relative hotspot score (0-100)",
}
model.write().overwrite().save("file:///models/hotspot_model")
results_dir = Path("/data/results")
results_dir.mkdir(parents=True, exist_ok=True)
with (results_dir / "model_metrics.json").open("w", encoding="utf-8") as metrics_file:
    json.dump(metrics, metrics_file, ensure_ascii=False, indent=2)
evaluation_predictions.unpersist()
open("/data/results/model_SUCCESS", "w", encoding="utf-8").close()
print(
    f"[PASS] Hotspot model trained and saved; holdout RMSE={rmse:.3f}, MAE={mae:.3f}",
    flush=True,
)
spark.stop()
