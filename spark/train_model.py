from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


spark = SparkSession.builder.appName("taxi-demand-model-trainer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
trips = spark.read.parquet("hdfs://namenode:9000/taxi/curated/trips")
features = trips.groupBy("pickup_zone", "pickup_hour", "pickup_dow").agg(
    count("*").cast("double").alias("demand")
)

cluster_features = VectorAssembler(
    inputCols=["pickup_hour", "pickup_dow"], outputCol="cluster_features"
)
kmeans = KMeans(k=2, seed=42, featuresCol="cluster_features", predictionCol="cluster")
demand_features = VectorAssembler(
    inputCols=["pickup_hour", "pickup_dow", "cluster"], outputCol="features"
)
regressor = GBTRegressor(
    featuresCol="features", labelCol="demand", predictionCol="prediction", maxIter=10, seed=42
)
pipeline = Pipeline(stages=[cluster_features, kmeans, demand_features, regressor])
model = pipeline.fit(features)
model.write().overwrite().save("file:///models/gbt_demand_model")
open("/data/results/model_SUCCESS", "w", encoding="utf-8").close()
print("[PASS] ML model trained and saved", flush=True)
spark.stop()
