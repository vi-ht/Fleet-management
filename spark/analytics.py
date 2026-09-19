from pyspark.sql import SparkSession
from pyspark.sql.functions import count, window


spark = SparkSession.builder.appName("taxi-demand-analytics").getOrCreate()
trips = spark.read.parquet("hdfs://namenode:9000/taxi/curated/trips")
summary = trips.groupBy("pickup_zone", "pickup_hour").agg(count("*").alias("demand"))
summary.orderBy("pickup_zone", "pickup_hour").show(50, truncate=False)
spark.stop()
