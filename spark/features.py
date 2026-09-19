from pyspark.sql import DataFrame
from pyspark.sql.functions import count


def demand_features(trips: DataFrame) -> DataFrame:
    return trips.groupBy("pickup_zone", "pickup_hour", "pickup_dow").agg(
        count("*").cast("double").alias("demand")
    )
