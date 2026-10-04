import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    dayofweek,
    greatest,
    lit,
    max as spark_max,
    round as spark_round,
    stddev_pop,
    sum as spark_sum,
    to_date,
    when,
)
from pyspark.sql.window import Window


spark = (
    SparkSession.builder.appName("taxi-hotspot-calendar-model-trainer")
    .config("spark.sql.session.timeZone", "America/New_York")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
trips = spark.read.parquet("hdfs://namenode:9000/taxi/curated/trips")
daily_hourly = (
    trips.withColumn("service_date", to_date("pickup_datetime"))
    .filter(col("service_date").isNotNull())
    .groupBy("service_date", "pickup_zone", "pickup_hour", "pickup_dow")
    .agg(count("*").cast("double").alias("trip_count"))
)

# Daily zone demand is normalized against the busiest zone for the same date
# and hour. The target remains a relative hotspot score, not a ride count.
bucket_window = Window.partitionBy("service_date", "pickup_hour", "pickup_dow")
features = (
    daily_hourly.withColumn("bucket_max", spark_max("trip_count").over(bucket_window))
    .withColumn(
        "hotspot_score",
        spark_round(
            col("trip_count") / greatest(col("bucket_max"), lit(1.0)) * lit(100.0),
            2,
        ),
    )
    .drop("bucket_max")
)

service_dates = [
    row.service_date
    for row in features.select("service_date").distinct().orderBy("service_date").collect()
]
if len(service_dates) < 2:
    raise ValueError(f"Temporal evaluation requires at least two distinct dates; found {len(service_dates)}")

# Keep the latest 20% of service dates completely out of model fitting.
train_date_count = min(max(1, int(len(service_dates) * 0.8)), len(service_dates) - 1)
train_dates, test_dates = service_dates[:train_date_count], service_dates[train_date_count:]
train_end_date, test_start_date, test_end_date = train_dates[-1], test_dates[0], test_dates[-1]
training = features.filter(col("service_date") <= lit(train_end_date)).cache()
evaluation = features.filter(col("service_date") >= lit(test_start_date)).cache()

# Create a complete zone/date/hour panel for absolute demand. Missing buckets
# represent zero observed trips, rather than an unknown or omitted observation.
zones = trips.select("pickup_zone").distinct()
hours = spark.range(24).select(col("id").cast("int").alias("pickup_hour"))
calendar = features.select("service_date").distinct()
demand_panel = (
    calendar.crossJoin(zones)
    .crossJoin(hours)
    .withColumn("pickup_dow", dayofweek("service_date") - 1)
    .join(
        daily_hourly.select("service_date", "pickup_zone", "pickup_hour", "trip_count"),
        ["service_date", "pickup_zone", "pickup_hour"],
        "left",
    )
    .fillna({"trip_count": 0.0})
)
demand_training = demand_panel.filter(col("service_date") <= lit(train_end_date)).cache()
demand_evaluation = demand_panel.filter(col("service_date") >= lit(test_start_date)).cache()

pipeline = Pipeline(
    stages=[
        StringIndexer(inputCol="pickup_zone", outputCol="zone_index", handleInvalid="keep"),
        OneHotEncoder(inputCols=["zone_index"], outputCols=["zone_vector"], handleInvalid="keep"),
        VectorAssembler(
            inputCols=["pickup_hour", "pickup_dow", "zone_vector"],
            outputCol="features",
            handleInvalid="keep",
        ),
        GBTRegressor(
            featuresCol="features",
            labelCol="hotspot_score",
            predictionCol="predicted_hotspot_score",
            maxIter=int(os.getenv("GBT_MAX_ITER", "20")),
            maxDepth=int(os.getenv("GBT_MAX_DEPTH", "5")),
            stepSize=float(os.getenv("GBT_STEP_SIZE", "0.1")),
            # Zones are one-hot encoded, so 64 bins is ample while keeping
            # each tree's histogram small enough for the 1 GB Spark worker.
            maxBins=int(os.getenv("GBT_MAX_BINS", "64")),
            seed=42,
        ),
    ]
)
model = pipeline.fit(training)
predictions = model.transform(evaluation).cache()
selected_metrics = {
    name: float(
        RegressionEvaluator(
            labelCol="hotspot_score",
            predictionCol="predicted_hotspot_score",
            metricName=name.lower(),
        ).evaluate(predictions)
    )
    for name in ("MAE", "RMSE", "R2")
}
counts = predictions.agg(
    spark_sum(
        when(
            (col("hotspot_score") >= 50.0) & (col("predicted_hotspot_score") >= 50.0),
            1,
        ).otherwise(0)
    ).alias("tp"),
    spark_sum(
        when(
            (col("hotspot_score") < 50.0) & (col("predicted_hotspot_score") >= 50.0),
            1,
        ).otherwise(0)
    ).alias("fp"),
    spark_sum(
        when(
            (col("hotspot_score") >= 50.0) & (col("predicted_hotspot_score") < 50.0),
            1,
        ).otherwise(0)
    ).alias("fn"),
).first()
tp, fp, fn = int(counts["tp"] or 0), int(counts["fp"] or 0), int(counts["fn"] or 0)
selected_metrics.update(
    {
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "rows": predictions.count(),
    }
)

demand_pipeline = Pipeline(
    stages=[
        StringIndexer(inputCol="pickup_zone", outputCol="zone_index", handleInvalid="keep"),
        OneHotEncoder(inputCols=["zone_index"], outputCols=["zone_vector"], handleInvalid="keep"),
        VectorAssembler(
            inputCols=["pickup_hour", "pickup_dow", "zone_vector"],
            outputCol="features",
            handleInvalid="keep",
        ),
        GBTRegressor(
            featuresCol="features",
            labelCol="trip_count",
            predictionCol="predicted_demand",
            maxIter=int(os.getenv("DEMAND_GBT_MAX_ITER", os.getenv("GBT_MAX_ITER", "20"))),
            maxDepth=int(os.getenv("DEMAND_GBT_MAX_DEPTH", os.getenv("GBT_MAX_DEPTH", "5"))),
            stepSize=float(os.getenv("GBT_STEP_SIZE", "0.1")),
            maxBins=int(os.getenv("GBT_MAX_BINS", "64")),
            seed=43,
        ),
    ]
)
demand_model = demand_pipeline.fit(demand_training)
demand_predictions = demand_model.transform(demand_evaluation).cache()
demand_metrics = {
    name.lower(): float(
        RegressionEvaluator(
            labelCol="trip_count", predictionCol="predicted_demand", metricName=name.lower()
        ).evaluate(demand_predictions)
    )
    for name in ("MAE", "RMSE", "R2")
}
demand_metrics["training_rows"] = demand_training.count()
demand_metrics["evaluation_rows"] = demand_predictions.count()

baseline_df = (
    demand_training.groupBy("pickup_zone", "pickup_hour", "pickup_dow")
    .agg(
        avg("trip_count").alias("mean_trip_count"),
        stddev_pop("trip_count").alias("stddev_trip_count"),
        count("*").alias("sample_count"),
    )
    .withColumn(
        "anomaly_threshold",
        col("mean_trip_count") + lit(3.0) * col("stddev_trip_count"),
    )
    .withColumn("baseline_unit", lit("trips_per_hour_same_weekday"))
    .cache()
)
historical_baseline = {
    f"{row.pickup_zone}|{row.pickup_hour}|{row.pickup_dow}": {
        "mean_trip_count": float(row.mean_trip_count or 0),
        "stddev_trip_count": float(row.stddev_trip_count or 0),
        "anomaly_threshold": float(row.anomaly_threshold or 0),
        "sample_count": int(row.sample_count),
    }
    for row in baseline_df.collect()
}
model_dir = Path(os.getenv("MODEL_DIR", "/models"))
model_dir.mkdir(parents=True, exist_ok=True)
model.write().overwrite().save((model_dir / "hotspot_model").as_uri())
demand_model.write().overwrite().save((model_dir / "demand_model").as_uri())
baseline_df.write.mode("overwrite").parquet((model_dir / "historical_baseline_table").as_uri())
(model_dir / "historical_baseline.json").write_text(
    json.dumps(historical_baseline, ensure_ascii=False), encoding="utf-8"
)

results_dir = Path(os.getenv("RESULTS_DIR", "/data/results"))
results_dir.mkdir(parents=True, exist_ok=True)
metrics = {
    "model": "GBTRegressor (zone/hour/weekday)",
    "model_family": "GBTRegressor",
    "features": ["pickup_zone", "pickup_hour", "pickup_dow"],
    "evaluation_method": "temporal holdout (earliest 80% of available dates train; latest 20% test)",
    "train_start_date": str(train_dates[0]),
    "train_end_date": str(train_end_date),
    "test_start_date": str(test_start_date),
    "test_end_date": str(test_end_date),
    "training_days": len(train_dates),
    "evaluation_days": len(test_dates),
    "training_rows": training.count(),
    "evaluation_rows": selected_metrics["rows"],
    "mae": selected_metrics["MAE"],
    "rmse": selected_metrics["RMSE"],
    "r2": selected_metrics["R2"],
    "hotspot_classification_threshold": 50.0,
    "hotspot_classification_positive_label": "hot (score >= threshold)",
    "hotspot_precision": selected_metrics["precision"],
    "hotspot_recall": selected_metrics["recall"],
    "hotspot_true_positive": selected_metrics["true_positive"],
    "hotspot_false_positive": selected_metrics["false_positive"],
    "hotspot_false_negative": selected_metrics["false_negative"],
    "label": "daily relative hotspot score (0-100)",
    "score_formula": "daily_zone_trip_count / max_daily_trip_count_for_same_hour_and_weekday * 100",
    "anomaly_rule": "predicted trips per zone/hour/weekday > historical mean_trip_count + 3 * population stddev; minimum 5 training service-days; no score cap",
    "anomaly_baseline_keys": "pickup_zone|pickup_hour|pickup_dow",
    "anomaly_min_samples": 5,
    "anomaly_sigma": 3.0,
    "demand_model": "GBTRegressor (absolute trips per zone/hour/weekday)",
    "demand_label": "trip_count (zero-filled zone/date/hour observations)",
    "demand_unit": "trips per hour for a service-day matching the weekday",
    "demand_features": ["pickup_zone", "pickup_hour", "pickup_dow"],
    "demand_training_rows": demand_metrics["training_rows"],
    "demand_evaluation_rows": demand_metrics["evaluation_rows"],
    "demand_mae": demand_metrics["mae"],
    "demand_rmse": demand_metrics["rmse"],
    "demand_r2": demand_metrics["r2"],
    "demand_baseline_artifact": "models/historical_baseline_table (Parquet) and historical_baseline.json",
    "demand_baseline_columns": ["pickup_zone", "pickup_hour", "pickup_dow", "mean_trip_count", "stddev_trip_count", "anomaly_threshold", "sample_count"],
    "demand_baseline_rule": "anomaly_threshold = mean_trip_count + 3 * stddev_trip_count; zero-trip date/hour buckets included",
    "anomaly_min_samples": 5,
    "anomaly_sigma": 3.0,
}
(results_dir / "model_metrics.json").write_text(
    json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8"
)
open(results_dir / "model_SUCCESS", "w", encoding="utf-8").close()
print(
    f"[PASS] Model={metrics['model']}; temporal test {test_start_date}..{test_end_date}; "
    f"train={metrics['training_rows']} rows/{len(train_dates)} days; "
    f"test={metrics['evaluation_rows']} rows/{len(test_dates)} days; "
    f"MAE={selected_metrics['MAE']:.3f}, RMSE={selected_metrics['RMSE']:.3f}, "
    f"R2={selected_metrics['R2']:.3f}; Precision={selected_metrics['precision']:.3f}, "
    f"Recall={selected_metrics['recall']:.3f}; demand MAE={demand_metrics['mae']:.3f}, "
    f"RMSE={demand_metrics['rmse']:.3f}, R2={demand_metrics['r2']:.3f}",
    flush=True,
)
predictions.unpersist()
training.unpersist()
evaluation.unpersist()
demand_predictions.unpersist()
demand_training.unpersist()
demand_evaluation.unpersist()
baseline_df.unpersist()
spark.stop()
