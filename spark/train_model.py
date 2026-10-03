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
    concat,
    count,
    date_format,
    dayofweek,
    greatest,
    lit,
    lpad,
    max as spark_max,
    round as spark_round,
    stddev_pop,
    sum as spark_sum,
    to_date,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from weather import WEATHER_FIELDS, fetch_historical_weather


spark = (
    SparkSession.builder.appName("taxi-hotspot-weather-model-trainer")
    .config("spark.sql.session.timeZone", "America/New_York")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
trips = spark.read.parquet("hdfs://namenode:9000/taxi/curated/trips")
daily_hourly = (
    trips.withColumn("service_date", to_date("pickup_datetime"))
    .filter(col("service_date") >= lit("2022-01-01"))
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
    .withColumn(
        "weather_hour",
        concat(date_format("service_date", "yyyy-MM-dd'T'"), lpad(col("pickup_hour").cast("string"), 2, "0")),
    )
)
service_dates = [
    row.service_date
    for row in features.select("service_date").distinct().orderBy("service_date").collect()
]
if len(service_dates) < 2:
    raise ValueError(f"Temporal evaluation requires at least two distinct dates; found {len(service_dates)}")
start_date, end_date = str(service_dates[0]), str(service_dates[-1])
weather_cache = Path(os.getenv("WEATHER_CACHE_DIR", "/data/reference/open_meteo"))
weather_rows = fetch_historical_weather(start_date, end_date, weather_cache)
weather_schema = StructType(
    [StructField("weather_hour", StringType(), False)]
    + [StructField(name, DoubleType(), True) for name in WEATHER_FIELDS]
)
weather_records = [
    {
        "weather_hour": row["hour"],
        **{
            name: float(row[name]) if row.get(name) is not None else None
            for name in WEATHER_FIELDS
        },
    }
    for row in weather_rows
]
if not weather_records:
    raise RuntimeError("Open-Meteo returned no historical forecast rows for the TLC date range")
weather = spark.createDataFrame(weather_records, weather_schema)
enriched = (
    features.join(weather, "weather_hour", "inner")
    .dropna(subset=list(WEATHER_FIELDS))
    .withColumn("weather_missing", lit(0.0))
    .withColumn("weather_code", col("weather_code").cast("int").cast("string"))
    .cache()
)
eligible_rows = features.count()
weather_rows_count = enriched.count()
weather_coverage = weather_rows_count / max(eligible_rows, 1)
if weather_coverage < float(os.getenv("WEATHER_MIN_TRAINING_COVERAGE", "0.95")):
    raise RuntimeError(
        f"Historical forecast coverage is only {weather_coverage:.1%}; "
        "refusing to train a weather model with a mostly missing feature set"
    )

# Keep the latest 20% of service dates completely out of model fitting.
service_dates = [
    row.service_date
    for row in enriched.select("service_date").distinct().orderBy("service_date").collect()
]
if len(service_dates) < 2:
    raise ValueError(f"Weather join leaves fewer than two distinct service dates: {len(service_dates)}")
train_date_count = min(max(1, int(len(service_dates) * 0.8)), len(service_dates) - 1)
train_dates, test_dates = service_dates[:train_date_count], service_dates[train_date_count:]
train_end_date, test_start_date, test_end_date = train_dates[-1], test_dates[0], test_dates[-1]
training = enriched.filter(col("service_date") <= lit(train_end_date)).cache()
evaluation = enriched.filter(col("service_date") >= lit(test_start_date)).cache()

baseline_features = ["pickup_hour", "pickup_dow", "zone_vector"]
weather_features = [
    "pickup_hour",
    "pickup_dow",
    "temperature_2m",
    "precipitation",
    "snowfall",
    "wind_speed_10m",
    "relative_humidity_2m",
    "zone_vector",
    "weather_vector",
]


def build_pipeline(include_weather: bool) -> Pipeline:
    stages = [StringIndexer(inputCol="pickup_zone", outputCol="zone_index", handleInvalid="keep")]
    if include_weather:
        stages.append(StringIndexer(inputCol="weather_code", outputCol="weather_code_index", handleInvalid="keep"))
        stages.append(
            OneHotEncoder(
                inputCols=["zone_index", "weather_code_index"],
                outputCols=["zone_vector", "weather_vector"],
                handleInvalid="keep",
            )
        )
    else:
        stages.append(OneHotEncoder(inputCols=["zone_index"], outputCols=["zone_vector"], handleInvalid="keep"))
    stages.extend(
        [
            VectorAssembler(
                inputCols=weather_features if include_weather else baseline_features,
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
    return Pipeline(stages=stages)


def evaluate(model, rows):
    predictions = model.transform(rows).cache()
    metrics = {
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
    metrics.update(
        {
            "precision": tp / (tp + fp) if tp + fp else None,
            "recall": tp / (tp + fn) if tp + fn else None,
            "true_positive": tp,
            "false_positive": fp,
            "false_negative": fn,
            "rows": predictions.count(),
        }
    )
    predictions.unpersist()
    return metrics


baseline_candidate = build_pipeline(include_weather=False).fit(training)
weather_candidate = build_pipeline(include_weather=True).fit(training)
baseline_metrics = evaluate(baseline_candidate, evaluation)
weather_metrics = evaluate(weather_candidate, evaluation)
# Deploy the weather model only when the untouched future-period holdout shows
# a real RMSE gain without materially degrading absolute error.
weather_selected = (
    weather_metrics["RMSE"] < baseline_metrics["RMSE"]
    and weather_metrics["MAE"] <= baseline_metrics["MAE"] * 1.02
)
model = weather_candidate if weather_selected else baseline_candidate
selected_metrics = weather_metrics if weather_selected else baseline_metrics
model_name = "GBTRegressor + thời tiết Open-Meteo" if weather_selected else "GBTRegressor (weather candidate not selected)"

baseline_rows = (
    training.groupBy("pickup_zone", "pickup_hour", "pickup_dow")
    .agg(
        avg("hotspot_score").alias("mean"),
        stddev_pop("hotspot_score").alias("stddev"),
        count("*").alias("count"),
    )
    .collect()
)
historical_baseline = {
    f"{row.pickup_zone}|{row.pickup_hour}|{row.pickup_dow}": {
        "mean": float(row.mean or 0),
        "stddev": float(row.stddev or 0),
        "count": int(row["count"]),
    }
    for row in baseline_rows
}
weather_medians = {
    name: float(training.approxQuantile(name, [0.5], 0.01)[0])
    for name in WEATHER_FIELDS
    if name != "weather_code"
}
weather_code_mode = (
    training.groupBy("weather_code")
    .count()
    .orderBy(col("count").desc(), col("weather_code").asc())
    .first()
)
weather_medians["weather_code"] = float(weather_code_mode.weather_code)
model_dir = Path(os.getenv("MODEL_DIR", "/models"))
model_dir.mkdir(parents=True, exist_ok=True)
model.write().overwrite().save((model_dir / "hotspot_model").as_uri())
(model_dir / "historical_baseline.json").write_text(
    json.dumps(historical_baseline, ensure_ascii=False), encoding="utf-8"
)
results_dir = Path(os.getenv("RESULTS_DIR", "/data/results"))
results_dir.mkdir(parents=True, exist_ok=True)
metrics = {
    "model": model_name,
    "model_family": "GBTRegressor",
    "weather_source": "Open-Meteo Historical Forecast API",
    "weather_features": list(WEATHER_FIELDS),
    "weather_timezone": "America/New_York",
    "weather_location": {"latitude": 40.7128, "longitude": -74.006, "label": "NYC city-center proxy"},
    "weather_selected_by_temporal_holdout": weather_selected,
    "weather_coverage_rows": weather_rows_count,
    "eligible_rows_since_2022": eligible_rows,
    "weather_coverage": weather_coverage,
    "weather_medians_for_fallback": weather_medians,
    "evaluation_method": "temporal holdout (earliest 80% of weather-covered dates train; latest 20% test)",
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
    "temporal_holdout_comparison": {
        "calendar_baseline": baseline_metrics,
        "weather_candidate": weather_metrics,
        "selection_rule": "use weather candidate only if RMSE improves and MAE is no more than 2% worse",
    },
    "label": "daily relative hotspot score (0-100)",
    "score_formula": "daily_zone_trip_count / max_daily_trip_count_for_same_hour_and_weekday * 100",
    "anomaly_rule": "score > mean(training daily zone score) + 3 * population_stddev; minimum 5 training samples; cap 100",
    "anomaly_baseline_keys": "pickup_zone|pickup_hour|pickup_dow",
    "anomaly_min_samples": 5,
    "anomaly_sigma": 3.0,
}
(results_dir / "model_metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
open(results_dir / "model_SUCCESS", "w", encoding="utf-8").close()
print(
    f"[PASS] Model={model_name}; temporal test {test_start_date}..{test_end_date}; "
    f"weather coverage={weather_coverage:.1%}; deployed MAE={selected_metrics['MAE']:.3f}, "
    f"RMSE={selected_metrics['RMSE']:.3f}, R2={selected_metrics['R2']:.3f}; "
    f"weather candidate MAE={weather_metrics['MAE']:.3f}, RMSE={weather_metrics['RMSE']:.3f}, "
    f"R2={weather_metrics['R2']:.3f}; weather_selected={weather_selected}",
    flush=True,
)
enriched.unpersist()
training.unpersist()
evaluation.unpersist()
spark.stop()
