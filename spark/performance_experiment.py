"""Controlled shuffle-partition experiment over one cached curated sample."""

import json
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import count


spark = SparkSession.builder.appName("taxi-partition-benchmark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sample_size = 100_000
sample = (
    spark.read.parquet("hdfs://namenode:9000/taxi/curated/trips")
    .select("pickup_zone", "pickup_hour", "pickup_dow")
    .limit(sample_size)
    .cache()
)
actual_rows = sample.count()
if actual_rows == 0:
    raise RuntimeError("Curated dataset is empty; cannot run partition experiment")

# Warm the JVM and cache before recording either configuration.
spark.conf.set("spark.sql.shuffle.partitions", "4")
sample.groupBy("pickup_zone", "pickup_hour", "pickup_dow").agg(
    count("*").alias("trip_count")
).collect()

measurements = {}
reference = None
for partitions in (4, 16):
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    durations = []
    grouped_rows = None
    for _ in range(3):
        started = time.perf_counter()
        output = (
            sample.groupBy("pickup_zone", "pickup_hour", "pickup_dow")
            .agg(count("*").alias("trip_count"))
            .collect()
        )
        durations.append(time.perf_counter() - started)
        grouped_rows = [tuple(row) for row in output]
    normalized = sorted(grouped_rows)
    if reference is None:
        reference = normalized
    elif normalized != reference:
        raise AssertionError("Partition settings produced different grouped results")
    measurements[str(partitions)] = {
        "shuffle_partitions": partitions,
        "repetitions_seconds": durations,
        "median_seconds": statistics.median(durations),
        "group_rows": len(grouped_rows),
    }

result = {
    "experiment": "same cached 100k-row sample; vary spark.sql.shuffle.partitions",
    "input_rows": actual_rows,
    "spark_version": spark.version,
    "cluster_manager": "Spark Standalone in Docker Compose",
    "warmup_runs_excluded": 1,
    "measured_repetitions_per_setting": 3,
    "settings": measurements,
    "outputs_identical": True,
    "interpretation": (
        "Compare medians only within this local Docker run; small timing differences "
        "are sensitive to cache, JVM and host load and do not establish scale-out performance."
    ),
}
Path("/data/results/performance_benchmark.json").write_text(
    json.dumps(result, indent=2), encoding="utf-8"
)
print(json.dumps(result, indent=2), flush=True)
spark.stop()
