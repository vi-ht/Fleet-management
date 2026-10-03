"""Run two business questions and persist Spark SQL formatted-plan evidence."""

import json
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("taxi-spark-sql-evidence").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
trips = spark.read.parquet("hdfs://namenode:9000/taxi/curated/trips")
trips.createOrReplaceTempView("curated_trips")

queries = {
    "top_zone_hour_dow": """
        SELECT pickup_zone, pickup_zone_name, pickup_borough, pickup_hour,
               pickup_dow, COUNT(*) AS trip_count
        FROM curated_trips
        GROUP BY pickup_zone, pickup_zone_name, pickup_borough,
                 pickup_hour, pickup_dow
        ORDER BY trip_count DESC, pickup_zone ASC, pickup_hour ASC
        LIMIT 20
    """,
    "borough_hour_profile": """
        SELECT pickup_borough, pickup_hour, COUNT(*) AS trip_count,
               COUNT(DISTINCT pickup_zone) AS active_pickup_zones
        FROM curated_trips
        WHERE pickup_borough IS NOT NULL
        GROUP BY pickup_borough, pickup_hour
        ORDER BY pickup_borough ASC, pickup_hour ASC
    """,
}

results = {"spark_version": spark.version, "queries": {}}
plans = []
for name, sql in queries.items():
    frame = spark.sql(sql)
    plan = StringIO()
    with redirect_stdout(plan):
        frame.explain("formatted")
    rows = [row.asDict(recursive=True) for row in frame.collect()]
    results["queries"][name] = {"sql": sql.strip(), "rows": rows, "row_count": len(rows)}
    plans.append(f"QUERY: {name}\nSQL:\n{sql.strip()}\nFORMATTED PLAN:\n{plan.getvalue()}")

Path("/data/results").mkdir(parents=True, exist_ok=True)
Path("/data/results/spark_query_results.json").write_text(
    json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
)
Path("/data/results/spark_query_plans.txt").write_text(
    "\n\n".join(plans), encoding="utf-8"
)
for name, data in results["queries"].items():
    print(f"[PASS] {name}: {data['row_count']} rows", flush=True)
print("[PASS] Spark SQL results and formatted plans saved under /data/results", flush=True)
spark.stop()
