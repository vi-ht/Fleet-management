#!/usr/bin/env bash
set -euo pipefail

SPARK_SUBMIT=/opt/spark/bin/spark-submit
COMMON=(--master spark://spark-master:7077 --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000)
"$SPARK_SUBMIT" "${COMMON[@]}" /workspace/spark/queries.py
"$SPARK_SUBMIT" "${COMMON[@]}" /workspace/spark/performance_experiment.py
