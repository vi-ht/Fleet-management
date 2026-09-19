#!/usr/bin/env bash
set -euo pipefail

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /workspace/spark/clean.py
