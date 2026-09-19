#!/usr/bin/env bash
set -euo pipefail

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /workspace/streaming/predict_stream.py
