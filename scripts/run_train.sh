#!/usr/bin/env bash
set -euo pipefail

if [[ "${FORCE_TRAIN:-false}" != "true" && -f /models/gbt_demand_model/metadata/part-00000 ]]; then
  echo "[PASS] ML model already exists; training skipped"
  exit 0
fi

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /workspace/spark/train_model.py
