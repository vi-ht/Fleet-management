#!/usr/bin/env bash
set -euo pipefail

if [[ "${FORCE_TRAIN:-false}" != "true" && -f /models/hotspot_model/metadata/part-00000 && -f /data/results/model_metrics.json ]]; then
  echo "[PASS] Hotspot model and holdout metrics already exist; training skipped"
  exit 0
fi

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /workspace/spark/train_model.py
