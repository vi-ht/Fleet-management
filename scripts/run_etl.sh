#!/usr/bin/env bash
set -euo pipefail

if [[ "${FORCE_ETL:-false}" != "true" && -f /data/results/spark_etl_SUCCESS ]]; then
  if PYTHONPATH=/workspace python3 -c 'from scripts.hdfs_client import exists; raise SystemExit(0 if exists("/taxi/curated/trips/_SUCCESS") else 1)'; then
    echo "[PASS] Curated HDFS Parquet already exists; ETL skipped"
    exit 0
  fi
  echo "[INFO] Local ETL marker exists but HDFS curated output is missing; rebuilding"
fi

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /workspace/spark/clean.py
