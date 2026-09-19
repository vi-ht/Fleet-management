#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  master)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host 0.0.0.0
    ;;
  worker)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker "${SPARK_MASTER_URL:?SPARK_MASTER_URL is required}"
    ;;
  *)
    exec "$@"
    ;;
esac
