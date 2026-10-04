#!/usr/bin/env bash
set -euo pipefail

if [[ "${FORCE_TRAIN:-false}" != "true" && -f /models/hotspot_model/metadata/part-00000 && -f /models/historical_baseline.json && -f /data/results/model_metrics.json ]] && python3 - <<'PY'
import json
from pathlib import Path
p=Path('/data/results/model_metrics.json')
try:
    m=json.loads(p.read_text())
    raise SystemExit(0 if m.get('features') == ['pickup_zone', 'pickup_hour', 'pickup_dow'] else 1)
except Exception:
    raise SystemExit(1)
PY
then
  echo "[PASS] Calendar-only model, anomaly baseline, and holdout metrics already exist; training skipped"
  exit 0
fi

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /workspace/spark/train_model.py
