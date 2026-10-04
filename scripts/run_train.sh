#!/usr/bin/env bash
set -euo pipefail

if [[ "${FORCE_TRAIN:-false}" != "true" && -f /models/hotspot_model/metadata/part-00000 && -f /models/demand_model/metadata/part-00000 && -f /models/historical_baseline_table/_SUCCESS && -f /models/historical_baseline.json && -f /data/results/model_metrics.json ]] && python3 - <<'PY'
import json
from pathlib import Path
p=Path('/data/results/model_metrics.json')
try:
    m=json.loads(p.read_text())
    raise SystemExit(0 if m.get('features') == ['pickup_zone', 'pickup_hour', 'pickup_dow'] and m.get('demand_mae') is not None and m.get('demand_baseline_rule') else 1)
except Exception:
    raise SystemExit(1)
PY
then
  echo "[PASS] Hotspot and demand models, Parquet baseline table, and temporal holdout metrics already exist; training skipped"
  exit 0
fi

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /workspace/spark/train_model.py
