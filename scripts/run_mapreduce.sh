#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH=/workspace
python3 - <<'PY'
import csv
import os
from collections import Counter

from mapreduce.baseline import generate
from scripts.hdfs_client import mkdirs, put_file, set_permission, wait_ready

raw = "/data/raw/taxi_trips.csv"
generate(raw)
wait_ready()
print("[PASS] HDFS ready", flush=True)

counts = Counter()
with open(raw, encoding="utf-8") as source:
    for row in csv.DictReader(source):
        pickup_datetime = row.get("tpep_pickup_datetime") or row.get("pickup_datetime", "")
        pickup_zone = row.get("PULocationID") or row.get("pickup_zone", "")
        if pickup_datetime and pickup_zone:
            counts[(pickup_zone, pickup_datetime[11:13])] += 1

aggregated = "/data/results/mapreduce_demand.csv"
with open(aggregated, "w", newline="", encoding="utf-8") as output:
    writer = csv.writer(output)
    writer.writerow(["pickup_zone", "pickup_hour", "demand"])
    for (zone, hour), demand in sorted(counts.items()):
        writer.writerow([zone, int(hour), demand])

mkdirs("/taxi/raw")
mkdirs("/taxi/raw/aggregated")
set_permission("/taxi", "777")
set_permission("/taxi/raw", "777")
put_file(raw, "/taxi/raw/taxi_trips.csv")
put_file(aggregated, "/taxi/raw/aggregated/demand.csv")
os.makedirs("/data/results", exist_ok=True)
open("/data/results/mapreduce_SUCCESS", "w", encoding="utf-8").close()
print("[PASS] MapReduce completed", flush=True)
PY
