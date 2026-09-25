#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH=/workspace
python3 - <<'PY'
import csv
import os
from collections import Counter
from pathlib import Path

from mapreduce.baseline import generate
from scripts.hdfs_client import exists, mkdirs, put_file, set_permission, wait_ready

raw = "/data/raw/taxi_trips.csv"
marker = "/data/results/mapreduce_SUCCESS"
remote_output = "/taxi/raw/aggregated/hotspot_observations.csv"
source_dir = Path(
    os.getenv(
        "RAW_DATA_PATH",
        "/workspace/Nyc taxi trip record/Nyc taxi trip record",
    )
)
parquet_files = sorted(source_dir.glob("yellow_tripdata_*.parquet"))
wait_ready()
print("[PASS] HDFS ready", flush=True)

if (
    os.getenv("FORCE_BATCH", "false").lower() != "true"
    and os.path.exists(marker)
    and exists(remote_output)
):
    print("[PASS] MapReduce artifact already exists; batch skipped", flush=True)
    raise SystemExit(0)

counts = Counter()
mkdirs("/taxi/raw")
mkdirs("/taxi/raw/parquet")
set_permission("/taxi", "777")
set_permission("/taxi/raw", "777")

if parquet_files:
    import pyarrow.dataset as ds

    dataset = ds.dataset([str(path) for path in parquet_files], format="parquet")
    scanner = dataset.scanner(
        columns=["tpep_pickup_datetime", "PULocationID"], batch_size=100_000
    )
    for batch in scanner.to_batches():
        values = batch.to_pydict()
        for pickup_datetime, pickup_zone in zip(
            values["tpep_pickup_datetime"], values["PULocationID"]
        ):
            if pickup_datetime is None or pickup_zone is None:
                continue
            pickup_hour = getattr(pickup_datetime, "hour", None)
            if pickup_hour is None:
                pickup_hour = int(str(pickup_datetime)[11:13])
            counts[(str(pickup_zone), int(pickup_hour))] += 1
    for parquet_file in parquet_files:
        put_file(parquet_file, f"/taxi/raw/parquet/{parquet_file.name}")
    print(f"[INFO] NYC TLC Parquet source selected: {len(parquet_files)} files", flush=True)
else:
    generate(raw)
    with open(raw, encoding="utf-8") as source:
        for row in csv.DictReader(source):
            pickup_datetime = row.get("tpep_pickup_datetime") or row.get("pickup_datetime", "")
            pickup_zone = row.get("PULocationID") or row.get("pickup_zone", "")
            if pickup_datetime and pickup_zone:
                counts[(pickup_zone, int(pickup_datetime[11:13]))] += 1
    put_file(raw, "/taxi/raw/taxi_trips.csv")
    print("[INFO] Synthetic fallback source selected", flush=True)

aggregated = "/data/results/mapreduce_hotspot_observations.csv"
with open(aggregated, "w", newline="", encoding="utf-8") as output:
    writer = csv.writer(output)
    writer.writerow(["pickup_zone", "pickup_hour", "trip_count"])
    for (zone, hour), trip_count in sorted(counts.items()):
        writer.writerow([zone, int(hour), trip_count])

mkdirs("/taxi/raw/aggregated")
put_file(aggregated, remote_output)
os.makedirs("/data/results", exist_ok=True)
open("/data/results/mapreduce_SUCCESS", "w", encoding="utf-8").close()
print("[PASS] MapReduce completed", flush=True)
PY
