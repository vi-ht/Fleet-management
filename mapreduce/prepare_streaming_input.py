"""Prepare raw TLC pickup rows for the Hadoop Streaming key/value job."""

import csv
import os
from collections import Counter
from pathlib import Path

from mapreduce.baseline import generate
from scripts.hdfs_client import exists, mkdirs, put_file, set_permission, wait_ready


ROOT = Path("/workspace")
RESULTS = Path("/data/results")
INPUT_PATH = "/taxi/raw/mapreduce_input"
EXPECTED_PATH = "/taxi/raw/aggregated/expected.csv"
MARKER = RESULTS / "mapreduce_SUCCESS"


def hour_of(value):
    hour = getattr(value, "hour", None)
    return int(hour if hour is not None else str(value)[11:13])


def main():
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
        and MARKER.exists()
        and exists("/taxi/raw/aggregated/mapreduce_output/_SUCCESS")
    ):
        print("[PASS] Hadoop Streaming output exists; preparation skipped", flush=True)
        return

    mkdirs("/taxi/raw")
    mkdirs("/taxi/raw/parquet")
    mkdirs(INPUT_PATH)
    mkdirs("/taxi/raw/aggregated")
    set_permission("/taxi", "777")
    set_permission("/taxi/raw", "777")

    input_path = RESULTS / "mapreduce_input.tsv"
    counts = Counter()
    with input_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
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
                    zone, hour = str(pickup_zone), hour_of(pickup_datetime)
                    writer.writerow((zone, hour, 1))
                    counts[(zone, hour)] += 1
            for path in parquet_files:
                put_file(path, f"/taxi/raw/parquet/{path.name}")
            print(
                f"[INFO] NYC TLC Parquet source selected: {len(parquet_files)} files",
                flush=True,
            )
        else:
            raw_csv = RESULTS / "taxi_trips.csv"
            generate(str(raw_csv))
            with raw_csv.open(encoding="utf-8", newline="") as source:
                for row in csv.DictReader(source):
                    timestamp = row.get("tpep_pickup_datetime") or row.get(
                        "pickup_datetime", ""
                    )
                    zone = row.get("PULocationID") or row.get("pickup_zone", "")
                    if not timestamp or not zone:
                        continue
                    hour = int(timestamp[11:13])
                    writer.writerow((zone, hour, 1))
                    counts[(zone, hour)] += 1
            put_file(raw_csv, "/taxi/raw/taxi_trips.csv")
            print("[INFO] Synthetic fallback source selected", flush=True)

    put_file(input_path, f"{INPUT_PATH}/part-00000.tsv")

    expected_path = RESULTS / "mapreduce_expected.csv"
    with expected_path.open("w", newline="", encoding="utf-8") as expected:
        output = csv.writer(expected, lineterminator="\n")
        output.writerow(("pickup_zone", "pickup_hour", "trip_count"))
        for (zone, hour), count in sorted(counts.items()):
            output.writerow((zone, hour, count))
    put_file(expected_path, EXPECTED_PATH)
    print(
        f"[PASS] Prepared {sum(counts.values()):,} pickup rows; "
        f"independent Counter baseline has {len(counts):,} zone-hour keys",
        flush=True,
    )


if __name__ == "__main__":
    main()
