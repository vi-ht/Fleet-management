"""Deterministic NYC TLC-shaped input for the realtime simulator."""

import csv
import os
from datetime import datetime, timedelta, timezone


DEFAULT_ZONE_IDS = (
    4, 7, 10, 12, 13, 14, 21, 24, 41, 42, 43, 48, 50, 61, 68, 74,
    75, 79, 87, 88, 90, 100, 107, 113, 114, 116, 120, 125, 127, 128,
    137, 138, 141, 142, 143, 144, 148, 151, 152, 153, 158, 161, 162,
    163, 164, 166, 170, 186, 194, 202, 209, 211, 224, 229, 230, 231,
    232, 233, 234, 236, 237, 238, 239, 243, 244, 246, 249, 261, 262, 263,
)


def _zone_ids() -> tuple[int, ...]:
    """Use the official lookup when mounted, otherwise use valid TLC IDs."""
    lookup = "/data/reference/taxi_zone_lookup.csv"
    if os.path.exists(lookup):
        with open(lookup, encoding="utf-8") as source:
            reader = csv.DictReader(source)
            ids = [int(row["LocationID"]) for row in reader if row.get("LocationID")]
        if ids:
            return tuple(sorted(set(ids)))
    return DEFAULT_ZONE_IDS


def generate(path: str, rows: int | None = None) -> None:
    if os.path.exists(path) and os.getenv("SIMULATION_REFRESH", "false").lower() != "true":
        return
    rows = rows or int(os.getenv("SIMULATION_ROWS", "10000"))
    zone_ids = _zone_ids()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    start = datetime(2024, 10, 1, tzinfo=timezone.utc)
    with open(path, "w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow([
            "event_id", "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
            "PULocationID", "DOLocationID", "payment_type", "fare_amount", "total_amount",
        ])
        for index in range(rows):
            event_time = start + timedelta(minutes=5 * index)
            hour = event_time.hour
            # Morning/evening demand peaks make the stream less artificial than a cycle.
            peak_offset = 11 if 7 <= hour <= 9 or 16 <= hour <= 19 else 0
            pickup_zone = zone_ids[(index * 17 + peak_offset) % len(zone_ids)]
            dropoff_zone = zone_ids[(index * 31 + 7 + peak_offset) % len(zone_ids)]
            if dropoff_zone == pickup_zone:
                dropoff_zone = zone_ids[(zone_ids.index(pickup_zone) + 1) % len(zone_ids)]
            distance = round(1.0 + ((index * 13) % 180) / 10, 2)
            fare = round(3.0 + distance * 2.75, 2)
            writer.writerow([
                f"trip-{index:05d}",
                1 + index % 2,
                event_time.isoformat().replace("+00:00", ""),
                (event_time + timedelta(minutes=8 + index % 35)).isoformat().replace("+00:00", ""),
                1 + index % 4,
                distance,
                1,
                "N",
                pickup_zone,
                dropoff_zone,
                1 if index % 5 else 2,
                fare,
                round(fare + 2.5 + (index % 4) * 0.5, 2),
            ])
