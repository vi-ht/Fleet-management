"""Download official NYC TLC sample inputs without changing the default simulator."""

import os
from pathlib import Path

import requests


TRIP_URL = os.getenv(
    "TLC_TRIP_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet",
)
ZONE_URL = os.getenv(
    "TLC_ZONE_URL",
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
)
OUTPUT = Path(os.getenv("TLC_REFERENCE_DIR", "data/reference"))


def download(url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with target.open("wb") as destination:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    destination.write(chunk)
    print(f"[PASS] Downloaded {target}", flush=True)


if __name__ == "__main__":
    download(TRIP_URL, OUTPUT / "yellow_tripdata_2024-10.parquet")
    download(ZONE_URL, OUTPUT / "taxi_zone_lookup.csv")
