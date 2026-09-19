"""Small Python mapper used by the local batch bootstrap."""

import csv
import sys


def map_rows(rows):
    for row in rows:
        pickup_datetime = row.get("tpep_pickup_datetime") or row.get("pickup_datetime", "")
        pickup_zone = row.get("PULocationID") or row.get("pickup_zone", "")
        yield f"{pickup_zone}|{pickup_datetime[11:13]}", 1


if __name__ == "__main__":
    for row in csv.DictReader(sys.stdin):
        key, value = next(map_rows([row]))
        print(f"{key}\t{value}")
