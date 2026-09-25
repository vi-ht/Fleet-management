"""Reducer for the hotspot observation key."""

import sys


for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    zone, hour = key.split("|", 1)
    print(f"{zone},{hour},{value}")
