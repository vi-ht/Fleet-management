"""Combiner for mapper output."""

import sys
from collections import defaultdict


totals = defaultdict(int)
for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    totals[key] += int(value)

for key, value in sorted(totals.items()):
    print(f"{key}\t{value}")
