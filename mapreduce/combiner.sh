#!/usr/bin/env bash
set -euo pipefail

awk -F '\t' '{ totals[$1] += $2 } END { for (key in totals) printf "%s\t%d\n", key, totals[key] }'
