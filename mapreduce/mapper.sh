#!/usr/bin/env bash
set -euo pipefail

awk -F '\t' 'NF == 3 && $1 != "" && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ { printf "%s|%d\t%d\n", $1, $2, $3 }'
