#!/usr/bin/env bash
set -euo pipefail

HADOOP="${HADOOP_HOME:-/opt/hadoop-3.2.1}/bin/hadoop"
STREAMING_JAR="${HADOOP_HOME:-/opt/hadoop-3.2.1}/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
INPUT="/taxi/raw/mapreduce_input"
OUTPUT="/taxi/raw/aggregated/mapreduce_output"
MARKER="/data/results/mapreduce_SUCCESS"
LOG="/data/results/hadoop_streaming.log"

if [[ "${FORCE_BATCH:-false}" != "true" && -f "$MARKER" ]] && "$HADOOP" fs -test -e "$OUTPUT/_SUCCESS"; then
  echo "[PASS] Hadoop Streaming output already exists; job skipped"
  exit 0
fi

"$HADOOP" fs -rm -r -f "$OUTPUT" >/dev/null 2>&1 || true
"$HADOOP" jar "$STREAMING_JAR" \
  -D mapreduce.job.name=nyc-taxi-zone-hour-count \
  -D mapreduce.job.reduces=1 \
  -files /workspace/mapreduce/mapper.sh,/workspace/mapreduce/combiner.sh,/workspace/mapreduce/reducer.sh \
  -input "$INPUT" \
  -output "$OUTPUT" \
  -mapper "bash mapper.sh" \
  -combiner "bash combiner.sh" \
  -reducer "bash reducer.sh" 2>&1 | tee "$LOG"

"$HADOOP" fs -test -e "$OUTPUT/_SUCCESS"
expected="/data/results/mapreduce_expected.csv"
actual="/data/results/mapreduce_actual.csv"
expected_keys="/data/results/mapreduce_expected_keys.tsv"
actual_keys="/data/results/mapreduce_actual_keys.tsv"

awk -F ',' 'NR > 1 { printf "%s|%s\t%s\n", $1, $2, $3 }' "$expected" | sort > "$expected_keys"
{
  echo 'pickup_zone,pickup_hour,trip_count'
  "$HADOOP" fs -cat "$OUTPUT"/part-* \
    | awk -F '\t' 'NF == 2 && $1 ~ /^[^|]+\|[0-9]+$/ && $2 ~ /^[0-9]+$/ { split($1, key, "|"); printf "%s,%s,%s\n", key[1], key[2], $2 }' \
    | sort -t ',' -k1,1n -k2,2n
} > "$actual"
awk -F ',' 'NR > 1 { printf "%s|%s\t%s\n", $1, $2, $3 }' "$actual" | sort > "$actual_keys"
diff -u "$expected_keys" "$actual_keys"

touch "$MARKER"
echo "[PASS] Hadoop Streaming mapper/shuffle/combiner/reducer output independently matches Counter baseline"
echo "[PASS] Result: $actual"
{
  echo "[PASS] Validated $(($(wc -l < "$actual") - 1)) aggregated zone-hour rows against the independent Counter baseline"
  echo "[PASS] LocalJobRunner input_rows=$(($(wc -l < /data/results/mapreduce_input.tsv)))"
} | tee -a "$LOG"
