#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH=/workspace
python3 /workspace/mapreduce/prepare_streaming_input.py
