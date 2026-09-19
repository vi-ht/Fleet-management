#!/usr/bin/env bash
set -euo pipefail

mkdir -p /workspace/evidence
date -u +%FT%TZ > /workspace/evidence/collected_at.txt
docker compose ps > /workspace/evidence/compose_ps.txt
docker compose logs --no-color --tail=200 > /workspace/evidence/compose_logs.txt
