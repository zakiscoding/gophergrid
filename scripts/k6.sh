#!/usr/bin/env bash
# Run the k6 REST load test against the compose stack. RATE=500 DURATION=30s by default.
set -euo pipefail
cd "$(dirname "$0")/.."
src="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 exec docker run --rm --network gophergrid_default \
  -v "$src/bench/k6":/scripts:ro -v "$src/bench/out":/out \
  -e RATE="${RATE:-500}" -e DURATION="${DURATION:-30s}" -e GOPHERGRID_URL="${GOPHERGRID_URL:-http://scheduler-1:8080}" \
  grafana/k6:1.2.3 run --quiet /scripts/submit.js
