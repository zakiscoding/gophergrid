#!/usr/bin/env bash
# Run ggbench on the compose network. Results land in bench/out/.
#   scripts/bench.sh -n 5000 -ms 0 -name baseline -reset
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bench/out
src="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 exec docker run --rm --network gophergrid_default \
  -v "$src/bench/out":/work/bench/out -v /var/run/docker.sock:/var/run/docker.sock \
  -e GOPHERGRID_URL="${GOPHERGRID_URL:-http://scheduler-1:8080}" \
  -e DATABASE_URL="postgres://gophergrid:${POSTGRES_PASSWORD:-gophergrid}@postgres:5432/gophergrid?sslmode=disable" \
  -e HEARTBEAT_LONG_POLL -e EAGER_SYNC -e SCHED_INTERVAL -e SCHED_BATCH -e HEARTBEAT_INTERVAL \
  gophergrid/ggbench:dev "$@"
