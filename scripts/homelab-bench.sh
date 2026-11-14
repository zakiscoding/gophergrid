#!/usr/bin/env bash
# Run ggbench against the homelab deployment (deploy/homelab): Postgres and the REST API on
# mos-imac over the tailnet. -chaos kills services of a compose project on this machine's Docker,
# so it only applies to a local stack; the homelab's schedulers and workers run on the iMac and GPU PC.
#   scripts/homelab-bench.sh -n 2000 -ms 0 -name homelab-baseline -reset
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . deploy/homelab/.env; set +a
imac="${IMAC_TS_IP:-100.91.113.98}"
mkdir -p bench/out
src="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 exec docker run --rm \
  -v "$src/bench/out":/work/bench/out -v /var/run/docker.sock:/var/run/docker.sock \
  -e GOPHERGRID_URL="http://$imac:8090" \
  -e DATABASE_URL="postgres://gophergrid:$POSTGRES_PASSWORD@$imac:5433/gophergrid?sslmode=disable" \
  gophergrid/ggbench:dev -project gophergrid-homelab "$@"
