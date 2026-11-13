#!/usr/bin/env bash
# Run the Go toolchain in Docker, so the host needs only Docker.
#   scripts/go.sh test ./...      scripts/go.sh mod tidy
set -euo pipefail
cd "$(dirname "$0")/.."
src="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 exec docker run --rm -i \
  -v "$src":/src -w /src \
  -v gophergrid-gomod:/go/pkg/mod -v gophergrid-gocache:/root/.cache/go-build \
  -e GOFLAGS="${GOFLAGS:-}" -e CGO_ENABLED="${CGO_ENABLED:-0}" \
  "${GO_IMAGE:-golang:1.27-bookworm}" go "$@"
