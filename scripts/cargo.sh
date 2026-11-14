#!/usr/bin/env bash
# Run cargo for worker-rs in Docker, so the host needs only Docker.
#   scripts/cargo.sh test      scripts/cargo.sh clippy --all-targets
# Build output and the registry live in Docker volumes, not in the (OneDrive-synced) repo.
set -euo pipefail
cd "$(dirname "$0")/.."
src="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 exec docker run --rm -i \
  -v "$src":/src -w /src/worker-rs \
  -v gophergrid-cargo-registry:/usr/local/cargo/registry -v gophergrid-cargo-target:/target \
  -e CARGO_TARGET_DIR=/target \
  "${RUST_IMAGE:-rust:1.99-bookworm}" cargo "$@"
