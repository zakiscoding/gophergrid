#!/usr/bin/env bash
# Regenerate gen/ from proto/ with protoc in Docker. Generated code is committed.
set -euo pipefail
cd "$(dirname "$0")/.."
src="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 docker run --rm -v "$src":/src -w /src -v gophergrid-gomod:/go/pkg/mod \
  "${GO_IMAGE:-golang:1.27-bookworm}" sh -euc '
    apt-get update -qq && apt-get install -y -qq protobuf-compiler >/dev/null
    GOBIN=/usr/local/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
    GOBIN=/usr/local/bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
    rm -rf gen && mkdir -p gen
    protoc -I proto --go_out=gen --go_opt=module=github.com/zakiscoding/gophergrid/gen \
      --go-grpc_out=gen --go-grpc_opt=module=github.com/zakiscoding/gophergrid/gen \
      proto/gophergrid/v1/*.proto
    protoc --version'
