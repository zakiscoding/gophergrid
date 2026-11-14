# syntax=docker/dockerfile:1
# Targets: scheduler, worker, autoscaler, ggctl, ggbench, fakellm, tester.  docker build --target scheduler -t gophergrid/scheduler .
ARG GO_IMAGE=golang:1.27-bookworm

# The build stage runs on the build machine's platform and cross-compiles for the target, so
# `--platform linux/arm64` (the homelab iMac) builds natively on an amd64 laptop.
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
ENV CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH}
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot AS scheduler
COPY --from=build /out/scheduler /scheduler
EXPOSE 8080 9090
ENTRYPOINT ["/scheduler"]

# The worker runs on Debian so `docker run` (container jobs) and nvidia-smi (injected by the
# NVIDIA container toolkit in GPU_MODE=nvidia) are available.
FROM debian:bookworm-slim AS worker
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=docker:29-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=build /out/worker /worker
EXPOSE 9100
ENTRYPOINT ["/worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS ggctl
COPY --from=build /out/ggctl /ggctl
ENTRYPOINT ["/ggctl"]

# The autoscaler runs as root to use the mounted Docker socket.
FROM gcr.io/distroless/static-debian12 AS autoscaler
COPY --from=build /out/autoscaler /autoscaler
ENTRYPOINT ["/autoscaler"]

# A deterministic OpenAI-compatible stand-in for vLLM, for the integration tests.
FROM gcr.io/distroless/static-debian12:nonroot AS fakellm
COPY --from=build /out/fakellm /fakellm
EXPOSE 8000
ENTRYPOINT ["/fakellm"]

# ggbench runs as root so it can use the mounted Docker socket for chaos runs.
FROM gcr.io/distroless/static-debian12 AS ggbench
COPY --from=build /out/ggbench /ggbench
WORKDIR /work
ENTRYPOINT ["/ggbench"]

# Integration tests run in a container on the compose network and kill workers through the
# Docker Engine API on the mounted socket.
FROM ${GO_IMAGE} AS tester
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# -p 1: packages share the database, so run them one at a time.
ENTRYPOINT ["go", "test", "-p", "1", "-count=1", "-tags=integration", "-v", "./internal/store/...", "./test/integration/..."]
