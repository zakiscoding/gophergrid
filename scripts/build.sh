#!/usr/bin/env bash
# Build the GopherGrid images.
#
# The build context is streamed as a tar archive instead of read from disk, because BuildKit
# can't read files that OneDrive has tagged as cloud files ("invalid file request"). On a
# machine without OneDrive, `docker compose build` works too.
set -euo pipefail
cd "$(dirname "$0")/.."
tag="${TAG:-dev}"
for target in ${*:-scheduler worker ggctl tester}; do
  echo "building gophergrid/$target:$tag"
  tar -c --exclude=.git --exclude=bin --exclude=bench/out --exclude='*.test' -f - . |
    docker build -q --target "$target" -t "gophergrid/$target:$tag" -f Dockerfile - >/dev/null
done
