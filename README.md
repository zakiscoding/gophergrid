# GopherGrid

A distributed compute scheduler in Go. Clients submit jobs over REST, a scheduler places
them on workers by CPU, memory and GPU capacity, and when a worker dies its jobs are
retried elsewhere without ever recording a result twice.

**Status: phase 1 of 4.** A scheduler with a REST API, three workers and a persistent
PostgreSQL job queue.

## Run it

Needs only Docker.

```bash
scripts/build.sh                                  # build images
docker compose up -d                              # postgres, scheduler, worker-1..3
docker compose --profile test run --rm tester     # store tests + integration tests
scripts/go.sh test ./...                          # unit tests, Go in Docker
```

Submit work with `ggctl`:

```bash
docker run --rm --network gophergrid_default -e GOPHERGRID_URL=http://scheduler:8080 \
  gophergrid/ggctl:dev submit -type image -n 100 -cpu 500 -payload '{"width":512,"height":512}' -wait
curl -s localhost:8080/v1/stats
```
