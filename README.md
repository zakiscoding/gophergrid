# GopherGrid

A distributed compute scheduler in Go. Clients submit jobs over REST, a scheduler places
them on workers by CPU, memory and GPU capacity, and when a worker dies its jobs are
retried elsewhere without ever recording a result twice.

**Status: phase 2 of 4.** Two scheduler instances elect a leader in etcd; workers talk to it
over gRPC, heartbeat every 500 ms, and hold renewable leases on their jobs. Killing a worker
or the leader mid-run loses no jobs and records no duplicate results.

## Run it

Needs only Docker.

```bash
scripts/build.sh                                  # build images
docker compose up -d                              # postgres, etcd, scheduler-1..2, worker-1..3
docker compose --profile test run --rm tester     # store tests + integration tests
scripts/go.sh test ./...                          # unit tests, Go in Docker
```

Submit work with `ggctl`:

```bash
docker run --rm --network gophergrid_default -e GOPHERGRID_URL=http://scheduler-1:8080 \
  gophergrid/ggctl:dev submit -type image -n 100 -cpu 500 -payload '{"width":512,"height":512}' -wait
curl -s localhost:8080/v1/stats      # either scheduler: :8080 or :8081
curl -s localhost:8080/v1/leader
```
