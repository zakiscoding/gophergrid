"""llm jobs on the homelab's live vLLM, with and without killing the GPU PC worker.

Run 1 submits N `llm` jobs (8 distinct prompts each) and waits. Run 2 submits the same
payloads again and SIGKILLs the gpu-pc worker container once it holds running jobs;
the script starts it again --down-s later. Then, from Postgres:
  * every job has exactly one result (results rows == distinct job ids == jobs)
  * how many jobs needed a second attempt, and which worker recorded each result
and from the API: throughput, job latency, and how often a job's outputs match the same
payload's outputs in run 1 (greedy decoding; vLLM batching can still change a token).

Needs: the tailnet, deploy/homelab/.env (POSTGRES_PASSWORD; never printed), Docker on this
machine (for psql) and `ssh zak-ts` for the kill.

    python bench/llm_homelab.py -n 100 --out bench/out/homelab-05-llm.json
"""

import argparse
import json
import os
import statistics
import subprocess
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TOPICS = ["Raft", "TCP slow start", "B-trees", "CUDA warps", "Postgres MVCC", "consistent hashing",
          "KV caching", "Go channels", "Linux cgroups", "DNS", "HTTP/2", "vector search", "LSM trees"]
ASKS = ["Explain {} in two sentences.", "Give one common mistake with {}.", "Name a trade-off of {}.",
        "Write one interview question about {}.", "Summarize {} for a manager.", "When would you avoid {}?",
        "Give a real-world analogy for {}.", "List two metrics to watch for {}."]


def payload(i):
    """Job i's payload: 8 prompts, distinct across jobs (no two jobs share a prompt set)."""
    prompts = [ASKS[k].format(TOPICS[(i + k) % len(TOPICS)]) + f" (case {i})" for k in range(len(ASKS))]
    return {"prompts": prompts, "max_tokens": 64, "seed": 0, "keep_text": 80}


def api(base, method, path, body=None):
    req = urllib.request.Request(base + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    for attempt in range(5):  # the tailnet drops the odd connection; retry reads and the submit
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except OSError:
            if attempt == 4:
                raise
            time.sleep(2)


def submit(base, n, cpu):
    specs = [{"type": "llm", "payload": payload(i), "max_attempts": 5, "timeout_ms": 120000,
              "resources": {"cpu_millis": cpu, "memory_mb": 64}} for i in range(n)]
    return [j["id"] for j in api(base, "POST", "/v1/jobs/batch", {"jobs": specs})["jobs"]]


def wait(base, ids, timeout_s=900):
    t0 = time.time()
    while True:
        jobs = [api(base, "GET", f"/v1/jobs/{i}") for i in ids]
        open_ = [j for j in jobs if j["status"] in ("queued", "running")]
        if not open_:
            return jobs
        if time.time() - t0 > timeout_s:
            raise TimeoutError(f"{len(open_)} jobs still open")
        time.sleep(3)


def psql(env, sql):
    """Run one query on the homelab Postgres; returns stdout. The password goes in via env."""
    imac = os.environ.get("IMAC_TS_IP", "100.91.113.98")
    out = subprocess.run(
        ["docker", "run", "--rm", "-e", "PGPASSWORD", "postgres:17-alpine", "psql", "-h", imac, "-p", "5433",
         "-U", "gophergrid", "-d", "gophergrid", "-At", "-F", ",", "-c", sql],
        capture_output=True, text=True, env={**os.environ, "PGPASSWORD": env["POSTGRES_PASSWORD"]}, check=True)
    return out.stdout.strip()


def summarize(name, jobs):
    t = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
    done = [j for j in jobs if j["status"] == "succeeded"]
    lat = sorted((t(j["finished_at"]) - t(j["created_at"])).total_seconds() for j in done)
    first = min(t(j["created_at"]) for j in jobs)
    last = max(t(j["finished_at"]) for j in done)
    wall = (last - first).total_seconds()
    tokens = sum(j["result"]["completion_tokens"] for j in done)
    q = lambda p: round(lat[min(len(lat) - 1, max(0, int(round(p / 100 * len(lat))) - 1))], 2)
    req_lat = sorted(o["latency_ms"] for j in done for o in j["result"]["outputs"])
    return {
        "run": name, "jobs": len(jobs), "succeeded": len(done), "failed": len(jobs) - len(done),
        "retried": sum(1 for j in jobs if j["attempt"] > 1), "wall_s": round(wall, 1),
        "jobs_per_s": round(len(done) / wall, 2), "completion_tokens": tokens,
        "completion_tok_s": round(tokens / wall, 1),
        "job_latency_s": {"p50": q(50), "p95": q(95), "max": round(lat[-1], 2)},
        "request_latency_ms": {"p50": req_lat[len(req_lat) // 2], "p95": req_lat[int(len(req_lat) * 0.95) - 1]},
        "workers": sorted({j["worker_id"] for j in done}),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=100)
    ap.add_argument("--cpu", type=int, default=1000, help="millicores per job: 8 jobs at once on the 8-core worker")
    ap.add_argument("--down-s", type=float, default=5.0, help="seconds the worker stays dead")
    ap.add_argument("--min-held", type=int, default=6, help="kill once the worker runs this many run-2 jobs")
    ap.add_argument("--env-file", default=str(ROOT / "deploy/homelab/.env"))
    ap.add_argument("--out", default=str(ROOT / "bench" / "out" / "homelab-05-llm.json"))
    args = ap.parse_args()
    env = dict(l.split("=", 1) for l in Path(args.env_file).read_text().split() if "=" in l)
    base = f"http://{os.environ.get('IMAC_TS_IP', '100.91.113.98')}:8090"

    print("run 1: no failures")
    ids1 = submit(base, args.n, args.cpu)
    jobs1 = wait(base, ids1)
    s1 = summarize("clean", jobs1)
    print(json.dumps(s1))

    print("run 2: SIGKILL the gpu-pc worker mid-run")
    ids2 = submit(base, args.n, args.cpu)
    kill = {}

    def killer():
        # Kill only once the worker holds running jobs of this run, so the kill lands mid-request.
        held, t0 = 0, time.time()
        while time.time() - t0 < 60:
            held = int(psql(env, f"SELECT count(*) FROM jobs WHERE status='running' AND worker_id='gpu-pc' "
                                 f"AND id = ANY('{{{','.join(ids2)}}}'::uuid[])") or 0)
            if held >= args.min_held:
                break
            time.sleep(0.3)
        kill["held_running"] = held
        kill["at"] = datetime.now(timezone.utc).isoformat()
        r = subprocess.run(["ssh", "-o", "BatchMode=yes", "zak-ts", "wsl -d Ubuntu -u root -- docker kill "
                            "--signal KILL gophergrid-gpu-worker-1"], capture_output=True, text=True)
        kill["ok"] = r.returncode == 0
        print(f"killed gpu-pc worker holding {held} running jobs: {kill['ok']}")
        # `docker kill` counts as a manual stop, so the restart policy doesn't bring the worker
        # back. Start it again after a short outage, like a crashed process under a supervisor.
        time.sleep(args.down_s)
        subprocess.run(["ssh", "-o", "BatchMode=yes", "zak-ts", "wsl -d Ubuntu -u root -- docker start "
                        "gophergrid-gpu-worker-1"], capture_output=True, text=True)
        kill["restarted_at"] = datetime.now(timezone.utc).isoformat()
        kill["down_s"] = args.down_s

    th = threading.Thread(target=killer)
    th.start()
    jobs2 = wait(base, ids2)
    th.join()
    s2 = summarize("worker-killed", jobs2)
    s2["kill"] = kill
    print(json.dumps(s2))

    all_ids = "','".join(ids1 + ids2)
    rows = psql(env, f"SELECT count(*), count(DISTINCT job_id) FROM results WHERE job_id IN ('{all_ids}')")
    results, distinct = map(int, rows.split(","))
    same = sum(1 for a, b in zip(jobs1, jobs2)
               if a["status"] == b["status"] == "succeeded"
               and a["result"]["outputs_digest"] == b["result"]["outputs_digest"])
    retried_ids = [j["id"] for j in jobs2 if j["attempt"] > 1]
    retried_same = sum(1 for a, b in zip(jobs1, jobs2) if b["attempt"] > 1
                       and a["result"]["outputs_digest"] == b["result"]["outputs_digest"])
    prompt_same = sum(1 for a, b in zip(jobs1, jobs2) for x, y in zip(a["result"]["outputs"], b["result"]["outputs"])
                      if x["digest"] == y["digest"])
    out = {
        "what": "GopherGrid llm jobs on the homelab: live vLLM (Qwen2.5-1.5B-Instruct, RTX 5070) via the gpu-pc worker",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "jobs_per_run": args.n, "prompts_per_job": 8, "max_tokens": 64, "cpu_millis_per_job": args.cpu,
        "runs": [s1, s2],
        "exactly_once": {"jobs": 2 * args.n, "result_rows": results, "distinct_jobs_with_results": distinct,
                         "duplicates": results - distinct},
        "determinism": {"jobs_same_outputs_as_run1": same, "of": args.n,
                        "prompts_same_output": prompt_same, "of_prompts": 8 * args.n,
                        "retried_jobs": len(retried_ids), "retried_jobs_same_as_run1": retried_same},
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, indent=2))
    print(json.dumps({k: out[k] for k in ("exactly_once", "determinism")}, indent=2))
    print("wrote", args.out)


if __name__ == "__main__":
    main()
