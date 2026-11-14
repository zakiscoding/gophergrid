"""Submit the gpu-systems-lab inference sweeps as GopherGrid benchmark jobs, one after another.

Each job runs serving/bench_sweep.py --launch on the GPU PC's RTX 5070: it starts a vLLM
server with the config below, sweeps concurrency 1/4/16/64, and stops it. They need the
whole card, so vllm-chat must be stopped and the gpu-pc worker restarted with a small
GPU_RESERVED_MB (docs/homelab.md). Jobs are submitted one at a time, so the order is fixed.

    python bench/submit_inference_sweeps.py --out bench/out/homelab-07-inference-sweeps.json
"""

import argparse
import json
import os
import time
import urllib.request

M = "Qwen/Qwen2.5-1.5B-Instruct"
COMMON = {"suite": "vllm-sweep", "gpu_mem_util": 0.80, "max_model_len": 4096}
SWEEPS = [
    # precision: random 512-in / 256-out tokens, the same workload as earlier sweeps
    {"label": "fp16", "model": M, "dtype": "float16", "concurrency": [1, 4, 16, 64]},
    {"label": "awq", "model": "Qwen/Qwen2.5-1.5B-Instruct-AWQ", "quantization": "awq_marlin", "dtype": "float16",
     "concurrency": [1, 4, 16, 64]},
    {"label": "fp8", "model": M, "quantization": "fp8", "dtype": "float16", "concurrency": [1, 4, 16, 64]},
    # speculative decoding: natural-language prompts, 256 output tokens
    {"label": "spec-off", "model": M, "dtype": "float16", "dataset": "custom", "concurrency": [1, 4, 16, 64]},
    {"label": "spec-ngram", "model": M, "dtype": "float16", "dataset": "custom", "concurrency": [1, 4, 16, 64],
     "speculative": {"method": "ngram", "num_speculative_tokens": 4, "prompt_lookup_max": 4, "prompt_lookup_min": 2}},
    {"label": "spec-draft", "model": M, "dtype": "float16", "dataset": "custom", "concurrency": [1, 4, 16, 64],
     "speculative": {"method": "draft_model", "model": "Qwen/Qwen2.5-0.5B-Instruct", "num_speculative_tokens": 4}},
]


def api(base, method, path, body=None):
    req = urllib.request.Request(base + path, method=method, headers={"Content-Type": "application/json"},
                                 data=json.dumps(body).encode() if body is not None else None)
    for i in range(5):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except OSError:
            if i == 4:
                raise
            time.sleep(3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="bench/out/homelab-07-inference-sweeps.json")
    ap.add_argument("--only", default="", help="comma-separated labels")
    ap.add_argument("--attempts", type=int, default=2)
    args = ap.parse_args()
    base = f"http://{os.environ.get('IMAC_TS_IP', '100.91.113.98')}:8090"
    sweeps = [s for s in SWEEPS if not args.only or s["label"] in args.only.split(",")]
    records = []
    for s in sweeps:
        spec = {"type": "benchmark", "payload": {**COMMON, **s}, "timeout_ms": 1_200_000,
                "max_attempts": 1 if s["label"] == "fp8" else args.attempts,  # fp8 may simply be unsupported
                "resources": {"cpu_millis": 4000, "memory_mb": 8192, "gpus": 1, "gpu_memory_mb": 10000}}
        job = api(base, "POST", "/v1/jobs", spec)
        t0 = time.time()
        print(f"{s['label']}: job {job['id']}", flush=True)
        while job["status"] in ("queued", "running"):
            time.sleep(10)
            job = api(base, "GET", f"/v1/jobs/{job['id']}")
        rows = ((job.get("result") or {}).get("summary") or {}).get("rows", [])
        print(f"  {job['status']} after {time.time() - t0:.0f}s, attempt {job['attempt']}: "
              + (", ".join(f"c{r['concurrency']}={r['output_tok_s']}" for r in rows) or (job.get("last_error") or "")[-600:]),
              flush=True)
        records.append(job)
        with open(args.out, "w") as f:
            json.dump(records, f, indent=2)


if __name__ == "__main__":
    main()
