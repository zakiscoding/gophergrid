package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// BenchConfig lets a worker run gpu-systems-lab benchmarks as `benchmark` jobs.
//
// A benchmark job names a suite and its parameters; it never carries a command. The worker
// builds the `docker run` itself from a fixed image and the lab checkout, so the REST API
// can't be used to run arbitrary code on the GPU machine.
type BenchConfig struct {
	Image   string // image with PyTorch and vLLM, e.g. vllm/vllm-openai:v0.32.0
	LabDir  string // host path of a gpu-systems-lab checkout, mounted read-only at /lab
	HFCache string // host path of the Hugging Face cache, mounted at /root/.cache/huggingface
	OutDir  string // host path for raw results; must be mounted at the same path in the worker
	GPUMode string
	Docker  string // docker CLI, "docker" by default
}

// benchPayload is a `benchmark` job.
type benchPayload struct {
	Suite string `json:"suite"` // "matmul" or "vllm-sweep"
	Label string `json:"label"`

	// matmul: benchmarks/smoke_test.py
	Size  int `json:"size"`
	Iters int `json:"iters"`

	// vllm-sweep: serving/bench_sweep.py --launch
	Model          string          `json:"model"`
	Concurrency    []int           `json:"concurrency"`
	InputLen       int             `json:"input_len"`
	OutputLen      int             `json:"output_len"`
	PromptsPerConc int             `json:"prompts_per_conc"`
	MinPrompts     int             `json:"min_prompts"`
	GPUMemUtil     float64         `json:"gpu_mem_util"`
	MaxModelLen    int             `json:"max_model_len"`
	Quantization   string          `json:"quantization"`
	DType          string          `json:"dtype"`
	KVCacheDType   string          `json:"kv_cache_dtype"`
	Speculative    json.RawMessage `json:"speculative"`
	EnforceEager   bool            `json:"enforce_eager"`
	Dataset        string          `json:"dataset"`
	AllowDownload  bool            `json:"allow_download"`
}

var (
	labelRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,47}$`)
	modelRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}/[A-Za-z0-9][A-Za-z0-9._-]{0,95}$`)
	quants  = []string{"", "awq", "awq_marlin", "gptq", "gptq_marlin", "fp8", "compressed-tensors", "bitsandbytes"}
	dtypes  = []string{"", "auto", "float16", "bfloat16"}
	kvTypes = []string{"", "auto", "fp8"}
	sets    = []string{"", "random", "custom"} // custom: the lab's natural-language prompts
	specs   = []string{"ngram", "eagle", "eagle3", "draft_model", "mtp", "suffix"}
)

// BenchmarkRunner returns the `benchmark` runner, or nil if the worker isn't set up for it.
func BenchmarkRunner(cfg BenchConfig) Runner {
	if cfg.Image == "" || cfg.LabDir == "" || cfg.OutDir == "" || cfg.GPUMode != "nvidia" {
		return nil
	}
	if cfg.Docker == "" {
		cfg.Docker = "docker"
	}
	return func(ctx context.Context, t Task) (json.RawMessage, error) { return runBenchmark(ctx, cfg, t) }
}

// benchArgs validates a payload and returns the `docker run` arguments, the directory (relative
// to OutDir) the attempt writes to, and the glob of the file holding its result.
func benchArgs(cfg BenchConfig, t Task, p benchPayload) (args []string, rel, resultGlob string, err error) {
	if len(t.GPUIDs) != 1 {
		return nil, "", "", permanent("benchmark jobs need exactly one GPU (resources.gpus = 1)")
	}
	if !labelRE.MatchString(p.Label) {
		return nil, "", "", permanent("label must match %s", labelRE)
	}
	// One directory per attempt: a retried benchmark never overwrites or mixes with the files
	// of an earlier attempt, and the recorded result names the directory of the attempt that won.
	rel = filepath.ToSlash(filepath.Join(t.JobID, "a"+strconv.Itoa(t.Attempt)))
	name := benchContainer(t)
	args = []string{"run", "--rm", "--name", name,
		"--label", "gophergrid.job=" + t.JobID, "--label", "gophergrid.attempt=" + strconv.Itoa(t.Attempt),
		"--gpus", `"device=` + t.GPUIDs[0] + `"`, "--ipc=host",
		"-v", cfg.LabDir + ":/lab:ro",
		"-v", filepath.ToSlash(filepath.Join(cfg.OutDir, rel)) + ":/out",
		"-e", "VLLM_USE_FLASHINFER_SAMPLER=0",
		"--cpus", strconv.FormatFloat(float64(max(t.Resources.CPUMillis, 1000))/1000, 'f', 3, 64),
		"--memory", strconv.FormatInt(max(t.Resources.MemoryMB, 2048), 10) + "m",
	}
	if cfg.HFCache != "" {
		args = append(args, "-v", cfg.HFCache+":/root/.cache/huggingface")
	}
	if !p.AllowDownload {
		args = append(args, "-e", "HF_HUB_OFFLINE=1")
	}

	switch p.Suite {
	case "matmul":
		size, iters := cmp0(p.Size, 8192), cmp0(p.Iters, 50)
		if size < 256 || size > 16384 || iters < 1 || iters > 1000 {
			return nil, "", "", permanent("matmul: size in [256, 16384], iters in [1, 1000]")
		}
		args = append(args, "--network=none", "--entrypoint", "python3", cfg.Image,
			"/lab/benchmarks/smoke_test.py", "--size", strconv.Itoa(size), "--iters", strconv.Itoa(iters), "--out", "/out")
		return args, rel, "smoke_*.json", nil

	case "vllm-sweep":
		if !modelRE.MatchString(p.Model) {
			return nil, "", "", permanent("model must be a Hugging Face id like Qwen/Qwen2.5-1.5B-Instruct")
		}
		conc := p.Concurrency
		if len(conc) == 0 {
			conc = []int{1, 4, 16, 64}
		}
		cs := make([]string, len(conc))
		for i, c := range conc {
			if c < 1 || c > 512 {
				return nil, "", "", permanent("concurrency levels must be in [1, 512]")
			}
			cs[i] = strconv.Itoa(c)
		}
		in, out := cmp0(p.InputLen, 512), cmp0(p.OutputLen, 256)
		ppc, minp := cmp0(p.PromptsPerConc, 4), cmp0(p.MinPrompts, 16)
		mml := cmp0(p.MaxModelLen, 4096)
		util := p.GPUMemUtil
		if util == 0 {
			util = 0.80
		}
		switch {
		case in < 1 || in > 8192 || out < 1 || out > 4096:
			return nil, "", "", permanent("input_len in [1, 8192], output_len in [1, 4096]")
		case ppc < 1 || ppc > 64 || minp < 1 || minp > 4096:
			return nil, "", "", permanent("prompts_per_conc in [1, 64], min_prompts in [1, 4096]")
		case util < 0.1 || util > 0.95:
			return nil, "", "", permanent("gpu_mem_util must be in [0.1, 0.95]")
		case mml < 256 || mml > 32768 || in+out > mml:
			return nil, "", "", permanent("max_model_len must be in [256, 32768] and fit input_len + output_len")
		case !slices.Contains(quants, p.Quantization):
			return nil, "", "", permanent("quantization must be one of %v", quants[1:])
		case !slices.Contains(dtypes, p.DType):
			return nil, "", "", permanent("dtype must be one of %v", dtypes[1:])
		case !slices.Contains(kvTypes, p.KVCacheDType):
			return nil, "", "", permanent("kv_cache_dtype must be one of %v", kvTypes[1:])
		case !slices.Contains(sets, p.Dataset):
			return nil, "", "", permanent("dataset must be one of %v", sets[1:])
		}
		serve := []string{}
		if p.Quantization != "" {
			serve = append(serve, "--quantization", p.Quantization)
		}
		if p.DType != "" {
			serve = append(serve, "--dtype", p.DType)
		}
		if p.KVCacheDType != "" {
			serve = append(serve, "--kv-cache-dtype", p.KVCacheDType)
		}
		if p.EnforceEager {
			serve = append(serve, "--enforce-eager")
		}
		if len(p.Speculative) > 0 && string(p.Speculative) != "null" {
			spec, err := speculativeConfig(p.Speculative)
			if err != nil {
				return nil, "", "", err
			}
			serve = append(serve, "--speculative-config", spec)
		}
		args = append(args, "--entrypoint", "python3", cfg.Image, "/lab/serving/bench_sweep.py", "--launch",
			"--label", p.Label, "--model", p.Model, "--out-dir", "/out",
			"--concurrency", strings.Join(cs, ","),
			"--input-len", strconv.Itoa(in), "--output-len", strconv.Itoa(out),
			"--prompts-per-conc", strconv.Itoa(ppc), "--min-prompts", strconv.Itoa(minp),
			"--gpu-mem-util", strconv.FormatFloat(util, 'f', 2, 64), "--max-model-len", strconv.Itoa(mml))
		if p.Dataset != "" {
			args = append(args, "--dataset", p.Dataset)
		}
		if len(serve) > 0 {
			// "=" form: the value starts with "--", which argparse would read as a flag.
			args = append(args, "--extra-serve-args="+strings.Join(serve, " "))
		}
		return args, rel, p.Label + "_summary_*.json", nil

	default:
		return nil, "", "", permanent("unknown benchmark suite %q (want matmul or vllm-sweep)", p.Suite)
	}
}

// speculativeConfig checks a speculative-decoding config and re-encodes it compactly (no
// spaces, so it survives bench_sweep.py splitting --extra-serve-args on whitespace).
func speculativeConfig(raw json.RawMessage) (string, error) {
	var s struct {
		Method    string `json:"method"`
		Model     string `json:"model"`
		NumTokens int    `json:"num_speculative_tokens"`
		LookupMax int    `json:"prompt_lookup_max"`
		LookupMin int    `json:"prompt_lookup_min"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return "", permanent("speculative: %v", err)
	}
	if !slices.Contains(specs, s.Method) {
		return "", permanent("speculative.method must be one of %v", specs)
	}
	if s.Model != "" && !modelRE.MatchString(s.Model) {
		return "", permanent("speculative.model must be a Hugging Face id")
	}
	if s.NumTokens < 1 || s.NumTokens > 16 || s.LookupMax < 0 || s.LookupMax > 16 || s.LookupMin < 0 || s.LookupMin > s.LookupMax && s.LookupMax > 0 {
		return "", permanent("speculative: num_speculative_tokens in [1, 16], 0 <= prompt_lookup_min <= prompt_lookup_max <= 16")
	}
	out := map[string]any{"method": s.Method, "num_speculative_tokens": s.NumTokens}
	if s.Model != "" {
		out["model"] = s.Model
	}
	if s.LookupMax > 0 {
		out["prompt_lookup_max"] = s.LookupMax
	}
	if s.LookupMin > 0 {
		out["prompt_lookup_min"] = s.LookupMin
	}
	b, _ := json.Marshal(out)
	return string(b), nil
}

func benchContainer(t Task) string {
	id := t.JobID
	if len(id) > 8 {
		id = id[:8]
	}
	return "gg-bench-" + id + "-a" + strconv.Itoa(t.Attempt)
}

func runBenchmark(ctx context.Context, cfg BenchConfig, t Task) (json.RawMessage, error) {
	var p benchPayload
	if err := decode(t, &p); err != nil {
		return nil, err
	}
	args, rel, glob, err := benchArgs(cfg, t, p)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(cfg.OutDir, rel)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("benchmark output dir: %w", err)
	}

	// An earlier attempt of this job that lost its lease may still hold the GPU on this
	// machine. Remove it first, so two attempts never share the card.
	removeJobContainers(cfg.Docker, t.JobID)
	start := time.Now()
	var out, errb bytes.Buffer
	cmd := exec.Command(cfg.Docker, args...)
	cmd.Stdout, cmd.Stderr = &limited{&out, 64 << 10}, &limited{&errb, 16 << 10}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("docker run: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err = <-done:
	case <-ctx.Done():
		// Killing the docker CLI would leave the container (and the GPU) busy. Remove the
		// container itself; the CLI then returns.
		removeContainer(cfg.Docker, benchContainer(t))
		<-done
		return nil, ctx.Err()
	}
	if err != nil {
		tail := tailLines(out.String()+"\n"+errb.String(), 25)
		return nil, fmt.Errorf("benchmark %s failed: %v: %s", p.Suite, err, tail)
	}

	matches, _ := filepath.Glob(filepath.Join(dir, glob))
	if len(matches) == 0 {
		return nil, fmt.Errorf("benchmark %s finished without writing %s: %s", p.Suite, glob, tailLines(out.String(), 10))
	}
	slices.Sort(matches)
	summary, err := os.ReadFile(matches[len(matches)-1])
	if err != nil || !json.Valid(summary) {
		return nil, fmt.Errorf("benchmark result %s unreadable: %v", matches[len(matches)-1], err)
	}
	entries, _ := os.ReadDir(dir)
	files := make([]string, 0, len(entries))
	for _, e := range entries {
		files = append(files, e.Name())
	}
	return json.Marshal(map[string]any{
		"suite": p.Suite, "label": p.Label, "attempt": t.Attempt, "gpu_ids": t.GPUIDs,
		"raw_dir": rel, "files": files, "wall_s": round1(time.Since(start).Seconds()),
		"summary": json.RawMessage(summary),
	})
}

func removeJobContainers(docker, jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ids, err := exec.CommandContext(ctx, docker, "ps", "-aq", "--filter", "label=gophergrid.job="+jobID).Output()
	if err != nil {
		return
	}
	for _, id := range strings.Fields(string(ids)) {
		_ = exec.CommandContext(ctx, docker, "rm", "-f", id).Run()
	}
}

func removeContainer(docker, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = exec.CommandContext(ctx, docker, "rm", "-f", name).Run()
}

func cmp0(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
