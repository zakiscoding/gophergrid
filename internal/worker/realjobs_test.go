package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeChat is a minimal OpenAI chat server whose reply depends only on the prompt.
func fakeChat(t *testing.T, status int, delay time.Duration) (*httptest.Server, *atomic.Int64) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer k" ||
			!strings.HasPrefix(r.Header.Get("X-Request-Id"), "gg-job1-a") {
			http.Error(w, "bad request shape", http.StatusTeapot)
			return
		}
		if status != http.StatusOK {
			http.Error(w, `{"error":"nope"}`, status)
			return
		}
		var req struct {
			Messages []struct{ Content string }
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]string{"content": "re: " + req.Messages[0].Content},
				"finish_reason": "stop"}},
			"usage": map[string]int{"prompt_tokens": 5, "completion_tokens": 3},
		})
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func llmTask(attempt int, payload string) Task {
	return Task{JobID: "job1", Attempt: attempt, Type: "llm", Payload: json.RawMessage(payload)}
}

func mustNot(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLLMRunnerDeterministicAcrossAttempts(t *testing.T) {
	srv, calls := fakeChat(t, http.StatusOK, 0)
	run := LLMRunner(LLMConfig{BaseURL: srv.URL + "/v1/", APIKey: "k", Model: "m", Concurrency: 2})
	payload := `{"prompts":["a","b","c"],"max_tokens":8}`
	var digests []string
	for attempt := 1; attempt <= 2; attempt++ {
		out, err := run(context.Background(), llmTask(attempt, payload))
		mustNot(t, err)
		var r struct {
			Prompts          int     `json:"prompts"`
			CompletionTokens int     `json:"completion_tokens"`
			Digest           string  `json:"outputs_digest"`
			Temperature      float64 `json:"temperature"`
			Outputs          []llmOutput
		}
		mustNot(t, json.Unmarshal(out, &r))
		if r.Prompts != 3 || r.CompletionTokens != 9 || len(r.Outputs) != 3 || r.Outputs[1].Text != "re: b" || r.Temperature != 0 {
			t.Fatalf("unexpected result %s", out)
		}
		digests = append(digests, r.Digest)
	}
	if digests[0] != digests[1] {
		t.Fatalf("attempts disagree: %v", digests)
	}
	if calls.Load() != 6 {
		t.Fatalf("want 6 requests, got %d", calls.Load())
	}
}

func TestLLMRunnerErrorClasses(t *testing.T) {
	for _, tc := range []struct {
		status    int
		permanent bool
	}{{http.StatusBadRequest, true}, {http.StatusUnauthorized, true}, {http.StatusTooManyRequests, false}, {http.StatusServiceUnavailable, false}} {
		srv, _ := fakeChat(t, tc.status, 0)
		run := LLMRunner(LLMConfig{BaseURL: srv.URL + "/v1", APIKey: "k", Model: "m"})
		_, err := run(context.Background(), llmTask(1, `{"prompts":["a","b"]}`))
		var pe PermanentError
		if err == nil || errors.As(err, &pe) != tc.permanent {
			t.Errorf("HTTP %d: err=%v, want permanent=%v", tc.status, err, tc.permanent)
		}
	}
	// A dead server is retryable: the job should move on, not fail.
	run := LLMRunner(LLMConfig{BaseURL: "http://127.0.0.1:1/v1", Model: "m"})
	_, err := run(context.Background(), llmTask(1, `{"prompts":["a"]}`))
	var pe PermanentError
	if err == nil || errors.As(err, &pe) {
		t.Fatalf("unreachable server: err=%v, want a retryable error", err)
	}
	// Bad payloads are permanent and send nothing.
	for _, p := range []string{`{"prompts":[]}`, `{"prompts":["a"],"max_tokens":0}`, `{"prompts":["a"],"temperature":3}`} {
		if _, err := run(context.Background(), llmTask(1, p)); !errors.As(err, &pe) {
			t.Errorf("payload %s: err=%v, want permanent", p, err)
		}
	}
	if LLMRunner(LLMConfig{}) != nil {
		t.Fatal("no URL should mean no llm runner")
	}
}

func TestLLMRunnerStopsWhenLeaseIsLost(t *testing.T) {
	srv, _ := fakeChat(t, http.StatusOK, 10*time.Second)
	run := LLMRunner(LLMConfig{BaseURL: srv.URL + "/v1", APIKey: "k", Model: "m"})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := run(ctx, llmTask(1, `{"prompts":["a","b","c"]}`))
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 3*time.Second {
		t.Fatalf("err=%v after %s, want a prompt context error", err, time.Since(start))
	}
}

func TestBenchArgsBuildsAFixedCommand(t *testing.T) {
	cfg := BenchConfig{Image: "vllm/vllm-openai:v0.32.0", LabDir: "/root/gpu-systems-lab", HFCache: "/root/.cache/huggingface",
		OutDir: "/root/gophergrid/bench-out", GPUMode: "nvidia"}
	task := Task{JobID: "0123456789abcdef", Attempt: 2, GPUIDs: []string{"GPU-abc"}}
	p := benchPayload{Suite: "vllm-sweep", Label: "awq", Model: "Qwen/Qwen2.5-1.5B-Instruct-AWQ", Concurrency: []int{1, 8},
		Quantization: "awq_marlin", Speculative: json.RawMessage(`{"method": "ngram", "num_speculative_tokens": 4, "prompt_lookup_max": 4}`)}
	args, rel, glob, err := benchArgs(cfg, task, p)
	mustNot(t, err)
	cmd := strings.Join(args, " ")
	for _, want := range []string{
		`--gpus "device=GPU-abc"`, "--name gg-bench-01234567-a2", "-v /root/gpu-systems-lab:/lab:ro",
		"-v /root/gophergrid/bench-out/0123456789abcdef/a2:/out", "-e HF_HUB_OFFLINE=1",
		"/lab/serving/bench_sweep.py --launch --label awq --model Qwen/Qwen2.5-1.5B-Instruct-AWQ",
		"--concurrency 1,8",
		`--extra-serve-args=--quantization awq_marlin --speculative-config {"method":"ngram","num_speculative_tokens":4,"prompt_lookup_max":4}`,
	} {
		if !strings.Contains(cmd, want) {
			t.Errorf("command lacks %q:\n%s", want, cmd)
		}
	}
	if rel != "0123456789abcdef/a2" || glob != "awq_summary_*.json" {
		t.Errorf("rel=%q glob=%q", rel, glob)
	}
}

func TestBenchArgsRejectsUnsafeOrBadPayloads(t *testing.T) {
	cfg := BenchConfig{Image: "img", LabDir: "/lab", OutDir: "/out", GPUMode: "nvidia"}
	one := Task{JobID: "j", Attempt: 1, GPUIDs: []string{"GPU-1"}}
	ok := benchPayload{Suite: "vllm-sweep", Label: "x", Model: "a/b"}
	for name, mut := range map[string]func(*benchPayload){
		"suite":        func(p *benchPayload) { p.Suite = "shell" },
		"label":        func(p *benchPayload) { p.Label = "../x" },
		"model inject": func(p *benchPayload) { p.Model = "a/b --trust-remote-code" },
		"model path":   func(p *benchPayload) { p.Model = "/etc/passwd" },
		"quant":        func(p *benchPayload) { p.Quantization = "--x" },
		"mem":          func(p *benchPayload) { p.GPUMemUtil = 0.99 },
		"conc":         func(p *benchPayload) { p.Concurrency = []int{0} },
		"len vs max":   func(p *benchPayload) { p.InputLen, p.OutputLen, p.MaxModelLen = 4000, 1000, 4096 },
		"spec method":  func(p *benchPayload) { p.Speculative = json.RawMessage(`{"method":"x","num_speculative_tokens":2}`) },
		"spec extra key": func(p *benchPayload) {
			p.Speculative = json.RawMessage(`{"method":"ngram","num_speculative_tokens":2,"x":1}`)
		},
		"spec tokens": func(p *benchPayload) {
			p.Speculative = json.RawMessage(`{"method":"ngram","num_speculative_tokens":0}`)
		},
	} {
		p := ok
		mut(&p)
		var pe PermanentError
		if _, _, _, err := benchArgs(cfg, one, p); !errors.As(err, &pe) {
			t.Errorf("%s: err=%v, want permanent", name, err)
		}
	}
	if _, _, _, err := benchArgs(cfg, Task{JobID: "j", Attempt: 1}, ok); err == nil {
		t.Error("no GPU: want an error")
	}
	if _, _, _, err := benchArgs(cfg, one, ok); err != nil {
		t.Errorf("valid payload: %v", err)
	}
	if BenchmarkRunner(BenchConfig{Image: "i", LabDir: "/l", OutDir: "/o", GPUMode: "sim"}) != nil {
		t.Error("benchmark runner must need a real GPU")
	}
}
