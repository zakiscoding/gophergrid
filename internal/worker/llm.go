package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LLMConfig points the worker at an OpenAI-compatible server (vLLM) for `llm` jobs.
type LLMConfig struct {
	BaseURL     string // e.g. http://vllm-chat:8000/v1
	APIKey      string // bearer token; never logged or returned
	Model       string // served model name used when a job doesn't name one
	Concurrency int    // requests in flight per job
	Client      *http.Client
}

// llmPayload is an `llm` job: a list of prompts sent to the server as chat completions.
//
// Decoding is greedy (temperature 0) with a fixed seed unless the job asks otherwise, so a
// retried attempt produces the same text. The scheduler's lease fencing already keeps only
// one result per job; determinism makes that result independent of which attempt won.
type llmPayload struct {
	Model       string   `json:"model"`
	Prompts     []string `json:"prompts"`
	MaxTokens   int      `json:"max_tokens"`
	Temperature float64  `json:"temperature"`
	Seed        int64    `json:"seed"`
	System      string   `json:"system"`
	KeepText    int      `json:"keep_text"` // characters of each completion kept in the result
}

type llmOutput struct {
	Index            int    `json:"index"`
	Digest           string `json:"digest"` // sha256 of the full completion, first 8 bytes
	Text             string `json:"text,omitempty"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	FinishReason     string `json:"finish_reason"`
	LatencyMS        int64  `json:"latency_ms"`
}

// LLMRunner returns the `llm` runner, or nil if no server is configured.
func LLMRunner(cfg LLMConfig) Runner {
	if cfg.BaseURL == "" {
		return nil
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 8
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 5 * time.Minute}
	}
	return func(ctx context.Context, t Task) (json.RawMessage, error) { return runLLM(ctx, cfg, t) }
}

func runLLM(ctx context.Context, cfg LLMConfig, t Task) (json.RawMessage, error) {
	p := llmPayload{Model: cfg.Model, MaxTokens: 64, KeepText: 200}
	if err := decode(t, &p); err != nil {
		return nil, err
	}
	switch {
	case p.Model == "":
		return nil, permanent("llm job needs a model (or LLM_MODEL on the worker)")
	case len(p.Prompts) == 0 || len(p.Prompts) > 1024:
		return nil, permanent("llm job needs 1-1024 prompts")
	case p.MaxTokens < 1 || p.MaxTokens > 4096:
		return nil, permanent("max_tokens must be in [1, 4096]")
	case p.Temperature < 0 || p.Temperature > 2:
		return nil, permanent("temperature must be in [0, 2]")
	case p.KeepText < 0:
		p.KeepText = 0
	}

	start := time.Now()
	outs := make([]llmOutput, len(p.Prompts))
	errs := make([]error, len(p.Prompts))
	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i, prompt := range p.Prompts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			defer func() { <-sem }()
			// One request id per prompt and attempt: server logs show which attempt sent what.
			reqID := fmt.Sprintf("gg-%s-a%d-p%d", t.JobID, t.Attempt, i)
			outs[i], errs[i] = chatCompletion(ctx, cfg, p, prompt, reqID)
			outs[i].Index = i
			if errs[i] != nil {
				cancel() // the attempt fails as a whole; stop sending the rest
			}
		}()
	}
	wg.Wait()
	// Our own cancel() after a failed request shouldn't hide that request's error.
	if first := firstReal(errs); first != nil {
		return nil, first
	}
	if err := parent.Err(); err != nil {
		return nil, err // lease lost or timed out
	}

	wall := time.Since(start)
	var promptTok, complTok int
	lat := make([]int64, len(outs))
	for i, o := range outs {
		promptTok += o.PromptTokens
		complTok += o.CompletionTokens
		lat[i] = o.LatencyMS
		if p.KeepText < len(outs[i].Text) {
			outs[i].Text = truncateRunes(outs[i].Text, p.KeepText)
		}
	}
	slices.Sort(lat)
	return json.Marshal(map[string]any{
		"model": p.Model, "prompts": len(p.Prompts), "max_tokens": p.MaxTokens,
		"temperature": p.Temperature, "seed": p.Seed,
		"prompt_tokens": promptTok, "completion_tokens": complTok,
		"wall_ms":             wall.Milliseconds(),
		"completion_tok_s":    round1(float64(complTok) / wall.Seconds()),
		"latency_p50_ms":      percentile(lat, 50),
		"latency_p95_ms":      percentile(lat, 95),
		"latency_max_ms":      lat[len(lat)-1],
		"outputs_digest":      outputsDigest(outs),
		"outputs":             outs,
		"request_concurrency": cfg.Concurrency,
	})
}

func chatCompletion(ctx context.Context, cfg LLMConfig, p llmPayload, prompt, reqID string) (llmOutput, error) {
	msgs := []map[string]string{}
	if p.System != "" {
		msgs = append(msgs, map[string]string{"role": "system", "content": p.System})
	}
	msgs = append(msgs, map[string]string{"role": "user", "content": prompt})
	body, _ := json.Marshal(map[string]any{
		"model": p.Model, "messages": msgs, "max_tokens": p.MaxTokens,
		"temperature": p.Temperature, "seed": p.Seed,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return llmOutput{}, permanent("bad LLM URL: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", reqID)
	if cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	}
	t0 := time.Now()
	resp, err := cfg.Client.Do(req)
	if err != nil {
		return llmOutput{}, fmt.Errorf("llm request: %w", err) // network: retry
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return llmOutput{}, fmt.Errorf("llm response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("llm server: HTTP %d: %s", resp.StatusCode, truncateRunes(strings.TrimSpace(string(raw)), 300))
		// 4xx means the request itself is wrong (bad model, prompt too long, bad key):
		// retrying won't help. 408/429 and 5xx are the server being busy or down.
		if resp.StatusCode >= 400 && resp.StatusCode < 500 &&
			resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusTooManyRequests {
			return llmOutput{}, permanent("%s", msg)
		}
		return llmOutput{}, errors.New(msg)
	}
	var r struct {
		Choices []struct {
			Message      struct{ Content string } `json:"message"`
			FinishReason string                   `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &r); err != nil || len(r.Choices) == 0 {
		return llmOutput{}, fmt.Errorf("llm server: unexpected response: %s", truncateRunes(string(raw), 200))
	}
	text := r.Choices[0].Message.Content
	sum := sha256.Sum256([]byte(text))
	return llmOutput{
		Digest: hex.EncodeToString(sum[:8]), Text: text,
		PromptTokens: r.Usage.PromptTokens, CompletionTokens: r.Usage.CompletionTokens,
		FinishReason: r.Choices[0].FinishReason, LatencyMS: time.Since(t0).Milliseconds(),
	}, nil
}

// outputsDigest hashes the per-prompt digests in order: one value to compare across attempts.
func outputsDigest(outs []llmOutput) string {
	h := sha256.New()
	for _, o := range outs {
		h.Write([]byte(strconv.Itoa(o.Index) + ":" + o.Digest + "\n"))
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

func firstReal(errs []error) error {
	for _, e := range errs {
		if e != nil && !errors.Is(e, context.Canceled) {
			return e
		}
	}
	return nil
}

func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	i := (len(sorted)*p + 99) / 100 // nearest-rank
	return sorted[max(0, min(len(sorted)-1, i-1))]
}

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
