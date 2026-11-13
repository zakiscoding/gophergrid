package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/zakiscoding/gophergrid/internal/jobs"
	"github.com/zakiscoding/gophergrid/internal/metrics"
	"github.com/zakiscoding/gophergrid/internal/store"
)

// InferenceBatch asks for a batched LLM inference run, split into shards that run on GPUs
// in parallel. Workers simulate the GPU work unless they run in GPU_MODE=nvidia with a
// container runner.
type InferenceBatch struct {
	Model          string  `json:"model"`
	Prompts        int     `json:"prompts"`
	ShardSize      int     `json:"shard_size"`
	Tokens         int     `json:"tokens"`
	MSPerToken     float64 `json:"ms_per_token"`
	GPUsPerShard   int     `json:"gpus_per_shard"`
	GPUMemoryMB    int64   `json:"gpu_memory_mb"`
	Priority       int     `json:"priority"`
	TimeoutMS      int64   `json:"timeout_ms"`
	IdempotencyKey string  `json:"idempotency_key,omitempty"`
}

const maxShards = 10_000

func (b *InferenceBatch) shards() ([]jobs.Spec, error) {
	if b.Model == "" {
		b.Model = "qwen2.5-1.5b-instruct"
	}
	if b.Prompts < 1 || b.Prompts > 1_000_000 {
		return nil, errors.New("prompts must be in [1, 1000000]")
	}
	if b.ShardSize == 0 {
		b.ShardSize = 64
	}
	if b.ShardSize < 1 {
		return nil, errors.New("shard_size must be positive")
	}
	if b.Tokens == 0 {
		b.Tokens = 128
	}
	if b.Tokens < 1 || b.Tokens > 32_768 {
		return nil, errors.New("tokens must be in [1, 32768]")
	}
	if b.MSPerToken == 0 {
		b.MSPerToken = 0.05
	}
	if b.GPUsPerShard == 0 {
		b.GPUsPerShard = 1
	}
	n := (b.Prompts + b.ShardSize - 1) / b.ShardSize
	if n > maxShards {
		return nil, fmt.Errorf("%d shards; at most %d (raise shard_size)", n, maxShards)
	}
	out := make([]jobs.Spec, 0, n)
	for i := 0; i < n; i++ {
		prompts := min(b.ShardSize, b.Prompts-i*b.ShardSize)
		payload, _ := json.Marshal(map[string]any{"model": b.Model, "prompts": prompts, "tokens": b.Tokens,
			"ms_per_token": b.MSPerToken, "shard": i})
		sp := jobs.Spec{Type: jobs.TypeInference, Payload: payload, Priority: b.Priority, TimeoutMS: b.TimeoutMS,
			Resources: jobs.Resources{CPUMillis: 250, MemoryMB: 512, GPUs: b.GPUsPerShard, GPUMemoryMB: b.GPUMemoryMB}}
		if err := sp.Normalize(); err != nil {
			return nil, err
		}
		out = append(out, sp)
	}
	return out, nil
}

func (s *Server) submitInference(w http.ResponseWriter, r *http.Request) {
	var req InferenceBatch
	if !readJSON(w, r, &req, 1<<16) {
		return
	}
	if k := r.Header.Get("Idempotency-Key"); k != "" && req.IdempotencyKey == "" {
		req.IdempotencyKey = k
	}
	shards, err := req.shards()
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	id, created, err := s.Store.CreateBatch(r.Context(), "inference", req, req.IdempotencyKey, shards)
	if err != nil {
		s.fail(w, "create batch", err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
		metrics.JobsSubmitted.WithLabelValues(jobs.TypeInference).Add(float64(len(shards)))
	}
	writeJSON(w, status, map[string]any{"batch_id": id, "shards": len(shards)})
}

func (s *Server) getBatch(w http.ResponseWriter, r *http.Request) {
	b, err := s.Store.GetBatch(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "batch not found")
		return
	}
	if err != nil {
		s.fail(w, "get batch", err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}
