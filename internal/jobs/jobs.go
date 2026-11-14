// Package jobs defines the job model shared by the scheduler, the store and the workers.
package jobs

import (
	"encoding/json"
	"fmt"
	"time"
)

// Status is a job's lifecycle state.
//
//	queued -> running -> succeeded
//	   ^         |
//	   +---------+  (lease expired or attempt failed, attempts left)
//	             |
//	             +--> failed (no attempts left, or a permanent error)
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Resources is what a job asks for, or what a worker offers.
type Resources struct {
	CPUMillis   int64 `json:"cpu_millis"`
	MemoryMB    int64 `json:"memory_mb"`
	GPUs        int   `json:"gpus"`
	GPUMemoryMB int64 `json:"gpu_memory_mb"` // per GPU
}

// Spec is what a client submits.
type Spec struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       int             `json:"priority"`
	Resources      Resources       `json:"resources"`
	TimeoutMS      int64           `json:"timeout_ms"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// Job is a stored job.
type Job struct {
	ID             string          `json:"id"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	Resources      Resources       `json:"resources"`
	TimeoutMS      int64           `json:"timeout_ms"`
	MaxAttempts    int             `json:"max_attempts"`
	Status         Status          `json:"status"`
	Attempt        int             `json:"attempt"`
	WorkerID       *string         `json:"worker_id,omitempty"`
	GPUIDs         []string        `json:"gpu_ids,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	BatchID        *string         `json:"batch_id,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	AssignedAt     *time.Time      `json:"assigned_at,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

// Known job types. Workers reject types they can't run.
const (
	TypeSleep     = "sleep"     // payload {"ms": 200}
	TypeImage     = "image"     // payload {"width":512,"height":512,"blur_radius":2,"seed":1}
	TypeHash      = "hash"      // payload {"rounds": 200000}
	TypeInference = "inference" // payload {"model":"qwen2.5-1.5b","prompts":8,"tokens":128}
	TypeContainer = "container" // payload {"image":"alpine:3.20","cmd":["echo","hi"]}
	// Real homelab work (docs/homelab.md). Only workers configured for them advertise them.
	TypeLLM       = "llm"       // payload {"prompts":["..."],"max_tokens":64}: chat completions on a live vLLM
	TypeBenchmark = "benchmark" // payload {"suite":"vllm-sweep","label":"fp16","model":"Qwen/..."}: gpu-systems-lab on a real GPU
)

// Defaults and limits applied to every submission.
const (
	DefaultTimeoutMS   = 60_000
	MaxTimeoutMS       = 3_600_000
	DefaultMaxAttempts = 5
	MaxPriority        = 100
	MinPriority        = -100
)

// Normalize fills defaults and validates a spec in place.
func (s *Spec) Normalize() error {
	if s.Type == "" {
		return fmt.Errorf("type is required")
	}
	switch s.Type {
	case TypeSleep, TypeImage, TypeHash, TypeInference, TypeContainer, TypeLLM, TypeBenchmark:
	default:
		return fmt.Errorf("unknown job type %q", s.Type)
	}
	if len(s.Payload) == 0 {
		s.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(s.Payload) {
		return fmt.Errorf("payload is not valid JSON")
	}
	if s.Priority < MinPriority || s.Priority > MaxPriority {
		return fmt.Errorf("priority must be in [%d, %d]", MinPriority, MaxPriority)
	}
	if s.TimeoutMS == 0 {
		s.TimeoutMS = DefaultTimeoutMS
	}
	if s.TimeoutMS < 0 || s.TimeoutMS > MaxTimeoutMS {
		return fmt.Errorf("timeout_ms must be in (0, %d]", MaxTimeoutMS)
	}
	if s.MaxAttempts == 0 {
		s.MaxAttempts = DefaultMaxAttempts
	}
	if s.MaxAttempts < 1 || s.MaxAttempts > 20 {
		return fmt.Errorf("max_attempts must be in [1, 20]")
	}
	r := &s.Resources
	if r.CPUMillis == 0 {
		r.CPUMillis = 100
	}
	if r.MemoryMB == 0 {
		r.MemoryMB = 64
	}
	if r.CPUMillis < 0 || r.MemoryMB < 0 || r.GPUs < 0 || r.GPUMemoryMB < 0 {
		return fmt.Errorf("resources must not be negative")
	}
	if r.GPUs == 0 && r.GPUMemoryMB > 0 {
		return fmt.Errorf("gpu_memory_mb needs gpus > 0")
	}
	if s.Type == TypeInference && r.GPUs == 0 {
		return fmt.Errorf("inference jobs need gpus >= 1")
	}
	// A benchmark owns the card for its run. llm jobs need no GPU of their own: they are
	// requests to a vLLM server that already holds one, placed on workers that can reach it.
	if s.Type == TypeBenchmark && r.GPUs != 1 {
		return fmt.Errorf("benchmark jobs need gpus = 1")
	}
	return nil
}
