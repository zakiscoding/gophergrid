// Package pullapi is the phase 1 worker protocol: workers poll the scheduler over HTTP to
// claim queued jobs, and the job's lease is its timeout plus a grace period. Phase 2 replaces
// it with gRPC heartbeats that renew leases (see internal/rpc).
package pullapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/zakiscoding/gophergrid/internal/api"
	"github.com/zakiscoding/gophergrid/internal/jobs"
	"github.com/zakiscoding/gophergrid/internal/metrics"
	"github.com/zakiscoding/gophergrid/internal/store"
)

// ClaimRequest asks for up to Max jobs that fit in Free.
type ClaimRequest struct {
	WorkerID string         `json:"worker_id"`
	Free     jobs.Resources `json:"free"`
	Max      int            `json:"max"`
}

// Task is a claimed job attempt.
type Task struct {
	JobID     string          `json:"job_id"`
	Attempt   int             `json:"attempt"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Resources jobs.Resources  `json:"resources"`
	TimeoutMS int64           `json:"timeout_ms"`
}

// CompleteRequest reports one attempt.
type CompleteRequest struct {
	WorkerID   string          `json:"worker_id"`
	JobID      string          `json:"job_id"`
	Attempt    int             `json:"attempt"`
	Success    bool            `json:"success"`
	Permanent  bool            `json:"permanent"`
	Output     json.RawMessage `json:"output,omitempty"`
	Error      string          `json:"error,omitempty"`
	DurationMS int64           `json:"duration_ms"`
}

// Server holds the handlers.
type Server struct {
	Store *store.Store
	Log   *slog.Logger
}

// Register adds the worker routes to mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/v1/claim", s.claim)
	mux.HandleFunc("POST /internal/v1/complete", s.complete)
}

func (s *Server) claim(w http.ResponseWriter, r *http.Request) {
	var req ClaimRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil || req.WorkerID == "" {
		api.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "bad claim request"})
		return
	}
	if req.Max <= 0 || req.Max > 32 {
		req.Max = 1
	}
	free := req.Free
	tasks := []Task{}
	for len(tasks) < req.Max {
		j, err := s.Store.Claim(r.Context(), req.WorkerID, free, store.LeaseGrace)
		if errors.Is(err, store.ErrNotFound) {
			break
		}
		if err != nil {
			s.Log.Error("claim", "err", err)
			break
		}
		if j.Attempt == 1 && j.AssignedAt != nil {
			metrics.QueueLatency.Observe(j.AssignedAt.Sub(j.CreatedAt).Seconds())
		}
		tasks = append(tasks, Task{JobID: j.ID, Attempt: j.Attempt, Type: j.Type, Payload: j.Payload,
			Resources: j.Resources, TimeoutMS: j.TimeoutMS})
		free.CPUMillis -= j.Resources.CPUMillis
		free.MemoryMB -= j.Resources.MemoryMB
		free.GPUs -= j.Resources.GPUs
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	var req CompleteRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&req); err != nil || req.JobID == "" {
		api.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "bad completion"})
		return
	}
	out, err := s.Store.Complete(r.Context(), store.Completion{JobID: req.JobID, Attempt: req.Attempt,
		WorkerID: req.WorkerID, Success: req.Success, Permanent: req.Permanent, Output: req.Output, Error: req.Error})
	if err != nil {
		s.Log.Error("complete", "err", err)
		api.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	RecordOutcome(out, req.Success, time.Duration(req.DurationMS)*time.Millisecond)
	api.WriteJSON(w, http.StatusOK, map[string]any{"accepted": out.Accepted, "reason": out.Reason})
}

// RecordOutcome updates metrics for a completion.
func RecordOutcome(out store.Outcome, success bool, d time.Duration) {
	typ := out.Type
	if !out.Accepted {
		metrics.StaleCompletions.Inc()
		return
	}
	if !success {
		metrics.TaskFailures.WithLabelValues("error").Inc()
	}
	switch out.NewStatus {
	case jobs.StatusSucceeded:
		metrics.JobsFinished.WithLabelValues(typ, "succeeded").Inc()
		metrics.RunTime.WithLabelValues(typ).Observe(d.Seconds())
	case jobs.StatusFailed:
		metrics.JobsFinished.WithLabelValues(typ, "failed").Inc()
	case jobs.StatusQueued:
		metrics.Requeues.WithLabelValues("error").Inc()
	}
}
