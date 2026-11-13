// Package api is GopherGrid's REST API for clients: submit jobs and track their status.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/zakiscoding/gophergrid/internal/jobs"
	"github.com/zakiscoding/gophergrid/internal/metrics"
	"github.com/zakiscoding/gophergrid/internal/store"
)

// MaxBatch is the most jobs one batch request may submit.
const MaxBatch = 1000

// Server serves the REST API.
type Server struct {
	Store *store.Store
	Log   *slog.Logger
	// Extra lets later phases add routes (workers, leader, autoscale) without this package
	// importing them.
	Extra func(mux *http.ServeMux)
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.Store.Pool.Ping(ctx); err != nil {
			writeErr(w, http.StatusServiceUnavailable, "database unreachable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/jobs", s.submit)
	mux.HandleFunc("POST /v1/jobs/batch", s.submitBatch)
	mux.HandleFunc("GET /v1/jobs/{id}", s.get)
	mux.HandleFunc("GET /v1/jobs", s.list)
	mux.HandleFunc("GET /v1/stats", s.stats)
	if s.Extra != nil {
		s.Extra(mux)
	}
	return mux
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	var spec jobs.Spec
	if !readJSON(w, r, &spec, 1<<20) {
		return
	}
	if k := r.Header.Get("Idempotency-Key"); k != "" && spec.IdempotencyKey == "" {
		spec.IdempotencyKey = k
	}
	if err := spec.Normalize(); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	j, created, err := s.Store.Submit(r.Context(), spec)
	if err != nil {
		s.fail(w, "submit", err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
		metrics.JobsSubmitted.WithLabelValues(j.Type).Inc()
	}
	writeJSON(w, status, j)
}

func (s *Server) submitBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Jobs []jobs.Spec `json:"jobs"`
	}
	if !readJSON(w, r, &req, 32<<20) {
		return
	}
	if len(req.Jobs) == 0 || len(req.Jobs) > MaxBatch {
		writeErr(w, http.StatusBadRequest, "jobs must have 1 to 1000 entries")
		return
	}
	for i := range req.Jobs {
		if err := req.Jobs[i].Normalize(); err != nil {
			writeErr(w, http.StatusBadRequest, "jobs["+strconv.Itoa(i)+"]: "+err.Error())
			return
		}
	}
	out := make([]jobs.Job, 0, len(req.Jobs))
	for _, spec := range req.Jobs {
		j, created, err := s.Store.Submit(r.Context(), spec)
		if err != nil {
			s.fail(w, "submit batch", err)
			return
		}
		if created {
			metrics.JobsSubmitted.WithLabelValues(j.Type).Inc()
		}
		out = append(out, j)
	}
	writeJSON(w, http.StatusCreated, map[string]any{"jobs": out})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	j, err := s.Store.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		s.fail(w, "get", err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	switch jobs.Status(status) {
	case "", jobs.StatusQueued, jobs.StatusRunning, jobs.StatusSucceeded, jobs.StatusFailed:
	default:
		writeErr(w, http.StatusBadRequest, "unknown status")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	js, err := s.Store.List(r.Context(), status, limit)
	if err != nil {
		s.fail(w, "list", err)
		return
	}
	if js == nil {
		js = []jobs.Job{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": js})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.Store.Stats(r.Context())
	if err != nil {
		s.fail(w, "stats", err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) fail(w http.ResponseWriter, op string, err error) {
	s.Log.Error("api error", "op", op, "err", err)
	writeErr(w, http.StatusInternalServerError, "internal error")
}

// readJSON decodes a JSON body of at most max bytes, writing a 400 on failure.
func readJSON(w http.ResponseWriter, r *http.Request, v any, max int64) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, max))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

// WriteJSON writes v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) { writeJSON(w, status, v) }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
