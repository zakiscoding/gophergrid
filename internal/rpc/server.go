// Package rpc implements the worker protocol (proto/gophergrid/v1/worker.proto).
package rpc

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/zakiscoding/gophergrid/gen/gophergridv1"
	"github.com/zakiscoding/gophergrid/internal/jobs"
	"github.com/zakiscoding/gophergrid/internal/metrics"
	"github.com/zakiscoding/gophergrid/internal/store"
)

// LeaderTrailer carries the leader's address on FAILED_PRECONDITION replies.
const LeaderTrailer = "gophergrid-leader"

// Leadership is what the server needs from the elector.
type Leadership interface {
	IsLeader() bool
	Leader() string
}

// Server is the WorkerService.
type Server struct {
	pb.UnimplementedWorkerServiceServer
	Store             *store.Store
	Leadership        Leadership
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	Log               *slog.Logger
	// Wait, if set, blocks a heartbeat that has nothing to deliver until the worker may have
	// new work or the deadline passes (long polling). Nil means reply at once.
	Wait func(ctx context.Context, workerID string, deadline time.Duration)
	// MaxWait bounds Wait; it must stay well under the heartbeat timeout.
	MaxWait time.Duration
}

func (s *Server) notLeader(ctx context.Context) error {
	leader := s.Leadership.Leader()
	_ = grpc.SetTrailer(ctx, metadata.Pairs(LeaderTrailer, leader))
	return status.Errorf(codes.FailedPrecondition, "not the leader (leader: %q)", leader)
}

// Register adds or refreshes a worker.
func (s *Server) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if !s.Leadership.IsLeader() {
		return nil, s.notLeader(ctx)
	}
	if req.WorkerId == "" || req.Capacity == nil {
		return nil, status.Error(codes.InvalidArgument, "worker_id and capacity are required")
	}
	w := store.Worker{ID: req.WorkerId, Hostname: req.Hostname, CPUMillis: req.Capacity.CpuMillis,
		MemoryMB: req.Capacity.MemoryMb, GPUMode: req.GpuMode, JobTypes: req.JobTypes}
	for _, g := range req.GpuList {
		w.GPUs = append(w.GPUs, store.GPUInfo{ID: g.Id, Model: g.Model, MemoryMB: g.MemoryMb})
	}
	if err := s.Store.UpsertWorker(ctx, w); err != nil {
		return nil, s.internal("register", err)
	}
	s.Log.Info("worker registered", "worker", w.ID, "cpu_millis", w.CPUMillis, "memory_mb", w.MemoryMB, "gpus", len(w.GPUs))
	return &pb.RegisterResponse{HeartbeatIntervalMs: s.HeartbeatInterval.Milliseconds(), LeaseTtlMs: s.LeaseTTL.Milliseconds()}, nil
}

// Heartbeat renews leases, delivers new assignments and cancels stale attempts.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if !s.Leadership.IsLeader() {
		return nil, s.notLeader(ctx)
	}
	metrics.Heartbeats.Inc()
	var free jobs.Resources
	if req.Free != nil {
		free = jobs.Resources{CPUMillis: req.Free.CpuMillis, MemoryMB: req.Free.MemoryMb, GPUs: int(req.Free.Gpus)}
	}
	ok, err := s.Store.TouchWorker(ctx, req.WorkerId, free, len(req.Running), req.CpuUtilization, req.Draining)
	if err != nil {
		return nil, s.internal("touch worker", err)
	}
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown or dead worker: register again")
	}
	held := make([]store.Held, len(req.Running))
	holding := map[store.Held]bool{}
	for i, a := range req.Running {
		held[i] = store.Held{JobID: a.JobId, Attempt: int(a.Attempt)}
		holding[held[i]] = true
	}
	if _, err := s.Store.RenewLeases(ctx, req.WorkerId, held, s.LeaseTTL); err != nil {
		return nil, s.internal("renew leases", err)
	}

	resp, err := s.reconcile(ctx, req.WorkerId, holding)
	if err != nil {
		return nil, err
	}
	if len(resp.Assignments) == 0 && len(resp.Cancel) == 0 && s.Wait != nil && !req.Draining && s.MaxWait > 0 {
		s.Wait(ctx, req.WorkerId, s.MaxWait)
		if resp, err = s.reconcile(ctx, req.WorkerId, holding); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// reconcile compares what the worker holds with what Postgres says it should hold.
func (s *Server) reconcile(ctx context.Context, workerID string, holding map[store.Held]bool) (*pb.HeartbeatResponse, error) {
	assigned, err := s.Store.HeldBy(ctx, workerID)
	if err != nil {
		return nil, s.internal("held by", err)
	}
	resp := &pb.HeartbeatResponse{}
	current := map[store.Held]bool{}
	now := time.Now()
	for _, j := range assigned {
		h := store.Held{JobID: j.ID, Attempt: j.Attempt}
		current[h] = true
		if holding[h] {
			continue
		}
		if j.StartedAt != nil {
			metrics.AssignToStart.Observe(now.Sub(*j.StartedAt).Seconds())
		}
		resp.Assignments = append(resp.Assignments, &pb.Assignment{
			JobId: j.ID, Attempt: int32(j.Attempt), Type: j.Type, Payload: j.Payload, TimeoutMs: j.TimeoutMS,
			GpuIds:    j.GPUIDs,
			Resources: &pb.Resources{CpuMillis: j.Resources.CPUMillis, MemoryMb: j.Resources.MemoryMB, Gpus: int32(j.Resources.GPUs), GpuMemoryMb: j.Resources.GPUMemoryMB},
		})
	}
	for h := range holding {
		if !current[h] {
			resp.Cancel = append(resp.Cancel, &pb.Attempt{JobId: h.JobID, Attempt: int32(h.Attempt)})
		}
	}
	return resp, nil
}

// CompleteJob records an attempt's result. Any instance may accept it.
func (s *Server) CompleteJob(ctx context.Context, req *pb.CompleteJobRequest) (*pb.CompleteJobResponse, error) {
	if req.JobId == "" || req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id and worker_id are required")
	}
	out, err := s.Store.Complete(ctx, store.Completion{JobID: req.JobId, Attempt: int(req.Attempt), WorkerID: req.WorkerId,
		Success: req.Success, Permanent: req.Permanent, Output: json.RawMessage(req.Output), Error: req.Error})
	if err != nil {
		return nil, s.internal("complete", err)
	}
	RecordOutcome(out, req.Success, time.Duration(req.DurationMs)*time.Millisecond)
	if !out.Accepted {
		s.Log.Info("stale completion rejected", "job", req.JobId, "attempt", req.Attempt, "worker", req.WorkerId, "reason", out.Reason)
	}
	return &pb.CompleteJobResponse{Accepted: out.Accepted, Reason: out.Reason}, nil
}

func (s *Server) internal(op string, err error) error {
	s.Log.Error("rpc error", "op", op, "err", err)
	return status.Error(codes.Internal, "internal error")
}

// RecordOutcome updates metrics for a completion.
func RecordOutcome(out store.Outcome, success bool, d time.Duration) {
	if !out.Accepted {
		metrics.StaleCompletions.Inc()
		return
	}
	if !success {
		metrics.TaskFailures.WithLabelValues("error").Inc()
	}
	switch out.NewStatus {
	case jobs.StatusSucceeded:
		metrics.JobsFinished.WithLabelValues(out.Type, "succeeded").Inc()
		metrics.RunTime.WithLabelValues(out.Type).Observe(d.Seconds())
	case jobs.StatusFailed:
		metrics.JobsFinished.WithLabelValues(out.Type, "failed").Inc()
	case jobs.StatusQueued:
		metrics.Requeues.WithLabelValues("error").Inc()
	}
}
