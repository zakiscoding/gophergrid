package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/zakiscoding/gophergrid/gen/gophergridv1"
	"github.com/zakiscoding/gophergrid/internal/jobs"
)

// LeaderTrailer must match rpc.LeaderTrailer.
const LeaderTrailer = "gophergrid-leader"

// GRPCTransport talks to the scheduler cluster. It keeps one connection per address and
// follows the leader.
type GRPCTransport struct {
	Addrs    []string // all scheduler instances, host:port
	WorkerID string
	Timeout  time.Duration // per call
	DialOpts []grpc.DialOption

	mu      sync.Mutex
	current string
	conns   map[string]*grpc.ClientConn
	info    Info
}

func (t *GRPCTransport) client(addr string) (pb.WorkerServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conns == nil {
		t.conns = map[string]*grpc.ClientConn{}
	}
	if c, ok := t.conns[addr]; ok {
		return pb.NewWorkerServiceClient(c), nil
	}
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, t.DialOpts...)
	c, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	t.conns[addr] = c
	return pb.NewWorkerServiceClient(c), nil
}

// call runs f against the leader, following redirects and failing over between addresses.
func (t *GRPCTransport) call(ctx context.Context, f func(ctx context.Context, c pb.WorkerServiceClient, opts ...grpc.CallOption) error) error {
	t.mu.Lock()
	addr := t.current
	if addr == "" {
		addr = t.Addrs[0]
	}
	t.mu.Unlock()
	var last error
	tried := map[string]bool{}
	for attempt := 0; attempt < len(t.Addrs)+2; attempt++ {
		tried[addr] = true
		c, err := t.client(addr)
		if err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, t.Timeout)
		var trailer metadata.MD
		err = f(cctx, c, grpc.Trailer(&trailer))
		cancel()
		if err == nil {
			t.mu.Lock()
			t.current = addr
			t.mu.Unlock()
			return nil
		}
		last = err
		switch status.Code(err) {
		case codes.FailedPrecondition:
			if l := trailer.Get(LeaderTrailer); len(l) > 0 && l[0] != "" && l[0] != addr {
				addr = l[0]
				continue
			}
		case codes.Unavailable, codes.DeadlineExceeded:
		default:
			return err
		}
		addr = t.next(tried)
	}
	return fmt.Errorf("no leader reachable: %w", last)
}

// next returns an address not yet tried, or the first one.
func (t *GRPCTransport) next(tried map[string]bool) string {
	for _, a := range t.Addrs {
		if !tried[a] {
			return a
		}
	}
	return t.Addrs[0]
}

// Register announces the worker to the leader.
func (t *GRPCTransport) Register(ctx context.Context, info Info) error {
	t.mu.Lock()
	t.info = info
	t.mu.Unlock()
	req := &pb.RegisterRequest{WorkerId: info.ID, Hostname: info.Hostname, GpuMode: info.GPUMode, JobTypes: info.Types,
		Capacity: &pb.Resources{CpuMillis: info.Capacity.CPUMillis, MemoryMb: info.Capacity.MemoryMB}}
	for _, g := range info.GPUList {
		req.GpuList = append(req.GpuList, &pb.Gpu{Id: g.ID, Model: g.Model, MemoryMb: g.MemoryMB})
	}
	return t.call(ctx, func(ctx context.Context, c pb.WorkerServiceClient, opts ...grpc.CallOption) error {
		_, err := c.Register(ctx, req, opts...)
		return err
	})
}

// Sync sends a heartbeat. If the leader doesn't know this worker (a new leader, or the
// worker was declared dead), it registers again and retries once.
func (t *GRPCTransport) Sync(ctx context.Context, st State) (SyncResult, error) {
	req := &pb.HeartbeatRequest{WorkerId: t.WorkerID, Draining: st.Draining, FreeGpuIds: st.FreeGPUs,
		CpuUtilization: st.CPUUtil,
		Free:           &pb.Resources{CpuMillis: st.Free.CPUMillis, MemoryMb: st.Free.MemoryMB, Gpus: int32(st.Free.GPUs)}}
	for _, r := range st.Running {
		req.Running = append(req.Running, &pb.Attempt{JobId: r.JobID, Attempt: int32(r.Attempt)})
	}
	var resp *pb.HeartbeatResponse
	hb := func(ctx context.Context, c pb.WorkerServiceClient, opts ...grpc.CallOption) error {
		var err error
		resp, err = c.Heartbeat(ctx, req, opts...)
		return err
	}
	err := t.call(ctx, hb)
	if status.Code(err) == codes.NotFound {
		t.mu.Lock()
		info := t.info
		t.mu.Unlock()
		if err = t.Register(ctx, info); err == nil {
			err = t.call(ctx, hb)
		}
	}
	if err != nil {
		return SyncResult{}, err
	}
	var res SyncResult
	for _, a := range resp.Assignments {
		task := Task{JobID: a.JobId, Attempt: int(a.Attempt), Type: a.Type, Payload: a.Payload,
			GPUIDs: a.GpuIds, Timeout: time.Duration(a.TimeoutMs) * time.Millisecond}
		if a.Resources != nil {
			task.Resources = jobs.Resources{CPUMillis: a.Resources.CpuMillis, MemoryMB: a.Resources.MemoryMb,
				GPUs: int(a.Resources.Gpus), GPUMemoryMB: a.Resources.GpuMemoryMb}
		}
		res.Tasks = append(res.Tasks, task)
	}
	for _, c := range resp.Cancel {
		res.Cancel = append(res.Cancel, Running{JobID: c.JobId, Attempt: int(c.Attempt)})
	}
	return res, nil
}

// Complete reports a result.
func (t *GRPCTransport) Complete(ctx context.Context, r Result) (bool, error) {
	req := &pb.CompleteJobRequest{WorkerId: t.WorkerID, JobId: r.JobID, Attempt: int32(r.Attempt), Success: r.Success,
		Permanent: r.Permanent, Output: r.Output, Error: r.Error, DurationMs: r.Duration.Milliseconds(), Declined: r.Declined}
	var accepted bool
	err := t.call(ctx, func(ctx context.Context, c pb.WorkerServiceClient, opts ...grpc.CallOption) error {
		resp, err := c.CompleteJob(ctx, req, opts...)
		if err == nil {
			accepted = resp.Accepted
		}
		return err
	})
	return accepted, err
}

// Close closes all connections.
func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		_ = c.Close()
	}
}
