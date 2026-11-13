// Package election runs leader election among scheduler instances on etcd.
//
// Every instance campaigns. The winner runs the leader loops (assign, reap, detect dead
// workers) until its etcd session ends: a crash, a partition, or a graceful resign. Its
// epoch is the revision of its election key, which only grows, so it can fence writes in
// Postgres (see store.TakeLeadership).
package election

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Elector campaigns for leadership.
type Elector struct {
	Client     *clientv3.Client
	Prefix     string // e.g. /gophergrid/leader
	ID         string // this instance's address for workers, e.g. scheduler-1:9090
	SessionTTL int    // seconds; how long a crashed leader holds the role
	Log        *slog.Logger

	leading atomic.Bool
	leader  atomic.Value // string
}

// IsLeader reports whether this instance currently leads.
func (e *Elector) IsLeader() bool { return e.leading.Load() }

// Leader returns the last known leader address, or "".
func (e *Elector) Leader() string {
	if v, ok := e.leader.Load().(string); ok {
		return v
	}
	return ""
}

// Run campaigns until ctx ends. Each time this instance wins, lead runs with a context that
// is cancelled the moment leadership is lost. On shutdown it resigns, so another instance
// takes over at once instead of after SessionTTL.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context, epoch int64)) {
	for ctx.Err() == nil {
		if err := e.term(ctx, lead); err != nil && ctx.Err() == nil {
			e.Log.Warn("election term ended", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

func (e *Elector) term(ctx context.Context, lead func(ctx context.Context, epoch int64)) error {
	session, err := concurrency.NewSession(e.Client, concurrency.WithTTL(e.SessionTTL))
	if err != nil {
		return err
	}
	defer session.Close()
	el := concurrency.NewElection(session, e.Prefix)

	// Follow the current leader so followers can redirect workers.
	obsCtx, stopObs := context.WithCancel(ctx)
	defer stopObs()
	go func() {
		for resp := range el.Observe(obsCtx) {
			if len(resp.Kvs) > 0 {
				e.leader.Store(string(resp.Kvs[0].Value))
			}
		}
	}()

	if err := el.Campaign(ctx, e.ID); err != nil {
		return err
	}
	epoch := el.Rev()
	e.leading.Store(true)
	e.leader.Store(e.ID)
	e.Log.Info("became leader", "epoch", epoch)

	leadCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		lead(leadCtx, epoch)
	}()

	select {
	case <-session.Done():
		err = errors.New("etcd session expired")
	case <-ctx.Done():
	case <-done:
		err = errors.New("leader loop exited")
	}
	e.leading.Store(false)
	cancel()
	<-done
	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = el.Resign(rctx)
	rcancel()
	e.Log.Info("stepped down", "epoch", epoch, "reason", err)
	return err
}
