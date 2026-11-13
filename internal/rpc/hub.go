package rpc

import (
	"context"
	"sync"
	"time"
)

// Hub lets the scheduling loop wake heartbeats that are waiting for work (long polling).
type Hub struct {
	mu      sync.Mutex
	waiters map[string]chan struct{}
}

// NewHub returns an empty hub.
func NewHub() *Hub { return &Hub{waiters: map[string]chan struct{}{}} }

// Notify wakes the waiting heartbeat of workerID, if any.
func (h *Hub) Notify(workerID string) {
	h.mu.Lock()
	if ch, ok := h.waiters[workerID]; ok {
		close(ch)
		delete(h.waiters, workerID)
	}
	h.mu.Unlock()
}

// Wait blocks until workerID is notified, d passes, or ctx ends.
func (h *Hub) Wait(ctx context.Context, workerID string, d time.Duration) {
	h.mu.Lock()
	ch, ok := h.waiters[workerID]
	if !ok {
		ch = make(chan struct{})
		h.waiters[workerID] = ch
	}
	h.mu.Unlock()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
	case <-t.C:
	case <-ctx.Done():
	}
}
