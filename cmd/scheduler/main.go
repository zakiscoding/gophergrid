// Command scheduler runs the GopherGrid control plane: the REST API, the worker protocol
// and the lease reaper.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/zakiscoding/gophergrid/internal/api"
	"github.com/zakiscoding/gophergrid/internal/config"
	"github.com/zakiscoding/gophergrid/internal/metrics"
	"github.com/zakiscoding/gophergrid/internal/pullapi"
	"github.com/zakiscoding/gophergrid/internal/store"
)

func main() {
	log := config.Logger("scheduler")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, config.String("DATABASE_URL", "postgres://gophergrid:gophergrid@localhost:5432/gophergrid?sslmode=disable"),
		int32(config.Int("DB_MAX_CONNS", 20)), config.Duration("DB_CONNECT_WAIT", 60*time.Second))
	if err != nil {
		log.Error("database", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}

	pull := &pullapi.Server{Store: st, Log: log}
	srv := &api.Server{Store: st, Log: log, Extra: func(mux *http.ServeMux) {
		pull.Register(mux)
		mux.Handle("GET /metrics", promhttp.Handler())
	}}
	httpSrv := &http.Server{Addr: config.String("HTTP_ADDR", ":8080"), Handler: srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second}

	go reapLoop(ctx, st, config.Duration("REAP_INTERVAL", time.Second), log)
	go gaugeLoop(ctx, st, 2*time.Second)

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
	}()
	log.Info("scheduler listening", "addr", httpSrv.Addr)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http", "err", err)
		os.Exit(1)
	}
}

func reapLoop(ctx context.Context, st *store.Store, every time.Duration, log interface {
	Info(string, ...any)
	Error(string, ...any)
}) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		r, err := st.ReapExpired(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("reap", "err", err)
			}
			continue
		}
		if r.Requeued+r.Failed > 0 {
			log.Info("expired leases reaped", "requeued", r.Requeued, "failed", r.Failed)
			metrics.TaskFailures.WithLabelValues("lease_expired").Add(float64(r.Requeued + r.Failed))
			metrics.Requeues.WithLabelValues("lease_expired").Add(float64(r.Requeued))
			metrics.JobsFinished.WithLabelValues("unknown", "failed").Add(float64(r.Failed))
		}
	}
}

func gaugeLoop(ctx context.Context, st *store.Store, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		var high, normal, low, running int
		err := st.Pool.QueryRow(ctx, `SELECT
			count(*) FILTER (WHERE status = 'queued' AND priority >= 50),
			count(*) FILTER (WHERE status = 'queued' AND priority >= 0 AND priority < 50),
			count(*) FILTER (WHERE status = 'queued' AND priority < 0),
			count(*) FILTER (WHERE status = 'running')
			FROM jobs WHERE status IN ('queued', 'running')`).Scan(&high, &normal, &low, &running)
		if err == nil {
			metrics.QueueDepth.WithLabelValues("high").Set(float64(high))
			metrics.QueueDepth.WithLabelValues("normal").Set(float64(normal))
			metrics.QueueDepth.WithLabelValues("low").Set(float64(low))
			metrics.RunningJobs.Set(float64(running))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
