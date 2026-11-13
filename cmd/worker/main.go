// Command worker runs a GopherGrid worker.
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

	"github.com/zakiscoding/gophergrid/internal/config"
	"github.com/zakiscoding/gophergrid/internal/jobs"
	"github.com/zakiscoding/gophergrid/internal/worker"
)

func main() {
	log := config.Logger("worker")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	host, _ := os.Hostname()
	id := config.String("WORKER_ID", host)
	gpuMode := config.String("GPU_MODE", "none")
	gpus, err := worker.DiscoverGPUs(ctx, gpuMode, int(config.Int("SIM_GPUS", 1)),
		config.Int("SIM_GPU_MEMORY_MB", 12288), config.String("SIM_GPU_MODEL", "sim-rtx-5070"))
	if err != nil {
		log.Error("gpu discovery", "err", err)
		os.Exit(1)
	}
	info := worker.Info{
		ID: id, Hostname: host, GPUList: gpus, GPUMode: gpuMode,
		Capacity: jobs.Resources{CPUMillis: config.Int("CPU_MILLIS", 2000), MemoryMB: config.Int("MEMORY_MB", 2048)},
	}
	runners := worker.Runners(config.Bool("ENABLE_CONTAINERS", false), gpuMode)
	for t := range runners {
		info.Types = append(info.Types, t)
	}

	transport := &worker.HTTPTransport{
		BaseURL:  config.String("SCHEDULER_URL", "http://localhost:8080"),
		WorkerID: id,
		Client:   &http.Client{Timeout: 10 * time.Second},
	}

	go func() {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.Handler())
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
		srv := &http.Server{Addr: config.String("HTTP_ADDR", ":9100"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()

	agent := &worker.Agent{Info: info, Transport: transport, Runners: runners,
		Interval: config.Duration("POLL_INTERVAL", 250*time.Millisecond), Log: log}
	if err := agent.Run(ctx, config.Duration("DRAIN_TIMEOUT", 30*time.Second)); err != nil {
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
	log.Info("worker stopped")
}
