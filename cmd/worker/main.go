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
	gpus, err := worker.DiscoverGPUs(ctx, gpuMode, id, int(config.Int("SIM_GPUS", 1)),
		config.Int("SIM_GPU_MEMORY_MB", 12288), config.String("SIM_GPU_MODEL", "sim-rtx-5070"))
	if err != nil {
		log.Error("gpu discovery", "err", err)
		os.Exit(1)
	}
	// Memory other processes hold on each GPU (e.g. a vLLM server sharing the card) is not
	// offered to jobs.
	if reserved := config.Int("GPU_RESERVED_MB", 0); reserved > 0 {
		for i := range gpus {
			gpus[i].MemoryMB = max(0, gpus[i].MemoryMB-reserved)
		}
	}
	info := worker.Info{
		ID: id, Hostname: host, GPUList: gpus, GPUMode: gpuMode,
		Capacity: jobs.Resources{CPUMillis: config.Int("CPU_MILLIS", 2000), MemoryMB: config.Int("MEMORY_MB", 2048)},
	}
	runners := worker.Runners(config.Bool("ENABLE_CONTAINERS", false), gpuMode)
	// Real work, enabled per worker: `llm` needs a reachable vLLM, `benchmark` a real GPU,
	// Docker and a gpu-systems-lab checkout. The API key is read from the environment only.
	if r := worker.LLMRunner(worker.LLMConfig{
		BaseURL: config.String("LLM_URL", ""), APIKey: os.Getenv("LLM_API_KEY"),
		Model: config.String("LLM_MODEL", ""), Concurrency: int(config.Int("LLM_CONCURRENCY", 8)),
	}); r != nil {
		runners[jobs.TypeLLM] = r
	}
	if config.Bool("ENABLE_CONTAINERS", false) {
		if r := worker.BenchmarkRunner(worker.BenchConfig{
			Image: config.String("BENCH_IMAGE", ""), LabDir: config.String("BENCH_LAB_DIR", ""),
			HFCache: config.String("BENCH_HF_CACHE", ""), OutDir: config.String("BENCH_OUT_DIR", ""),
			GPUMode: gpuMode,
		}); r != nil {
			runners[jobs.TypeBenchmark] = r
		}
	}
	for t := range runners {
		info.Types = append(info.Types, t)
	}

	transport := &worker.GRPCTransport{
		Addrs:    config.List("SCHEDULER_ADDRS", "localhost:9090"),
		WorkerID: id,
		Timeout:  config.Duration("RPC_TIMEOUT", 5*time.Second),
	}
	defer transport.Close()

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
		Interval: config.Duration("HEARTBEAT_INTERVAL", 500*time.Millisecond),
		Eager:    config.Bool("EAGER_SYNC", true), Log: log}
	if err := agent.Run(ctx, config.Duration("DRAIN_TIMEOUT", 30*time.Second)); err != nil {
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
	log.Info("worker stopped")
}
