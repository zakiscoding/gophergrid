// Command scheduler runs a GopherGrid control-plane instance: the REST API for clients, the
// gRPC worker protocol, and, while it holds leadership in etcd, the scheduling loop.
//
// Run several instances against the same Postgres and etcd. All of them serve REST and
// accept job completions; only the leader assigns work and answers heartbeats.
package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "github.com/zakiscoding/gophergrid/gen/gophergridv1"
	"github.com/zakiscoding/gophergrid/internal/api"
	"github.com/zakiscoding/gophergrid/internal/config"
	"github.com/zakiscoding/gophergrid/internal/election"
	"github.com/zakiscoding/gophergrid/internal/rpc"
	"github.com/zakiscoding/gophergrid/internal/sched"
	"github.com/zakiscoding/gophergrid/internal/store"
)

func main() {
	log := config.Logger("scheduler")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	host, _ := os.Hostname()
	advertise := config.String("ADVERTISE_ADDR", host+":9090") // what workers dial
	log = log.With("instance", advertise)

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

	cfg := sched.Config{
		ID:               advertise,
		SchedInterval:    config.Duration("SCHED_INTERVAL", 50*time.Millisecond),
		BatchSize:        int(config.Int("SCHED_BATCH", 256)),
		LeaseTTL:         config.Duration("LEASE_TTL", 10*time.Second),
		HeartbeatTimeout: config.Duration("HEARTBEAT_TIMEOUT", 3*time.Second),
		TimeoutGrace:     config.Duration("TIMEOUT_GRACE", 5*time.Second),
		Policy:           sched.Policy{KeepGPUNodesFree: config.Bool("KEEP_GPU_NODES_FREE", true)},
	}
	cfg.Defaults()
	hub := rpc.NewHub()
	scheduler := &sched.Scheduler{Store: st, Config: cfg, Log: log, Notify: hub.Notify}

	// Leadership: etcd when configured, otherwise this instance always leads (single node).
	var lead rpc.Leadership
	var leaderAddr func() string
	if endpoints := config.List("ETCD_ENDPOINTS", ""); len(endpoints) > 0 {
		cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
		if err != nil {
			log.Error("etcd", "err", err)
			os.Exit(1)
		}
		defer cli.Close()
		el := &election.Elector{Client: cli, Prefix: config.String("ETCD_PREFIX", "/gophergrid/leader"), ID: advertise,
			SessionTTL: int(config.Int("ELECTION_TTL_SECONDS", 5)), Log: log}
		go el.Run(ctx, scheduler.Lead)
		lead, leaderAddr = el, el.Leader
	} else {
		log.Warn("ETCD_ENDPOINTS not set: running as the only scheduler")
		self := &solo{addr: advertise}
		go func() {
			self.leading.Store(true)
			scheduler.Lead(ctx, time.Now().UnixNano()) // epoch must grow across restarts
			self.leading.Store(false)
		}()
		lead, leaderAddr = self, self.Leader
	}

	// gRPC for workers.
	grpcSrv := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
		grpc.MaxRecvMsgSize(8<<20),
	)
	maxWait := config.Duration("HEARTBEAT_LONG_POLL", 0)
	var wait func(context.Context, string, time.Duration)
	if maxWait > 0 {
		wait = hub.Wait
	}
	pb.RegisterWorkerServiceServer(grpcSrv, &rpc.Server{Store: st, Leadership: lead, LeaseTTL: cfg.LeaseTTL,
		HeartbeatInterval: config.Duration("HEARTBEAT_INTERVAL", 500*time.Millisecond), Log: log,
		Wait: wait, MaxWait: maxWait})
	lis, err := net.Listen("tcp", config.String("GRPC_ADDR", ":9090"))
	if err != nil {
		log.Error("grpc listen", "err", err)
		os.Exit(1)
	}
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Error("grpc", "err", err)
		}
	}()

	// REST for clients.
	srv := &api.Server{Store: st, Log: log, Extra: func(mux *http.ServeMux) {
		mux.Handle("GET /metrics", promhttp.Handler())
		mux.HandleFunc("GET /v1/workers", func(w http.ResponseWriter, r *http.Request) {
			ws, err := st.ListWorkers(r.Context())
			if err != nil {
				api.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
				return
			}
			if ws == nil {
				ws = []store.Worker{}
			}
			api.WriteJSON(w, http.StatusOK, map[string]any{"workers": ws})
		})
		mux.HandleFunc("GET /v1/leader", func(w http.ResponseWriter, r *http.Request) {
			api.WriteJSON(w, http.StatusOK, map[string]any{"leader": leaderAddr(), "self": advertise, "is_leader": lead.IsLeader()})
		})
	}}
	httpSrv := &http.Server{Addr: config.String("HTTP_ADDR", ":8080"), Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
		grpcSrv.GracefulStop()
	}()
	log.Info("scheduler listening", "http", httpSrv.Addr, "grpc", lis.Addr().String())
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http", "err", err)
		os.Exit(1)
	}
}

// solo is leadership without etcd.
type solo struct {
	addr    string
	leading atomic.Bool
}

func (s *solo) IsLeader() bool { return s.leading.Load() }
func (s *solo) Leader() string { return s.addr }
