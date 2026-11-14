//! Runs a GopherGrid worker. Configured with the same environment variables as the Go worker
//! (cmd/worker), so either image can be dropped into the same compose service.

use std::time::Duration;

use axum::Router;
use axum::routing::get;
use gophergrid_worker::agent::{Agent, Info, Metrics};
use gophergrid_worker::runners::Runners;
use gophergrid_worker::transport::Transport;
use gophergrid_worker::{config, gpu};
use prometheus::{Encoder, IntCounterVec, IntGauge, IntGaugeVec, Opts, TextEncoder};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: gethostname writes at most buf.len() bytes into buf.
    if unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) } == 0 {
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        return String::from_utf8_lossy(&buf[..end]).into_owned();
    }
    "unknown".into()
}

async fn metrics() -> String {
    let mut out = Vec::new();
    let _ = TextEncoder::new().encode(&prometheus::gather(), &mut out);
    String::from_utf8(out).unwrap_or_default()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_target(false)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| "info".into()))
        .init();

    let host = hostname();
    let id = config::string("WORKER_ID", &host);
    let gpu_mode = config::string("GPU_MODE", "none");
    let mut gpus = match gpu::discover(
        &gpu_mode,
        &id,
        config::int("SIM_GPUS", 1),
        config::int("SIM_GPU_MEMORY_MB", 12288),
        &config::string("SIM_GPU_MODEL", "sim-rtx-5070"),
    )
    .await
    {
        Ok(g) => g,
        Err(e) => {
            error!(err = %e, "gpu discovery");
            std::process::exit(1);
        }
    };
    // Memory other processes hold on each GPU (e.g. vLLM sharing the card) is not offered.
    let reserved = config::int("GPU_RESERVED_MB", 0);
    if reserved > 0 {
        for g in &mut gpus {
            g.memory_mb = (g.memory_mb - reserved).max(0);
        }
    }

    let runners = Runners { containers: config::boolean("ENABLE_CONTAINERS", false), gpu_mode: gpu_mode.clone() };
    let info = Info {
        id: id.clone(),
        hostname: host,
        gpus,
        gpu_mode,
        types: runners.types(),
        cpu_millis: config::int("CPU_MILLIS", 2000),
        memory_mb: config::int("MEMORY_MB", 2048),
    };
    let transport = Transport::new(
        config::list("SCHEDULER_ADDRS", "localhost:9090"),
        id,
        config::duration("RPC_TIMEOUT", Duration::from_secs(5)),
    );

    let tasks = IntCounterVec::new(
        Opts::new("gophergrid_worker_tasks_total", "Attempts this worker finished, by type and outcome."),
        &["type", "outcome"],
    )
    .expect("metric");
    let running = IntGauge::new("gophergrid_worker_running_tasks", "Attempts running on this worker.").expect("metric");
    let build = IntGaugeVec::new(
        Opts::new("gophergrid_worker_info", "Worker implementation and version."),
        &["implementation", "version"],
    )
    .expect("metric");
    build.with_label_values(&["rust", env!("CARGO_PKG_VERSION")]).set(1);
    for m in
        [Box::new(tasks.clone()) as Box<dyn prometheus::core::Collector>, Box::new(running.clone()), Box::new(build)]
    {
        prometheus::register(m).expect("register metric");
    }

    let addr = config::listen_addr(&config::string("HTTP_ADDR", ":9100"));
    let app = Router::new().route("/metrics", get(metrics)).route("/healthz", get(|| async { "ok\n" }));
    match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => {
            tokio::spawn(async move {
                if let Err(e) = axum::serve(l, app).await {
                    error!(err = %e, "metrics server");
                }
            });
        }
        Err(e) => error!(addr, err = %e, "metrics server"),
    }

    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    tokio::spawn(async move {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        stop.cancel();
    });

    let agent = Agent::new(
        info,
        transport,
        runners,
        config::duration("HEARTBEAT_INTERVAL", Duration::from_millis(500)),
        config::boolean("EAGER_SYNC", true),
        Metrics { tasks, running },
    );
    if let Err(e) = agent.run(shutdown, config::duration("DRAIN_TIMEOUT", Duration::from_secs(30))).await {
        error!(err = %e, "worker stopped");
        std::process::exit(1);
    }
    info!("worker stopped");
}
