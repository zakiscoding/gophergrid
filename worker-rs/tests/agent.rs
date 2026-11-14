//! Runs the real agent against a fake two-scheduler cluster: a follower that redirects to the
//! leader, and a leader that forgets the worker once (NOT_FOUND), assigns work, cancels an
//! attempt, and records completions.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use gophergrid_worker::agent::{Agent, Info, Metrics};
use gophergrid_worker::pb::worker_service_server::{WorkerService, WorkerServiceServer};
use gophergrid_worker::pb::*;
use gophergrid_worker::runners::Runners;
use gophergrid_worker::transport::{LEADER_TRAILER, Transport};
use prometheus::{IntCounterVec, IntGauge, Opts};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataMap;
use tonic::{Code, Request, Response, Status};

#[derive(Default)]
struct State {
    registers: usize,
    heartbeats: usize,
    completions: Vec<CompleteJobRequest>,
    last_running: Vec<(String, i32)>,
}

struct Leader(Arc<Mutex<State>>);

fn assignment(job: &str, kind: &str, payload: &str, cpu: i64, timeout_ms: i64) -> Assignment {
    Assignment {
        job_id: job.into(),
        attempt: 1,
        r#type: kind.into(),
        payload: payload.as_bytes().to_vec(),
        resources: Some(Resources { cpu_millis: cpu, memory_mb: 64, ..Default::default() }),
        timeout_ms,
        gpu_ids: vec![],
    }
}

#[tonic::async_trait]
impl WorkerService for Leader {
    async fn register(&self, _: Request<RegisterRequest>) -> Result<Response<RegisterResponse>, Status> {
        self.0.lock().unwrap().registers += 1;
        Ok(Response::new(RegisterResponse { heartbeat_interval_ms: 50, lease_ttl_ms: 10_000 }))
    }

    async fn heartbeat(&self, req: Request<HeartbeatRequest>) -> Result<Response<HeartbeatResponse>, Status> {
        let mut s = self.0.lock().unwrap();
        s.heartbeats += 1;
        s.last_running = req.get_ref().running.iter().map(|a| (a.job_id.clone(), a.attempt)).collect();
        let mut resp = HeartbeatResponse::default();
        match s.heartbeats {
            // A new leader that hasn't seen this worker yet.
            1 => return Err(Status::not_found("unknown worker")),
            2 => {
                resp.assignments = vec![
                    assignment(
                        "image-job",
                        "image",
                        r#"{"width":64,"height":48,"blur_radius":2,"seed":3}"#,
                        500,
                        60_000,
                    ),
                    assignment("long-job", "sleep", r#"{"ms":60000}"#, 500, 120_000),
                    assignment("too-big", "sleep", r#"{"ms":1}"#, 5_000, 60_000),
                    assignment("slow-job", "sleep", r#"{"ms":60000}"#, 100, 100),
                ]
            }
            // The scheduler takes long-job back (reassigned elsewhere).
            n if n >= 4 => resp.cancel = vec![Attempt { job_id: "long-job".into(), attempt: 1 }],
            _ => {}
        }
        Ok(Response::new(resp))
    }

    async fn complete_job(&self, req: Request<CompleteJobRequest>) -> Result<Response<CompleteJobResponse>, Status> {
        self.0.lock().unwrap().completions.push(req.into_inner());
        Ok(Response::new(CompleteJobResponse { accepted: true, reason: String::new() }))
    }
}

/// A follower: refuses heartbeats and registrations, naming the leader in a trailer.
struct Follower(String);

impl Follower {
    fn redirect(&self) -> Status {
        let mut md = MetadataMap::new();
        md.insert(LEADER_TRAILER, self.0.parse().unwrap());
        Status::with_metadata(Code::FailedPrecondition, "not the leader", md)
    }
}

#[tonic::async_trait]
impl WorkerService for Follower {
    async fn register(&self, _: Request<RegisterRequest>) -> Result<Response<RegisterResponse>, Status> {
        Err(self.redirect())
    }
    async fn heartbeat(&self, _: Request<HeartbeatRequest>) -> Result<Response<HeartbeatResponse>, Status> {
        Err(self.redirect())
    }
    async fn complete_job(&self, _: Request<CompleteJobRequest>) -> Result<Response<CompleteJobResponse>, Status> {
        Err(self.redirect())
    }
}

async fn serve<S: WorkerService>(svc: S) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(WorkerServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(l)),
    );
    addr
}

async fn wait_for(state: &Arc<Mutex<State>>, what: &str, f: impl Fn(&State) -> bool) {
    for _ in 0..200 {
        if f(&state.lock().unwrap()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn follows_the_leader_runs_work_and_honours_cancels() {
    let state = Arc::new(Mutex::new(State::default()));
    let leader = serve(Leader(state.clone())).await;
    let follower = serve(Follower(leader.clone())).await;

    let info = Info {
        id: "rust-1".into(),
        hostname: "test".into(),
        cpu_millis: 2_000,
        memory_mb: 1_024,
        gpus: vec![],
        gpu_mode: "none".into(),
        types: vec!["sleep".into(), "image".into()],
    };
    // The follower is listed first: every call must end up at the leader.
    let transport = Transport::new(vec![follower, leader], "rust-1".into(), Duration::from_secs(2));
    let metrics = Metrics {
        tasks: IntCounterVec::new(Opts::new("t", "t"), &["type", "outcome"]).unwrap(),
        running: IntGauge::new("r", "r").unwrap(),
    };
    let agent = Agent::new(
        info,
        transport,
        Runners { containers: false, gpu_mode: "none".into() },
        Duration::from_millis(50),
        true,
        metrics,
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn({
        let (agent, shutdown) = (agent.clone(), shutdown.clone());
        async move { agent.run(shutdown, Duration::from_secs(5)).await }
    });

    wait_for(&state, "image, over-capacity and timeout reports", |s| s.completions.len() >= 3).await;
    // The cancelled attempt must stop being listed, and must never be reported.
    wait_for(&state, "long-job to be dropped", |s| {
        s.heartbeats > 5 && !s.last_running.iter().any(|(j, _)| j == "long-job")
    })
    .await;
    shutdown.cancel();
    run.await.unwrap().expect("clean drain");

    let s = state.lock().unwrap();
    assert_eq!(s.registers, 2, "registered once, then again after NOT_FOUND");
    let by_job = |job: &str| s.completions.iter().find(|c| c.job_id == job);

    let image = by_job("image-job").expect("image reported");
    assert!(image.success);
    let out: serde_json::Value = serde_json::from_slice(&image.output).unwrap();
    assert_eq!(out["checksum"], "1682d6bdc6025b5b", "same checksum as the Go worker");

    let too_big = by_job("too-big").expect("over-capacity task reported");
    assert!(!too_big.success && too_big.declined && too_big.error == "worker over capacity");

    let slow = by_job("slow-job").expect("timed-out task reported");
    assert!(!slow.success && slow.error.starts_with("timed out"), "{}", slow.error);

    assert!(by_job("long-job").is_none(), "a cancelled attempt must not report");
    assert_eq!(s.completions.len(), 3);
}
