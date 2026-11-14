//! gRPC client for the scheduler cluster. Same behavior as the Go worker's GRPCTransport:
//! one lazily connected channel per scheduler, follow the leader redirect in the
//! `gophergrid-leader` trailer, fail over between addresses, re-register on NOT_FOUND.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::pb;
use crate::pb::worker_service_client::WorkerServiceClient;

pub const LEADER_TRAILER: &str = "gophergrid-leader";

type Client = WorkerServiceClient<Channel>;

pub struct Transport {
    addrs: Vec<String>,
    worker_id: String,
    timeout: Duration,
    current: Mutex<Option<String>>,
    clients: Mutex<HashMap<String, Client>>,
    registration: Mutex<Option<pb::RegisterRequest>>,
}

impl Transport {
    pub fn new(addrs: Vec<String>, worker_id: String, timeout: Duration) -> Self {
        assert!(!addrs.is_empty(), "at least one scheduler address");
        Self {
            addrs,
            worker_id,
            timeout,
            current: Mutex::new(None),
            clients: Mutex::new(HashMap::new()),
            registration: Mutex::new(None),
        }
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    fn client(&self, addr: &str) -> Result<Client, Status> {
        let mut clients = self.clients.lock().unwrap();
        if let Some(c) = clients.get(addr) {
            return Ok(c.clone());
        }
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| Status::invalid_argument(format!("bad scheduler address {addr:?}: {e}")))?
            .connect_timeout(self.timeout)
            .tcp_nodelay(true)
            .connect_lazy();
        let c = WorkerServiceClient::new(channel);
        clients.insert(addr.to_string(), c.clone());
        Ok(c)
    }

    fn next(&self, tried: &HashSet<String>) -> String {
        self.addrs.iter().find(|a| !tried.contains(*a)).unwrap_or(&self.addrs[0]).clone()
    }

    /// Runs `f` against the leader, following redirects and failing over between addresses.
    async fn call<T, F, Fut>(&self, mut f: F) -> Result<T, Status>
    where
        F: FnMut(Client) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let mut addr = self.current.lock().unwrap().clone().unwrap_or_else(|| self.addrs[0].clone());
        let mut tried = HashSet::new();
        let mut last = Status::unavailable("no scheduler tried");
        for _ in 0..self.addrs.len() + 2 {
            tried.insert(addr.clone());
            let client = self.client(&addr)?;
            let status = match tokio::time::timeout(self.timeout, f(client)).await {
                Ok(Ok(resp)) => {
                    *self.current.lock().unwrap() = Some(addr);
                    return Ok(resp.into_inner());
                }
                Ok(Err(s)) => s,
                Err(_) => Status::deadline_exceeded(format!("no answer from {addr} in {:?}", self.timeout)),
            };
            match status.code() {
                Code::FailedPrecondition => {
                    let leader = status.metadata().get(LEADER_TRAILER).and_then(|v| v.to_str().ok()).unwrap_or("");
                    if !leader.is_empty() && leader != addr {
                        addr = leader.to_string();
                        last = status;
                        continue;
                    }
                }
                Code::Unavailable | Code::DeadlineExceeded => {}
                _ => return Err(status),
            }
            last = status;
            addr = self.next(&tried);
        }
        Err(Status::new(last.code(), format!("no leader reachable: {}", last.message())))
    }

    pub async fn register(&self, req: pb::RegisterRequest) -> Result<(), Status> {
        *self.registration.lock().unwrap() = Some(req.clone());
        self.call(|mut c| {
            let r = req.clone();
            async move { c.register(r).await }
        })
        .await
        .map(|_| ())
    }

    /// Sends a heartbeat. If the leader doesn't know this worker (a new leader, or the worker
    /// was declared dead), registers again and retries once.
    pub async fn heartbeat(&self, req: pb::HeartbeatRequest) -> Result<pb::HeartbeatResponse, Status> {
        let hb = |mut c: Client| {
            let r = req.clone();
            async move { c.heartbeat(r).await }
        };
        match self.call(hb).await {
            Err(s) if s.code() == Code::NotFound => {
                let reg = self.registration.lock().unwrap().clone();
                if let Some(reg) = reg {
                    self.register(reg).await?;
                }
                self.call(hb).await
            }
            other => other,
        }
    }

    pub async fn complete(&self, req: pb::CompleteJobRequest) -> Result<bool, Status> {
        self.call(|mut c| {
            let r = req.clone();
            async move { c.complete_job(r).await }
        })
        .await
        .map(|r| r.accepted)
    }
}
