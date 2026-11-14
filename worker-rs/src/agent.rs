//! The worker loop: heartbeat, start assigned tasks within capacity, report results, drain.
//! Mirrors internal/worker/agent.go.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prometheus::{IntCounterVec, IntGauge};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::gpu::Gpu;
use crate::pb;
use crate::runners::{RunError, Runners, Task};
use crate::transport::Transport;

pub struct Info {
    pub id: String,
    pub hostname: String,
    pub cpu_millis: i64,
    pub memory_mb: i64,
    pub gpus: Vec<Gpu>,
    pub gpu_mode: String,
    pub types: Vec<String>,
}

pub struct Metrics {
    pub tasks: IntCounterVec,
    pub running: IntGauge,
}

type Key = (String, i32);

#[derive(Default)]
struct Inner {
    used_cpu: i64,
    used_mem: i64,
    gpu_busy: HashSet<String>,
    // Attempts held: running, or finished and waiting for the scheduler to acknowledge.
    running: HashMap<Key, CancellationToken>,
    draining: bool,
    cpu: CpuMeter,
}

pub struct Agent {
    pub info: Info,
    pub transport: Transport,
    pub runners: Runners,
    pub interval: Duration,
    /// Heartbeat again right after receiving work or finishing a task (with the scheduler's
    /// long polling, this keeps a worker with free capacity waiting at the leader).
    pub eager: bool,
    pub metrics: Metrics,
    inner: Mutex<Inner>,
    wake: Notify,
    tracker: TaskTracker,
}

struct Report {
    job_id: String,
    attempt: i32,
    success: bool,
    permanent: bool,
    /// Refused before running (no room): the scheduler requeues without using an attempt.
    declined: bool,
    output: Vec<u8>,
    error: String,
    duration: Duration,
}

impl Agent {
    pub fn new(
        info: Info,
        transport: Transport,
        runners: Runners,
        interval: Duration,
        eager: bool,
        metrics: Metrics,
    ) -> Arc<Self> {
        Arc::new(Self {
            info,
            transport,
            runners,
            interval,
            eager,
            metrics,
            inner: Mutex::new(Inner::default()),
            wake: Notify::new(),
            tracker: TaskTracker::new(),
        })
    }

    /// Heartbeats until `shutdown` fires, then stops taking work and waits up to `drain` for
    /// running tasks to finish and report.
    pub async fn run(self: &Arc<Self>, shutdown: CancellationToken, drain: Duration) -> Result<(), String> {
        let req = self.register_request();
        retry(&shutdown, "register", || self.transport.register(req.clone())).await?;
        info!(id = %self.info.id, cpu_millis = self.info.cpu_millis, memory_mb = self.info.memory_mb,
            gpus = self.info.gpus.len(), gpu_mode = %self.info.gpu_mode, implementation = "rust", "worker registered");

        // Tasks run under this token, which outlives `shutdown` so work can finish while draining.
        let tasks = CancellationToken::new();
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + self.interval, self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let got = self.sync(&shutdown, &tasks).await;
            if got > 0 && self.eager && !shutdown.is_cancelled() {
                continue; // capacity may remain; ask again at once
            }
            tokio::select! {
                _ = shutdown.cancelled() => return self.drain(tasks, drain).await,
                _ = ticker.tick() => {}
                _ = self.wake.notified() => {}
            }
        }
    }

    fn register_request(&self) -> pb::RegisterRequest {
        pb::RegisterRequest {
            worker_id: self.info.id.clone(),
            hostname: self.info.hostname.clone(),
            capacity: Some(pb::Resources {
                cpu_millis: self.info.cpu_millis,
                memory_mb: self.info.memory_mb,
                ..Default::default()
            }),
            gpu_list: self
                .info
                .gpus
                .iter()
                .map(|g| pb::Gpu { id: g.id.clone(), model: g.model.clone(), memory_mb: g.memory_mb })
                .collect(),
            gpu_mode: self.info.gpu_mode.clone(),
            job_types: self.info.types.clone(),
        }
    }

    /// Heartbeats once and returns how many tasks it started.
    async fn sync(self: &Arc<Self>, shutdown: &CancellationToken, tasks: &CancellationToken) -> usize {
        let req = self.state();
        let resp = tokio::select! {
            r = self.transport.heartbeat(req) => r,
            _ = shutdown.cancelled() => return 0,
        };
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                warn!(err = %e.message(), code = ?e.code(), "sync failed");
                return 0;
            }
        };
        {
            let inner = self.inner.lock().unwrap();
            for c in &resp.cancel {
                if let Some(token) = inner.running.get(&(c.job_id.clone(), c.attempt)) {
                    warn!(job = %c.job_id, attempt = c.attempt, "scheduler cancelled attempt");
                    token.cancel();
                }
            }
        }
        let n = resp.assignments.len();
        for a in resp.assignments {
            let r = a.resources.unwrap_or_default();
            self.start(
                tasks,
                Task {
                    job_id: a.job_id,
                    attempt: a.attempt,
                    kind: a.r#type,
                    payload: a.payload,
                    cpu_millis: r.cpu_millis,
                    memory_mb: r.memory_mb,
                    gpu_ids: a.gpu_ids,
                    timeout: Duration::from_millis(a.timeout_ms.max(0) as u64),
                },
            );
        }
        n
    }

    fn state(&self) -> pb::HeartbeatRequest {
        let mut inner = self.inner.lock().unwrap();
        let cpu_utilization = inner.cpu.sample(self.info.cpu_millis);
        let mut req = pb::HeartbeatRequest {
            worker_id: self.info.id.clone(),
            draining: inner.draining,
            cpu_utilization,
            running: inner.running.keys().map(|(j, a)| pb::Attempt { job_id: j.clone(), attempt: *a }).collect(),
            ..Default::default()
        };
        if !inner.draining {
            req.free_gpu_ids =
                self.info.gpus.iter().filter(|g| !inner.gpu_busy.contains(&g.id)).map(|g| g.id.clone()).collect();
            req.free = Some(pb::Resources {
                cpu_millis: self.info.cpu_millis - inner.used_cpu,
                memory_mb: self.info.memory_mb - inner.used_mem,
                gpus: req.free_gpu_ids.len() as i32,
                gpu_memory_mb: 0,
            });
        } else {
            req.free = Some(pb::Resources::default());
        }
        req
    }

    /// Reserves the task's resources and runs it. The scheduler only sends tasks that fit, but
    /// the worker re-checks: a resource limit is only real if both sides enforce it.
    fn start(self: &Arc<Self>, tasks: &CancellationToken, task: Task) {
        let key: Key = (task.job_id.clone(), task.attempt);
        let token = {
            let mut inner = self.inner.lock().unwrap();
            if inner.running.contains_key(&key) {
                return;
            }
            let fits = inner.used_cpu + task.cpu_millis <= self.info.cpu_millis
                && inner.used_mem + task.memory_mb <= self.info.memory_mb
                && task
                    .gpu_ids
                    .iter()
                    .all(|id| !inner.gpu_busy.contains(id) && self.info.gpus.iter().any(|g| &g.id == id));
            if !fits {
                drop(inner);
                warn!(job = %task.job_id, "rejecting task that doesn't fit");
                let me = Arc::clone(self);
                self.tracker.spawn(async move {
                    me.report(Report {
                        job_id: task.job_id,
                        attempt: task.attempt,
                        success: false,
                        permanent: false,
                        declined: true,
                        output: Vec::new(),
                        error: "worker over capacity".into(),
                        duration: Duration::ZERO,
                    })
                    .await;
                });
                return;
            }
            inner.used_cpu += task.cpu_millis;
            inner.used_mem += task.memory_mb;
            inner.gpu_busy.extend(task.gpu_ids.iter().cloned());
            let token = tasks.child_token();
            inner.running.insert(key.clone(), token.clone());
            token
        };
        self.metrics.running.inc();

        let me = Arc::clone(self);
        self.tracker.spawn(async move {
            let started = Instant::now();
            let kind = task.kind.clone();
            let timeout = task.timeout;
            // The runner gets its own task, so a panic in it becomes an error result instead of
            // leaving this attempt's resources reserved forever.
            let mut handle = tokio::spawn(me.runners.clone().run(task.clone(), token.clone()));
            let mut timed_out = false;
            let joined = tokio::select! {
                r = &mut handle => r,
                _ = tokio::time::sleep(timeout) => {
                    timed_out = true;
                    token.cancel();
                    handle.await
                }
            };
            let result = joined.unwrap_or_else(|e| Err(RunError::Failed(format!("panic: {e}"))));
            let scheduler_cancelled = token.is_cancelled() && !timed_out;
            {
                let mut inner = me.inner.lock().unwrap();
                inner.used_cpu -= task.cpu_millis;
                inner.used_mem -= task.memory_mb;
                for id in &task.gpu_ids {
                    inner.gpu_busy.remove(id);
                }
            }
            me.metrics.running.dec();
            let mut r = Report {
                job_id: task.job_id.clone(),
                attempt: task.attempt,
                success: false,
                permanent: false,
                declined: false,
                output: Vec::new(),
                error: String::new(),
                duration: started.elapsed(),
            };
            let outcome = match result {
                Ok(v) => {
                    r.success = true;
                    r.output = serde_json::to_vec(&v).unwrap_or_default();
                    "success"
                }
                Err(RunError::Permanent(m)) => {
                    (r.permanent, r.error) = (true, m);
                    "permanent_failure"
                }
                Err(RunError::Failed(m)) => {
                    r.error = m;
                    "failure"
                }
                Err(RunError::Cancelled) if timed_out => {
                    r.error = format!("timed out after {timeout:?}");
                    "timeout"
                }
                Err(RunError::Cancelled) => {
                    r.error = "cancelled".into();
                    "cancelled"
                }
            };
            me.metrics.tasks.with_label_values(&[&kind, outcome]).inc();
            // The attempt stays in `running` until the report is acknowledged, so heartbeats keep
            // listing it and a new leader doesn't hand it out again in the meantime.
            if !scheduler_cancelled || r.success {
                me.report(r).await;
            } // else: the scheduler took the attempt back; nothing to report
            me.inner.lock().unwrap().running.remove(&key);
            if me.eager {
                me.wake.notify_one(); // capacity freed: heartbeat now, not at the next tick
            }
        });
    }

    /// Sends a result, retrying transport errors for up to 2 minutes. A rejection is final: the
    /// attempt is stale and its result is dropped by design.
    async fn report(&self, r: Report) {
        let req = pb::CompleteJobRequest {
            worker_id: self.transport.worker_id().to_string(),
            job_id: r.job_id.clone(),
            attempt: r.attempt,
            success: r.success,
            permanent: r.permanent,
            declined: r.declined,
            output: r.output,
            error: r.error,
            duration_ms: r.duration.as_millis() as i64,
        };
        let deadline = CancellationToken::new();
        let guard = deadline.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(120)).await;
            guard.cancel();
        });
        let res = retry(&deadline, "complete", || self.transport.complete(req.clone())).await;
        timer.abort();
        match res {
            Err(e) => error!(job = %r.job_id, attempt = r.attempt, err = %e,
                "could not report result; the lease will expire and the job will be retried"),
            Ok(false) => warn!(job = %r.job_id, attempt = r.attempt, "result rejected as stale"),
            Ok(true) => debug!(job = %r.job_id, attempt = r.attempt, ok = r.success, "result accepted"),
        }
    }

    async fn drain(self: &Arc<Self>, tasks: CancellationToken, timeout: Duration) -> Result<(), String> {
        let n = {
            let mut inner = self.inner.lock().unwrap();
            inner.draining = true;
            inner.running.len()
        };
        info!(running = n, timeout = ?timeout, "draining");
        // Tell the scheduler we're draining so it stops sending work.
        let _ = tokio::time::timeout(Duration::from_secs(2), self.transport.heartbeat(self.state())).await;
        self.tracker.close();
        if tokio::time::timeout(timeout, self.tracker.wait()).await.is_ok() {
            return Ok(());
        }
        tasks.cancel();
        self.tracker.wait().await;
        Err("drain timed out; unfinished jobs will be retried elsewhere".into())
    }
}

/// Retries `f` with exponential backoff (100 ms doubling to 5 s) until it succeeds or `stop`
/// fires.
async fn retry<T, F, Fut>(stop: &CancellationToken, op: &str, mut f: F) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, tonic::Status>>,
{
    let mut backoff = Duration::from_millis(100);
    loop {
        let err = tokio::select! {
            r = f() => match r {
                Ok(v) => return Ok(v),
                Err(e) => e,
            },
            _ = stop.cancelled() => return Err(format!("{op}: stopped")),
        };
        debug!(op, err = %err.message(), backoff = ?backoff, "retrying");
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = stop.cancelled() => return Err(format!("{op}: stopped (last error: {})", err.message())),
        }
        if backoff < Duration::from_secs(5) {
            backoff *= 2;
        }
    }
}

/// This process's CPU time between samples, as a fraction of the worker's capacity.
#[derive(Default)]
struct CpuMeter {
    last: Option<(Duration, Instant)>,
}

impl CpuMeter {
    fn sample(&mut self, capacity_millis: i64) -> f64 {
        let Some(cpu) = process_cpu_time() else { return 0.0 };
        let now = Instant::now();
        let prev = self.last.replace((cpu, now));
        let Some((last_cpu, last_wall)) = prev else { return 0.0 };
        let wall = now.duration_since(last_wall).as_secs_f64();
        if capacity_millis <= 0 || wall <= 0.0 {
            return 0.0;
        }
        ((cpu.saturating_sub(last_cpu)).as_secs_f64() / wall / (capacity_millis as f64 / 1000.0)).min(1.0)
    }
}

#[cfg(unix)]
fn process_cpu_time() -> Option<Duration> {
    // SAFETY: getrusage only writes into the struct we pass.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
        return None;
    }
    let tv = |t: libc::timeval| Duration::from_secs(t.tv_sec as u64) + Duration::from_micros(t.tv_usec as u64);
    Some(tv(ru.ru_utime) + tv(ru.ru_stime))
}

#[cfg(not(unix))]
fn process_cpu_time() -> Option<Duration> {
    None
}
