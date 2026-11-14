//! Job runners. Each matches the Go worker's runner of the same type (internal/worker/
//! runners.go): payload fields and defaults, limits, and output. The image job must give the
//! same checksum as Go's, because a retry can run on either worker.

use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::sync::CancellationToken;

/// One attempt of a job.
#[derive(Clone, Debug, Default)]
pub struct Task {
    pub job_id: String,
    pub attempt: i32,
    pub kind: String,
    pub payload: Vec<u8>,
    pub cpu_millis: i64,
    pub memory_mb: i64,
    pub gpu_ids: Vec<String>,
    pub timeout: Duration,
}

#[derive(Debug, PartialEq)]
pub enum RunError {
    /// Retrying won't help (bad payload, unknown type).
    Permanent(String),
    /// The attempt failed; the scheduler may retry it.
    Failed(String),
    /// Stopped by its cancellation token (timeout, scheduler cancel, drain).
    Cancelled,
}

pub type RunResult = Result<Value, RunError>;

#[derive(Clone)]
pub struct Runners {
    pub containers: bool,
    pub gpu_mode: String,
}

impl Runners {
    pub fn types(&self) -> Vec<String> {
        let mut t: Vec<String> = ["sleep", "image", "hash", "inference"].map(String::from).to_vec();
        if self.containers {
            t.push("container".into());
        }
        t
    }

    /// Runs a task until it finishes or `cancel` fires.
    pub async fn run(self, task: Task, cancel: CancellationToken) -> RunResult {
        match task.kind.as_str() {
            "sleep" => run_sleep(&task, &cancel).await,
            "image" => blocking(task, cancel, run_image).await,
            "hash" => blocking(task, cancel, run_hash).await,
            "inference" => run_inference_sim(&task, &cancel).await,
            "container" if self.containers => run_container(&task, &cancel, &self.gpu_mode).await,
            other => Err(RunError::Permanent(format!("worker can't run type {other}"))),
        }
    }
}

/// CPU-bound runners go on the blocking pool and poll the token as they work.
async fn blocking(task: Task, cancel: CancellationToken, f: fn(&Task, &CancellationToken) -> RunResult) -> RunResult {
    match tokio::task::spawn_blocking(move || f(&task, &cancel)).await {
        Ok(r) => r,
        Err(e) => Err(RunError::Failed(format!("panic: {e}"))),
    }
}

fn decode<T: DeserializeOwned + Default>(task: &Task) -> Result<T, RunError> {
    if task.payload.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(&task.payload).map_err(|e| RunError::Permanent(format!("bad {} payload: {e}", task.kind)))
}

async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) -> Result<(), RunError> {
    tokio::select! {
        _ = tokio::time::sleep(d) => Ok(()),
        _ = cancel.cancelled() => Err(RunError::Cancelled),
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct SleepPayload {
    ms: i64,
    fail_attempts: i64,
}

async fn run_sleep(task: &Task, cancel: &CancellationToken) -> RunResult {
    let p: SleepPayload = decode(task)?;
    if !(0..=3_600_000).contains(&p.ms) {
        return Err(RunError::Permanent("ms out of range".into()));
    }
    sleep_or_cancel(Duration::from_millis(p.ms as u64), cancel).await?;
    if i64::from(task.attempt) <= p.fail_attempts {
        return Err(RunError::Failed(format!("injected failure on attempt {}", task.attempt)));
    }
    Ok(json!({ "slept_ms": p.ms }))
}

#[derive(Deserialize)]
#[serde(default)]
struct ImagePayload {
    width: i64,
    height: i64,
    blur_radius: i64,
    seed: i64,
}

impl Default for ImagePayload {
    fn default() -> Self {
        Self { width: 256, height: 256, blur_radius: 2, seed: 1 }
    }
}

/// Fills `b` with the SplitMix64 stream for `seed`, 8 bytes little-endian per value. Must match
/// fillSplitMix64 in the Go worker.
pub fn fill_splitmix64(b: &mut [u8], seed: u64) {
    let mut state = seed;
    for chunk in b.chunks_mut(8) {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        chunk.copy_from_slice(&z.to_le_bytes()[..chunk.len()]);
    }
}

/// The image-processing job: a deterministic noise image, box blur, grayscale, checksum. The
/// loops are the same as the Go worker's, so the two can be compared fairly.
fn run_image(task: &Task, cancel: &CancellationToken) -> RunResult {
    let p: ImagePayload = decode(task)?;
    let area = p.width.checked_mul(p.height);
    if p.width < 1 || p.height < 1 || area.is_none_or(|a| a > 16_000_000) || !(0..=32).contains(&p.blur_radius) {
        return Err(RunError::Permanent("image size or blur radius out of range".into()));
    }
    let (w, h, r) = (p.width as usize, p.height as usize, p.blur_radius as isize);
    let mut src = vec![0u8; w * h * 4]; // RGBA, like Go's image.RGBA
    fill_splitmix64(&mut src, p.seed as u64);
    let mut gray = vec![0u8; w * h];
    for y in 0..h {
        if y % 64 == 0 && cancel.is_cancelled() {
            return Err(RunError::Cancelled);
        }
        for x in 0..w {
            let (mut sr, mut sg, mut sb, mut n) = (0i64, 0i64, 0i64, 0i64);
            for dy in -r..=r {
                let yy = y as isize + dy;
                if yy < 0 || yy >= h as isize {
                    continue;
                }
                for dx in -r..=r {
                    let xx = x as isize + dx;
                    if xx < 0 || xx >= w as isize {
                        continue;
                    }
                    let i = (yy as usize * w + xx as usize) * 4;
                    sr += i64::from(src[i]);
                    sg += i64::from(src[i + 1]);
                    sb += i64::from(src[i + 2]);
                    n += 1;
                }
            }
            // ITU-R BT.601 luma
            gray[y * w + x] = ((299 * sr + 587 * sg + 114 * sb) / (1000 * n)) as u8;
        }
    }
    let sum = Sha256::digest(&gray);
    Ok(json!({
        "width": p.width, "height": p.height, "blur_radius": p.blur_radius,
        "checksum": hex::encode(&sum.as_slice()[..8]),
    }))
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct HashPayload {
    rounds: Option<i64>,
    input: Option<String>,
}

fn run_hash(task: &Task, cancel: &CancellationToken) -> RunResult {
    let p: HashPayload = decode(task)?;
    let rounds = p.rounds.unwrap_or(100_000);
    let input = p.input.unwrap_or_else(|| task.job_id.clone());
    if !(1..=50_000_000).contains(&rounds) {
        return Err(RunError::Permanent("rounds out of range".into()));
    }
    let mut h = Sha256::digest(input.as_bytes());
    for i in 1..rounds {
        if i % 10_000 == 0 && cancel.is_cancelled() {
            return Err(RunError::Cancelled);
        }
        h = Sha256::digest(h.as_slice());
    }
    Ok(json!({ "rounds": rounds, "digest": hex::encode(h.as_slice()) }))
}

#[derive(Deserialize)]
#[serde(default)]
struct InferencePayload {
    model: String,
    prompts: i64,
    tokens: i64,
    ms_per_token: f64,
}

impl Default for InferencePayload {
    fn default() -> Self {
        Self { model: "qwen2.5-1.5b-instruct".into(), prompts: 8, tokens: 64, ms_per_token: 0.25 }
    }
}

/// Stands in for a batched LLM inference shard on a simulated GPU, like the Go worker's.
async fn run_inference_sim(task: &Task, cancel: &CancellationToken) -> RunResult {
    let p: InferencePayload = decode(task)?;
    if task.gpu_ids.is_empty() {
        return Err(RunError::Permanent("inference task was placed without a GPU".into()));
    }
    let total = p.prompts.checked_mul(p.tokens);
    if p.prompts < 1 || p.tokens < 1 || total.is_none_or(|t| t > 10_000_000) || p.ms_per_token < 0.0 {
        return Err(RunError::Permanent("prompts/tokens out of range".into()));
    }
    let total = total.unwrap_or(0);
    let ms = total as f64 * p.ms_per_token / task.gpu_ids.len() as f64;
    sleep_or_cancel(Duration::from_secs_f64(ms / 1000.0), cancel).await?;
    Ok(json!({
        "model": p.model, "prompts": p.prompts, "generated_tokens": total,
        "gpu_ids": task.gpu_ids, "simulated": true,
    }))
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ContainerPayload {
    image: String,
    entrypoint: String,
    cmd: Vec<String>,
}

/// The `docker run` arguments for a container job: CPU and memory as hard limits, the
/// assigned GPUs passed through on a real-GPU worker, no network.
pub fn container_args(task: &Task, image: &str, entrypoint: &str, cmd: &[String], gpu_mode: &str) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "run".into(),
        "--rm".into(),
        "--network=none".into(),
        "--name".into(),
        format!("gg-{}-{}", &task.job_id[..task.job_id.len().min(8)], task.attempt),
        "--cpus".into(),
        format!("{:.3}", task.cpu_millis as f64 / 1000.0),
        "--memory".into(),
        format!("{}m", task.memory_mb),
        "--label".into(),
        format!("gophergrid.job={}", task.job_id),
    ];
    if !task.gpu_ids.is_empty() && gpu_mode == "nvidia" {
        args.push("--gpus".into());
        args.push(format!("\"device={}\"", task.gpu_ids.join(",")));
    }
    if !entrypoint.is_empty() {
        args.push("--entrypoint".into());
        args.push(entrypoint.into());
    }
    args.push(image.into());
    args.extend(cmd.iter().cloned());
    args
}

/// Reads a stream to the end, keeping only the first `limit` bytes.
async fn read_limited(mut r: impl AsyncRead + Unpin, limit: usize) -> Vec<u8> {
    let mut kept = Vec::new();
    let mut buf = [0u8; 8192];
    while let Ok(n) = r.read(&mut buf).await {
        if n == 0 {
            break;
        }
        let room = limit.saturating_sub(kept.len());
        kept.extend_from_slice(&buf[..n.min(room)]);
    }
    kept
}

async fn run_container(task: &Task, cancel: &CancellationToken, gpu_mode: &str) -> RunResult {
    let p: ContainerPayload = decode(task)?;
    if p.image.is_empty() {
        return Err(RunError::Permanent("container payload needs image".into()));
    }
    let mut child = tokio::process::Command::new("docker")
        .args(container_args(task, &p.image, &p.entrypoint, &p.cmd, gpu_mode))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| RunError::Failed(format!("docker run: {e}")))?;
    let stdout = tokio::spawn(read_limited(child.stdout.take().expect("piped"), 64 << 10));
    let stderr = tokio::spawn(read_limited(child.stderr.take().expect("piped"), 16 << 10));
    let status = tokio::select! {
        s = child.wait() => s.map_err(|e| RunError::Failed(format!("docker run: {e}")))?,
        _ = cancel.cancelled() => {
            let _ = child.kill().await;
            return Err(RunError::Cancelled);
        }
    };
    let out = stdout.await.unwrap_or_default();
    let err = stderr.await.unwrap_or_default();
    if !status.success() {
        return Err(RunError::Failed(format!("docker run: {status}: {}", String::from_utf8_lossy(&err).trim())));
    }
    Ok(json!({ "stdout": String::from_utf8_lossy(&out) }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The same vectors are in the Go worker's TestImageChecksumMatchesSpec, and were also
    // computed by a separate Python implementation.
    const SPLITMIX_SEED1: &str = "c15c0289ec2d0a9167ec";
    const IMAGE_CHECKSUM_64X48: &str = "1682d6bdc6025b5b";

    fn task(kind: &str, payload: &str) -> Task {
        Task {
            job_id: "job-0123456789".into(),
            attempt: 1,
            kind: kind.into(),
            payload: payload.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    fn runners() -> Runners {
        Runners { containers: false, gpu_mode: "sim".into() }
    }

    #[test]
    fn splitmix64_matches_the_go_worker() {
        let mut b = [0u8; 10];
        fill_splitmix64(&mut b, 1);
        assert_eq!(hex::encode(b), SPLITMIX_SEED1);
    }

    #[test]
    fn image_checksum_matches_the_go_worker() {
        let out = run_image(
            &task("image", r#"{"width":64,"height":48,"blur_radius":2,"seed":3}"#),
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(out["checksum"], IMAGE_CHECKSUM_64X48);
        assert_eq!(out["width"], 64);
        let other = run_image(
            &task("image", r#"{"width":64,"height":48,"blur_radius":2,"seed":4}"#),
            &CancellationToken::new(),
        )
        .unwrap();
        assert_ne!(other["checksum"], out["checksum"]);
    }

    #[test]
    fn output_json_has_sorted_keys_like_go() {
        let out = run_image(&task("image", r#"{"width":8,"height":8}"#), &CancellationToken::new()).unwrap();
        let s = serde_json::to_string(&out).unwrap();
        assert!(s.starts_with(r#"{"blur_radius":2,"checksum":""#), "{s}");
    }

    #[test]
    fn hash_defaults_to_the_job_id() {
        let out = run_hash(&task("hash", r#"{"rounds":1}"#), &CancellationToken::new()).unwrap();
        assert_eq!(out["digest"], hex::encode(Sha256::digest(b"job-0123456789").as_slice()));
        let two = run_hash(&task("hash", r#"{"rounds":2,"input":"x"}"#), &CancellationToken::new()).unwrap();
        assert_eq!(two["digest"], hex::encode(Sha256::digest(Sha256::digest(b"x").as_slice()).as_slice()));
    }

    #[tokio::test]
    async fn payload_errors_are_permanent() {
        for (kind, payload) in [
            ("image", r#"{"width":-1}"#),
            ("sleep", r#"{"ms":"soon"}"#),
            ("inference", "{}"),
            ("hash", r#"{"rounds":0}"#),
            ("image", r#"{"width":9223372036854775807,"height":2}"#),
            ("sleep", "[1,2"),
        ] {
            let r = runners().run(task(kind, payload), CancellationToken::new()).await;
            assert!(matches!(r, Err(RunError::Permanent(_))), "{kind} {payload}: {r:?}");
        }
        let r = runners().run(task("container", r#"{"image":"x"}"#), CancellationToken::new()).await;
        assert_eq!(r, Err(RunError::Permanent("worker can't run type container".into())));
    }

    #[tokio::test]
    async fn sleep_fail_attempts() {
        let mut t = task("sleep", r#"{"ms":1,"fail_attempts":1}"#);
        assert!(matches!(runners().run(t.clone(), CancellationToken::new()).await, Err(RunError::Failed(_))));
        t.attempt = 2;
        assert_eq!(runners().run(t, CancellationToken::new()).await, Ok(json!({ "slept_ms": 1 })));
    }

    #[tokio::test]
    async fn cancellation_stops_sleep_and_image() {
        let c = CancellationToken::new();
        c.cancel();
        assert_eq!(runners().run(task("sleep", r#"{"ms":60000}"#), c.clone()).await, Err(RunError::Cancelled));
        assert_eq!(runners().run(task("image", r#"{"width":2000,"height":2000}"#), c).await, Err(RunError::Cancelled));
    }

    #[tokio::test]
    async fn inference_splits_time_across_gpus() {
        let mut t = task("inference", r#"{"prompts":2,"tokens":4,"ms_per_token":1}"#);
        t.gpu_ids = vec!["g0".into(), "g1".into()];
        let out = runners().run(t, CancellationToken::new()).await.unwrap();
        assert_eq!(out["generated_tokens"], 8);
        assert_eq!(out["gpu_ids"], json!(["g0", "g1"]));
        assert_eq!(out["simulated"], true);
    }

    #[test]
    fn container_args_set_limits_and_gpus() {
        let mut t = task("container", "");
        t.cpu_millis = 2500;
        t.memory_mb = 4096;
        t.gpu_ids = vec!["GPU-a".into(), "GPU-b".into()];
        let args = container_args(&t, "img:1", "python3", &["-c".into(), "print(1)".into()], "nvidia");
        let s = args.join(" ");
        assert!(s.starts_with("run --rm --network=none --name gg-job-0123-1 --cpus 2.500 --memory 4096m"), "{s}");
        assert!(s.contains(r#"--gpus "device=GPU-a,GPU-b" --entrypoint python3 img:1 -c print(1)"#), "{s}");
        assert!(!container_args(&t, "img", "", &[], "sim").contains(&"--gpus".to_string()));
    }

    /// Matches the Go worker's BenchmarkImageJob / BenchmarkHashJob:
    /// `cargo test --release -- --ignored --nocapture bench_`
    fn bench(name: &str, kind: &str, payload: &str, iters: u32) {
        let t = task(kind, payload);
        let f: fn(&Task, &CancellationToken) -> RunResult = if kind == "image" { run_image } else { run_hash };
        f(&t, &CancellationToken::new()).unwrap(); // warm up
        let start = std::time::Instant::now();
        for _ in 0..iters {
            f(&t, &CancellationToken::new()).unwrap();
        }
        let per = start.elapsed().as_secs_f64() / f64::from(iters);
        println!("{name}: {:.0} ns/op ({iters} iterations)", per * 1e9);
    }

    #[test]
    #[ignore]
    fn bench_image_job() {
        bench("bench_image_job", "image", r#"{"width":512,"height":512,"blur_radius":3,"seed":7}"#, 30);
    }

    #[test]
    #[ignore]
    fn bench_hash_job() {
        bench("bench_hash_job", "hash", r#"{"rounds":1000000}"#, 10);
    }
}
