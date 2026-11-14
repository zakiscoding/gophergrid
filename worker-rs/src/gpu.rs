//! GPU discovery: none, simulated, or real NVIDIA GPUs from nvidia-smi.

use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Gpu {
    pub id: String,
    pub model: String,
    pub memory_mb: i64,
}

pub async fn discover(mode: &str, worker_id: &str, n: i64, memory_mb: i64, model: &str) -> Result<Vec<Gpu>, String> {
    match mode {
        "" | "none" => Ok(Vec::new()),
        // IDs are unique across workers, like real GPU UUIDs.
        "sim" => Ok((0..n.max(0))
            .map(|i| Gpu { id: format!("{worker_id}-gpu{i}"), model: model.to_string(), memory_mb })
            .collect()),
        "nvidia" => {
            let cmd = tokio::process::Command::new("nvidia-smi")
                .args(["--query-gpu=uuid,name,memory.total", "--format=csv,noheader,nounits"])
                .kill_on_drop(true)
                .output();
            let out = tokio::time::timeout(Duration::from_secs(10), cmd)
                .await
                .map_err(|_| "nvidia-smi: timed out".to_string())?
                .map_err(|e| format!("nvidia-smi: {e}"))?;
            if !out.status.success() {
                return Err(format!("nvidia-smi: {}", out.status));
            }
            parse_nvidia_smi(&String::from_utf8_lossy(&out.stdout))
        }
        other => Err(format!("unknown GPU mode {other:?} (want none, sim or nvidia)")),
    }
}

pub fn parse_nvidia_smi(out: &str) -> Result<Vec<Gpu>, String> {
    let mut gpus = Vec::new();
    for line in out.trim().lines() {
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split(',').map(str::trim).collect();
        if f.len() != 3 {
            return Err(format!("unexpected nvidia-smi line {line:?}"));
        }
        let memory_mb = f[2].parse().map_err(|e| format!("bad memory in {line:?}: {e}"))?;
        gpus.push(Gpu { id: f[0].to_string(), model: f[1].to_string(), memory_mb });
    }
    Ok(gpus)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nvidia_smi() {
        let out = "GPU-53bd28f4-0992-8fc7-850c-56f8efbe0387, NVIDIA GeForce RTX 5070, 12227\n\
                   GPU-aaaa, NVIDIA H100 80GB HBM3, 81559\n";
        let gpus = parse_nvidia_smi(out).unwrap();
        assert_eq!(gpus.len(), 2);
        assert_eq!(
            gpus[0],
            Gpu {
                id: "GPU-53bd28f4-0992-8fc7-850c-56f8efbe0387".into(),
                model: "NVIDIA GeForce RTX 5070".into(),
                memory_mb: 12227
            }
        );
        assert!(parse_nvidia_smi("GPU-x, model").is_err());
        assert!(parse_nvidia_smi("GPU-x, model, lots").is_err());
        assert!(parse_nvidia_smi("").unwrap().is_empty());
    }

    #[tokio::test]
    async fn simulated_gpus_have_unique_ids() {
        let gpus = discover("sim", "w1", 2, 12288, "sim-rtx-5070").await.unwrap();
        assert_eq!(gpus.iter().map(|g| g.id.as_str()).collect::<Vec<_>>(), ["w1-gpu0", "w1-gpu1"]);
        assert!(discover("cuda", "w1", 1, 1, "x").await.is_err());
    }
}
