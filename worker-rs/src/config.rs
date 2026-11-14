//! Settings from environment variables, with the same names and defaults as the Go worker.

use std::time::Duration;

pub fn string(key: &str, default: &str) -> String {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

pub fn int(key: &str, default: i64) -> i64 {
    std::env::var(key).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default)
}

pub fn boolean(key: &str, default: bool) -> bool {
    match std::env::var(key).ok().as_deref().map(str::trim) {
        Some("1" | "true" | "TRUE" | "True" | "t" | "T") => true,
        Some("0" | "false" | "FALSE" | "False" | "f" | "F") => false,
        _ => default,
    }
}

pub fn list(key: &str, default: &str) -> Vec<String> {
    string(key, default).split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect()
}

pub fn duration(key: &str, default: Duration) -> Duration {
    std::env::var(key).ok().and_then(|v| parse_duration(&v)).unwrap_or(default)
}

/// Parses Go-style durations such as `500ms`, `30s`, `1m30s` or `1.5h`.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s == "0" {
        return Some(Duration::ZERO);
    }
    let mut total = 0f64;
    let mut rest = s;
    if rest.is_empty() {
        return None;
    }
    while !rest.is_empty() {
        let num_len = rest.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(rest.len());
        if num_len == 0 {
            return None;
        }
        let value: f64 = rest[..num_len].parse().ok()?;
        rest = &rest[num_len..];
        let unit_len = rest.find(|c: char| c.is_ascii_digit() || c == '.').unwrap_or(rest.len());
        let secs = match &rest[..unit_len] {
            "ns" => 1e-9,
            "us" | "µs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return None,
        };
        total += value * secs;
        rest = &rest[unit_len..];
    }
    Some(Duration::from_secs_f64(total))
}

/// Go's `:9100` means every interface; Rust needs an explicit address.
pub fn listen_addr(addr: &str) -> String {
    if addr.starts_with(':') { format!("0.0.0.0{addr}") } else { addr.to_string() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_go_durations() {
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("1m30s"), Some(Duration::from_secs(90)));
        assert_eq!(parse_duration("1.5h"), Some(Duration::from_secs(5400)));
        assert_eq!(parse_duration("0"), Some(Duration::ZERO));
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("10"), None);
        assert_eq!(parse_duration("5 parsecs"), None);
    }

    #[test]
    fn go_listen_addresses() {
        assert_eq!(listen_addr(":9100"), "0.0.0.0:9100");
        assert_eq!(listen_addr("127.0.0.1:9100"), "127.0.0.1:9100");
    }
}
