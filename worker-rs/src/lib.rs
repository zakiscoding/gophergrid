//! GopherGrid worker in Rust. It speaks the same gRPC protocol as the Go worker
//! (proto/gophergrid/v1/worker.proto) and relies on the same scheduler-side guarantees: the
//! attempt number fences stale reports, and leases expire if heartbeats stop.

pub mod agent;
pub mod config;
pub mod gpu;
pub mod runners;
pub mod transport;

#[allow(clippy::all, clippy::pedantic)]
pub mod pb {
    tonic::include_proto!("gophergrid.v1");
}
