// Generates the gRPC client (and a server, for tests) from the same proto the Go code uses.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../proto/gophergrid/v1/worker.proto";
    println!("cargo:rerun-if-changed={proto}");
    if std::env::var_os("PROTOC").is_none() {
        // A vendored protoc, so building needs no system packages.
        // SAFETY: build scripts are single-threaded.
        unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?) };
    }
    tonic_prost_build::configure().compile_protos(&[proto], &["../proto"])?;
    Ok(())
}
