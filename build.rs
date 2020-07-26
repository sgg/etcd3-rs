fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build etcd client stubs etcd API
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile(
            &["jetcd/jetcd-core/src/main/proto/rpc.proto"],
            &["jetcd/jetcd-core/src/main/proto"]
        )?;

    Ok(())
}
