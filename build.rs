fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Only compile core Torii protobuf
    // Sink-specific protobufs are compiled in their own crates (torii-sql-sink, torii-log-sink, etc.)
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path("target/descriptor.bin")
        .compile_protos(&["proto/torii.proto"], &["proto"])?;
    Ok(())
}
