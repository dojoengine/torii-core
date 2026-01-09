fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create generated directory if it doesn't exist
    std::fs::create_dir_all("src/generated")?;

    // Compile protobuf definitions with file descriptor set for gRPC reflection
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .out_dir("src/generated")
        .file_descriptor_set_path("src/generated/log_descriptor.bin")
        .compile_protos(&["proto/log.proto"], &["proto"])?;

    // Tell Cargo to rerun if proto files change
    println!("cargo:rerun-if-changed=proto/log.proto");

    Ok(())
}
