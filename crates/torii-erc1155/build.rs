fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create generated directory if it doesn't exist
    std::fs::create_dir_all("src/generated")?;

    // Compile protobuf files with file descriptor set for gRPC reflection
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .out_dir("src/generated")
        .file_descriptor_set_path("src/generated/erc1155_descriptor.bin")
        .compile_protos(&["proto/erc1155.proto"], &["proto"])?;

    // Tell cargo to rerun this build script if proto files change
    println!("cargo:rerun-if-changed=proto/erc1155.proto");

    Ok(())
}
