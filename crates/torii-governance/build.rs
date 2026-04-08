fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("src/generated")?;

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .out_dir("src/generated")
        .file_descriptor_set_path("src/generated/governance_descriptor.bin")
        .compile_protos(&["proto/governance.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/governance.proto");

    Ok(())
}
