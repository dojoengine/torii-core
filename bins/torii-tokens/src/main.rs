//! Torii Tokens - Unified Starknet Token Indexer
//!
//! A production-ready indexer for ERC20, ERC721, and ERC1155 tokens on Starknet.
//!
//! # Features
//!
//! - Explicit contract mapping with token type specification
//! - ERC20: Transfer and approval tracking
//! - ERC721: NFT ownership tracking with transfer history
//! - ERC1155: Semi-fungible token transfer tracking (single and batch)
//! - SQLite persistence with separate tables per token type
//! - gRPC subscriptions for real-time updates
//! - Historical queries with pagination
//!
//! # Usage
//!
//! ```bash
//! # Index ETH and STRK as ERC20
//! torii-tokens run --include-well-known --from-block 100000
//!
//! # Index specific ERC20 contracts
//! torii-tokens run --erc20 0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7,0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D
//!
//! # Index multiple token types
//! torii-tokens run \
//!   --erc20 0x...eth,0x...strk \
//!   --erc721 0x...nft_contract \
//!   --erc1155 0x...game_items \
//!   --from-block 100000
//!
//! # Backfill historical data for specific contracts
//! torii-tokens backfill \
//!   --erc20 0x...eth:100000 \
//!   --to-block 500000
//! ```

mod backfill;
mod config;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::Config;
use std::path::Path;
use std::sync::Arc;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::{BlockRangeConfig, BlockRangeExtractor};

// Import from ERC20 library crate
use torii_erc20::proto::erc20_server::Erc20Server;
use torii_erc20::{Erc20Decoder, Erc20Service, Erc20Sink, Erc20Storage, FILE_DESCRIPTOR_SET as ERC20_DESCRIPTOR_SET};

// Import from ERC721 library crate
use torii_erc721::proto::erc721_server::Erc721Server;
use torii_erc721::{Erc721Decoder, Erc721Service, Erc721Sink, Erc721Storage, FILE_DESCRIPTOR_SET as ERC721_DESCRIPTOR_SET};

// Import from ERC1155 library crate
use torii_erc1155::proto::erc1155_server::Erc1155Server;
use torii_erc1155::{Erc1155Decoder, Erc1155Service, Erc1155Sink, Erc1155Storage, FILE_DESCRIPTOR_SET as ERC1155_DESCRIPTOR_SET};

/// Root CLI with subcommands
#[derive(Parser, Debug)]
#[command(name = "torii-tokens")]
#[command(about = "Unified Starknet token indexer for ERC20, ERC721, and ERC1155")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// For backward compatibility: these args are passed to 'run' when no subcommand is given
    #[command(flatten)]
    run_args: Config,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the indexer (continuous operation)
    Run(Config),

    /// Backfill historical data for specific contracts
    Backfill(backfill::BackfillArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging with default INFO level, overridable via RUST_LOG
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    // Parse CLI arguments
    let cli = Cli::parse();

    // Dispatch to appropriate command
    match cli.command {
        Some(Commands::Backfill(args)) => {
            backfill::run_backfill(args).await
        }
        Some(Commands::Run(config)) => {
            run_indexer(config).await
        }
        None => {
            // Backward compatibility: if no subcommand, run the indexer with root args
            run_indexer(cli.run_args).await
        }
    }
}

/// Run the main indexer
async fn run_indexer(config: Config) -> Result<()> {
    // Start profiler if enabled
    #[cfg(feature = "profiling")]
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();

    tracing::info!("Starting Torii Unified Token Indexer");
    tracing::info!("RPC URL: {}", config.rpc_url);
    tracing::info!("From block: {}", config.from_block);
    tracing::info!("Batch size: {}", config.batch_size);
    tracing::info!("Database: {}", config.db_path);

    // Validate configuration
    if !config.has_tokens() {
        tracing::warn!("No token contracts specified. Use --erc20, --erc721, --erc1155, or --include-well-known");
        tracing::warn!("Example: torii-tokens --include-well-known --from-block 100000");
        return Ok(());
    }

    // Create Starknet provider
    let provider = Arc::new(starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(&config.rpc_url).expect("Invalid RPC URL"),
        ),
    ));

    // Create extractor
    let extractor_config = BlockRangeConfig {
        rpc_url: config.rpc_url.clone(),
        from_block: config.from_block,
        to_block: config.to_block,
        batch_size: config.batch_size,
        retry_policy: torii::etl::extractor::RetryPolicy::default(),
    };

    let extractor = Box::new(BlockRangeExtractor::new(provider.clone(), extractor_config));
    tracing::info!("Extractor configured");

    // Build gRPC reflection with all descriptor sets
    let mut reflection_builder = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET);

    // Build Torii configuration
    let db_path = Path::new(&config.db_path);
    let database_root = db_path
        .parent()
        .unwrap_or(Path::new("."))
        .to_string_lossy();

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .database_root(database_root.as_ref())
        .with_extractor(extractor);

    // Track which token types are enabled
    let mut enabled_types: Vec<&str> = Vec::new();

    // Store gRPC services to add later
    let mut erc20_grpc_service: Option<Erc20Service> = None;
    let mut erc721_grpc_service: Option<Erc721Service> = None;
    let mut erc1155_grpc_service: Option<Erc1155Service> = None;

    // Collect ERC20 addresses
    let mut erc20_addresses: Vec<starknet::core::types::Felt> = Vec::new();

    // Add well-known contracts if requested
    if config.include_well_known {
        for (address, name) in Config::well_known_erc20_contracts() {
            tracing::info!("Adding well-known ERC20: {} at {:#x}", name, address);
            erc20_addresses.push(address);
        }
    }

    // Add custom ERC20 contracts
    for addr_str in &config.erc20 {
        let address = Config::parse_address(addr_str)?;
        tracing::info!("Adding ERC20 contract: {:#x}", address);
        erc20_addresses.push(address);
    }

    // Configure ERC20 if any addresses
    if !erc20_addresses.is_empty() {
        enabled_types.push("ERC20");

        // Create storage with unique database file
        let erc20_db_path = format!("{}-erc20.db", config.db_path.trim_end_matches(".db"));
        let storage = Arc::new(Erc20Storage::new(&erc20_db_path)?);
        tracing::info!("ERC20 database initialized: {}", erc20_db_path);

        // Create decoder
        let decoder = Arc::new(Erc20Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        // Create gRPC service
        let grpc_service = Erc20Service::new(storage.clone());

        // Create sink with gRPC service
        let sink = Box::new(Erc20Sink::new(storage).with_grpc_service(grpc_service.clone()));
        torii_config = torii_config.add_sink_boxed(sink);

        // Store gRPC service for later
        erc20_grpc_service = Some(grpc_service);
        reflection_builder = reflection_builder.register_encoded_file_descriptor_set(ERC20_DESCRIPTOR_SET);

        // Map contracts to decoder
        let erc20_decoder_id = DecoderId::new("erc20");
        for address in &erc20_addresses {
            torii_config = torii_config.map_contract(*address, vec![erc20_decoder_id]);
        }

        tracing::info!("ERC20 configured with {} contracts", erc20_addresses.len());
    }

    // Collect ERC721 addresses
    let mut erc721_addresses: Vec<starknet::core::types::Felt> = Vec::new();
    for addr_str in &config.erc721 {
        let address = Config::parse_address(addr_str)?;
        tracing::info!("Adding ERC721 contract: {:#x}", address);
        erc721_addresses.push(address);
    }

    // Configure ERC721 if any addresses
    if !erc721_addresses.is_empty() {
        enabled_types.push("ERC721");

        // Create storage with unique database file
        let erc721_db_path = format!("{}-erc721.db", config.db_path.trim_end_matches(".db"));
        let storage = Arc::new(Erc721Storage::new(&erc721_db_path)?);
        tracing::info!("ERC721 database initialized: {}", erc721_db_path);

        // Create decoder
        let decoder = Arc::new(Erc721Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        // Create gRPC service
        let grpc_service = Erc721Service::new(storage.clone());

        // Create sink with gRPC service
        let sink = Box::new(Erc721Sink::new(storage).with_grpc_service(grpc_service.clone()));
        torii_config = torii_config.add_sink_boxed(sink);

        // Store gRPC service for later
        erc721_grpc_service = Some(grpc_service);
        reflection_builder = reflection_builder.register_encoded_file_descriptor_set(ERC721_DESCRIPTOR_SET);

        // Map contracts to decoder
        let erc721_decoder_id = DecoderId::new("erc721");
        for address in &erc721_addresses {
            torii_config = torii_config.map_contract(*address, vec![erc721_decoder_id]);
        }

        tracing::info!("ERC721 configured with {} contracts", erc721_addresses.len());
    }

    // Collect ERC1155 addresses
    let mut erc1155_addresses: Vec<starknet::core::types::Felt> = Vec::new();
    for addr_str in &config.erc1155 {
        let address = Config::parse_address(addr_str)?;
        tracing::info!("Adding ERC1155 contract: {:#x}", address);
        erc1155_addresses.push(address);
    }

    // Configure ERC1155 if any addresses
    if !erc1155_addresses.is_empty() {
        enabled_types.push("ERC1155");

        // Create storage with unique database file
        let erc1155_db_path = format!("{}-erc1155.db", config.db_path.trim_end_matches(".db"));
        let storage = Arc::new(Erc1155Storage::new(&erc1155_db_path)?);
        tracing::info!("ERC1155 database initialized: {}", erc1155_db_path);

        // Create decoder
        let decoder = Arc::new(Erc1155Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        // Create gRPC service
        let grpc_service = Erc1155Service::new(storage.clone());

        // Create sink with gRPC service
        let sink = Box::new(Erc1155Sink::new(storage).with_grpc_service(grpc_service.clone()));
        torii_config = torii_config.add_sink_boxed(sink);

        // Store gRPC service for later
        erc1155_grpc_service = Some(grpc_service);
        reflection_builder = reflection_builder.register_encoded_file_descriptor_set(ERC1155_DESCRIPTOR_SET);

        // Map contracts to decoder
        let erc1155_decoder_id = DecoderId::new("erc1155");
        for address in &erc1155_addresses {
            torii_config = torii_config.map_contract(*address, vec![erc1155_decoder_id]);
        }

        tracing::info!("ERC1155 configured with {} contracts", erc1155_addresses.len());
    }

    // Build reflection service
    let reflection = reflection_builder
        .build_v1()
        .expect("Failed to build gRPC reflection service");

    // Build gRPC router with all services
    // Start with Server builder
    let mut grpc_builder = tonic::transport::Server::builder();

    // Add services based on what's enabled
    // We need to build this in a specific order since add_service returns Router
    let grpc_router = match (erc20_grpc_service, erc721_grpc_service, erc1155_grpc_service) {
        (Some(erc20), Some(erc721), Some(erc1155)) => {
            grpc_builder
                .add_service(Erc20Server::new(erc20))
                .add_service(Erc721Server::new(erc721))
                .add_service(Erc1155Server::new(erc1155))
                .add_service(reflection)
        }
        (Some(erc20), Some(erc721), None) => {
            grpc_builder
                .add_service(Erc20Server::new(erc20))
                .add_service(Erc721Server::new(erc721))
                .add_service(reflection)
        }
        (Some(erc20), None, Some(erc1155)) => {
            grpc_builder
                .add_service(Erc20Server::new(erc20))
                .add_service(Erc1155Server::new(erc1155))
                .add_service(reflection)
        }
        (None, Some(erc721), Some(erc1155)) => {
            grpc_builder
                .add_service(Erc721Server::new(erc721))
                .add_service(Erc1155Server::new(erc1155))
                .add_service(reflection)
        }
        (Some(erc20), None, None) => {
            grpc_builder
                .add_service(Erc20Server::new(erc20))
                .add_service(reflection)
        }
        (None, Some(erc721), None) => {
            grpc_builder
                .add_service(Erc721Server::new(erc721))
                .add_service(reflection)
        }
        (None, None, Some(erc1155)) => {
            grpc_builder
                .add_service(Erc1155Server::new(erc1155))
                .add_service(reflection)
        }
        (None, None, None) => {
            // No token services, just reflection
            grpc_builder
                .add_service(reflection)
        }
    };

    // Finalize Torii configuration
    let torii_config = torii_config
        .with_grpc_router(grpc_router)
        .with_custom_reflection(true)
        .build();

    tracing::info!("Torii configured, starting ETL pipeline...");
    tracing::info!("Enabled token types: {}", enabled_types.join(", "));
    tracing::info!("gRPC service available at localhost:{}", config.port);
    tracing::info!("  - torii.Torii (EventBus subscriptions)");

    if !erc20_addresses.is_empty() {
        tracing::info!("  - torii.sinks.erc20.Erc20 (ERC20 queries and subscriptions)");
    }
    if !erc721_addresses.is_empty() {
        tracing::info!("  - torii.sinks.erc721.Erc721 (ERC721 queries and subscriptions)");
    }
    if !erc1155_addresses.is_empty() {
        tracing::info!("  - torii.sinks.erc1155.Erc1155 (ERC1155 queries and subscriptions)");
    }

    // Run Torii (blocks until shutdown)
    torii::run(torii_config)
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {}", e))?;

    tracing::info!("Torii shutdown complete");

    // Generate flamegraph if profiling enabled
    #[cfg(feature = "profiling")]
    {
        if let Ok(report) = guard.report().build() {
            let file = std::fs::File::create("flamegraph.svg").unwrap();
            report.flamegraph(file).unwrap();
            tracing::info!("Flamegraph generated: flamegraph.svg");
        }
    }

    Ok(())
}
