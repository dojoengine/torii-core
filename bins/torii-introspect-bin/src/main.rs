//! Torii Introspect - Dojo introspect and token indexer backed by PostgreSQL or SQLite.

mod config;

use anyhow::Result;
use clap::Parser;
use config::{Config, StorageBackend};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use starknet::core::types::Felt;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::{
    ContractEventConfig, EventExtractor, EventExtractorConfig, Extractor, RetryPolicy,
};
use torii::{EtlConcurrencyConfig, ToriiConfigBuilder};
use torii_common::{MetadataFetcher, TokenUriService};
use torii_config_common::apply_observability_env;
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::store::postgres::PgStore;
use torii_dojo::store::sqlite::SqliteStore;
use torii_ecs_sink::proto::world::world_server::WorldServer;
use torii_ecs_sink::{EcsSink, FILE_DESCRIPTOR_SET as ECS_DESCRIPTOR_SET};
use torii_erc1155::proto::erc1155_server::Erc1155Server;
use torii_erc1155::{
    Erc1155Decoder, Erc1155MetadataCommandHandler, Erc1155Service, Erc1155Sink, Erc1155Storage,
    FILE_DESCRIPTOR_SET as ERC1155_DESCRIPTOR_SET,
};
use torii_erc20::proto::erc20_server::Erc20Server;
use torii_erc20::{
    Erc20Decoder, Erc20MetadataCommandHandler, Erc20Service, Erc20Sink, Erc20Storage,
    FILE_DESCRIPTOR_SET as ERC20_DESCRIPTOR_SET,
};
use torii_erc721::proto::erc721_server::Erc721Server;
use torii_erc721::{
    Erc721Decoder, Erc721MetadataCommandHandler, Erc721Service, Erc721Sink, Erc721Storage,
    FILE_DESCRIPTOR_SET as ERC721_DESCRIPTOR_SET,
};
use torii_introspect_postgres_sink::processor::IntrospectPgDb;
use torii_introspect_postgres_sink::SchemaMode;
use torii_introspect_sqlite_sink::processor::IntrospectSqliteDb;
use torii_runtime_common::database::{resolve_token_db_setup, TokenDbSetup};
use torii_sqlite::{is_sqlite_memory_path, sqlite_connect_options};

type StarknetProvider =
    starknet::providers::jsonrpc::JsonRpcClient<starknet::providers::jsonrpc::HttpTransport>;
type ReflectionBuilder = tonic_reflection::server::Builder<'static>;

const TOKEN_COMMAND_QUEUE_SIZE: usize = 4096;
const TOKEN_METADATA_COMMAND_PARALLELISM: usize = 1;
const TOKEN_METADATA_MAX_RETRIES: u8 = 3;
const TOKEN_URI_FETCH_PARALLELISM: usize = 8;

#[derive(Clone, Debug, Default)]
struct TokenTargets {
    erc20: Vec<Felt>,
    erc721: Vec<Felt>,
    erc1155: Vec<Felt>,
}

impl TokenTargets {
    fn from_config(config: &Config) -> Result<Self> {
        Ok(Self {
            erc20: config.erc20_addresses()?,
            erc721: config.erc721_addresses()?,
            erc1155: config.erc1155_addresses()?,
        })
    }

    fn has_any(&self) -> bool {
        !self.erc20.is_empty() || !self.erc721.is_empty() || !self.erc1155.is_empty()
    }

    fn total_len(&self) -> usize {
        self.erc20.len() + self.erc721.len() + self.erc1155.len()
    }
}

#[derive(Default)]
struct TokenGrpcServices {
    erc20: Option<Erc20Service>,
    erc721: Option<Erc721Service>,
    erc1155: Option<Erc1155Service>,
}

fn append_unique_contract_configs(
    configs: &mut Vec<ContractEventConfig>,
    seen: &mut HashSet<Felt>,
    addresses: &[Felt],
    from_block: u64,
    to_block: u64,
) {
    for &address in addresses {
        if seen.insert(address) {
            configs.push(ContractEventConfig {
                address,
                from_block,
                to_block,
            });
        }
    }
}

fn add_mapping(
    mappings: &mut HashMap<Felt, Vec<DecoderId>>,
    contract: Felt,
    decoder_id: DecoderId,
) {
    let entry = mappings.entry(contract).or_default();
    if !entry.contains(&decoder_id) {
        entry.push(decoder_id);
    }
}

fn apply_contract_mappings(
    mut torii_config: ToriiConfigBuilder,
    contracts: &[Felt],
    token_targets: &TokenTargets,
) -> ToriiConfigBuilder {
    let dojo_decoder_id = DecoderId::new("dojo-introspect");
    let erc20_decoder_id = DecoderId::new("erc20");
    let erc721_decoder_id = DecoderId::new("erc721");
    let erc1155_decoder_id = DecoderId::new("erc1155");

    let mut mappings: HashMap<Felt, Vec<DecoderId>> = HashMap::new();

    for &contract in contracts {
        add_mapping(&mut mappings, contract, dojo_decoder_id);
    }
    for &contract in &token_targets.erc20 {
        add_mapping(&mut mappings, contract, erc20_decoder_id);
    }
    for &contract in &token_targets.erc721 {
        add_mapping(&mut mappings, contract, erc721_decoder_id);
    }
    for &contract in &token_targets.erc1155 {
        add_mapping(&mut mappings, contract, erc1155_decoder_id);
    }

    for (contract, decoder_ids) in mappings {
        tracing::info!(
            "Mapping contract {:#x} to decoders {:?}",
            contract,
            decoder_ids
        );
        torii_config = torii_config.map_contract(contract, decoder_ids);
    }

    torii_config
}

async fn configure_token_support(
    token_targets: &TokenTargets,
    token_db_setup: Option<&TokenDbSetup>,
    provider: Arc<StarknetProvider>,
    mut torii_config: ToriiConfigBuilder,
    mut reflection_builder: ReflectionBuilder,
) -> Result<(
    ToriiConfigBuilder,
    ReflectionBuilder,
    TokenGrpcServices,
    Vec<TokenUriService>,
)> {
    let mut services = TokenGrpcServices::default();
    let mut token_uri_services = Vec::new();

    if !token_targets.has_any() {
        return Ok((
            torii_config,
            reflection_builder,
            services,
            token_uri_services,
        ));
    }

    let db_setup =
        token_db_setup.expect("token DB setup must exist when token targets are configured");

    if !token_targets.erc20.is_empty() {
        let storage = Arc::new(Erc20Storage::new(&db_setup.erc20_url).await?);
        tracing::info!("ERC20 database initialized: {}", db_setup.erc20_url);

        let decoder: Arc<dyn torii::etl::Decoder> = Arc::new(Erc20Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        let grpc_service = Erc20Service::new(storage.clone());
        let sink = Box::new(
            Erc20Sink::new(storage.clone())
                .with_grpc_service(grpc_service.clone())
                .with_balance_tracking(provider.clone())
                .with_metadata_pipeline(
                    TOKEN_METADATA_COMMAND_PARALLELISM,
                    TOKEN_COMMAND_QUEUE_SIZE,
                    TOKEN_METADATA_MAX_RETRIES,
                ),
        );
        torii_config = torii_config
            .add_sink_boxed(sink)
            .with_command_handler(Box::new(Erc20MetadataCommandHandler::new(
                provider.clone(),
                storage,
                TOKEN_METADATA_MAX_RETRIES,
            )));
        services.erc20 = Some(grpc_service);
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC20_DESCRIPTOR_SET);
    }

    if !token_targets.erc721.is_empty() {
        let storage = Arc::new(Erc721Storage::new(&db_setup.erc721_url).await?);
        tracing::info!("ERC721 database initialized: {}", db_setup.erc721_url);

        let decoder: Arc<dyn torii::etl::Decoder> = Arc::new(Erc721Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        let grpc_service = Erc721Service::new(storage.clone());
        let (token_uri_sender, token_uri_service) = TokenUriService::spawn_with_image_cache(
            Arc::new(MetadataFetcher::new(provider.clone())),
            storage.clone(),
            TOKEN_COMMAND_QUEUE_SIZE,
            TOKEN_URI_FETCH_PARALLELISM,
            Some(Path::new("./data").join("image-cache")),
            4,
        );
        token_uri_services.push(token_uri_service);
        let sink = Box::new(
            Erc721Sink::new(storage.clone())
                .with_grpc_service(grpc_service.clone())
                .with_metadata_commands()
                .with_token_uri_sender(token_uri_sender),
        );
        torii_config = torii_config
            .add_sink_boxed(sink)
            .with_command_handler(Box::new(Erc721MetadataCommandHandler::new(
                provider.clone(),
                storage,
                TOKEN_METADATA_MAX_RETRIES,
            )));
        services.erc721 = Some(grpc_service);
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC721_DESCRIPTOR_SET);
    }

    if !token_targets.erc1155.is_empty() {
        let storage = Arc::new(Erc1155Storage::new(&db_setup.erc1155_url).await?);
        tracing::info!("ERC1155 database initialized: {}", db_setup.erc1155_url);

        let decoder: Arc<dyn torii::etl::Decoder> = Arc::new(Erc1155Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        let grpc_service = Erc1155Service::new(storage.clone());
        let (token_uri_sender, token_uri_service) = TokenUriService::spawn_with_image_cache(
            Arc::new(MetadataFetcher::new(provider.clone())),
            storage.clone(),
            TOKEN_COMMAND_QUEUE_SIZE,
            TOKEN_URI_FETCH_PARALLELISM,
            Some(Path::new("./data").join("image-cache")),
            4,
        );
        token_uri_services.push(token_uri_service);
        let sink = Box::new(
            Erc1155Sink::new(storage.clone())
                .with_grpc_service(grpc_service.clone())
                .with_balance_tracking(provider.clone())
                .with_metadata_commands()
                .with_token_uri_sender(token_uri_sender),
        );
        torii_config = torii_config
            .add_sink_boxed(sink)
            .with_command_handler(Box::new(Erc1155MetadataCommandHandler::new(
                provider.clone(),
                storage,
            )));
        services.erc1155 = Some(grpc_service);
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC1155_DESCRIPTOR_SET);
    }

    Ok((
        torii_config,
        reflection_builder,
        services,
        token_uri_services,
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    let config = Config::parse();
    run_indexer(config).await
}

async fn run_indexer(config: Config) -> Result<()> {
    tracing::info!("Starting Torii Introspect Indexer");

    apply_observability_env(config.observability);

    let db_dir = Path::new(&config.db_dir);
    let storage_database_url = config.storage_database_url(db_dir)?;
    let engine_database_url = config.engine_database_url(db_dir);
    let contracts = config.contract_addresses()?;
    let token_targets = TokenTargets::from_config(&config)?;
    let token_db_setup = if token_targets.has_any() {
        Some(resolve_token_db_setup(
            db_dir,
            config.database_url.as_deref(),
            config.storage_database_url.as_deref(),
        )?)
    } else {
        None
    };
    let backend = config.storage_backend();

    tracing::info!("RPC URL: {}", config.rpc_url);
    tracing::info!("Dojo contracts: {}", contracts.len());
    tracing::info!("ERC20 contracts: {}", token_targets.erc20.len());
    tracing::info!("ERC721 contracts: {}", token_targets.erc721.len());
    tracing::info!("ERC1155 contracts: {}", token_targets.erc1155.len());
    tracing::info!(
        "Total configured targets: {}",
        contracts.len() + token_targets.total_len()
    );
    tracing::info!("From block: {}", config.from_block);
    if let Some(to_block) = config.to_block {
        tracing::info!("To block: {}", to_block);
    } else {
        tracing::info!("To block: following chain head");
    }
    tracing::info!("Storage backend: {:?}", backend);
    tracing::info!("Engine database URL: {}", engine_database_url);
    tracing::info!("Storage database URL: {}", storage_database_url);
    if let Some(db_setup) = &token_db_setup {
        tracing::info!("ERC20 storage database URL: {}", db_setup.erc20_url);
        tracing::info!("ERC721 storage database URL: {}", db_setup.erc721_url);
        tracing::info!("ERC1155 storage database URL: {}", db_setup.erc1155_url);
    }
    tracing::info!("Database backend: {:?}", backend);
    tracing::info!(
        "ETL concurrency: prefetch_batches={} rpc_parallelism={}",
        config.max_prefetch_batches,
        config.rpc_parallelism,
    );
    tracing::info!(
        "Saved state handling: {}",
        if config.ignore_saved_state {
            "ignoring persisted extractor state"
        } else {
            "resuming from persisted extractor state when available"
        }
    );
    tracing::info!(
        "Observability: {}",
        if config.observability {
            "enabled"
        } else {
            "disabled"
        }
    );

    let provider = starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(&config.rpc_url).expect("Invalid RPC URL"),
        ),
    );
    let to_block = config.to_block.unwrap_or(u64::MAX);
    let mut extractor_contracts = Vec::new();
    let mut seen_contracts = HashSet::new();
    append_unique_contract_configs(
        &mut extractor_contracts,
        &mut seen_contracts,
        &contracts,
        config.from_block,
        to_block,
    );
    append_unique_contract_configs(
        &mut extractor_contracts,
        &mut seen_contracts,
        &token_targets.erc20,
        config.from_block,
        to_block,
    );
    append_unique_contract_configs(
        &mut extractor_contracts,
        &mut seen_contracts,
        &token_targets.erc721,
        config.from_block,
        to_block,
    );
    append_unique_contract_configs(
        &mut extractor_contracts,
        &mut seen_contracts,
        &token_targets.erc1155,
        config.from_block,
        to_block,
    );

    let extractor_provider = Arc::new(provider.clone());
    let extractor = Box::new(EventExtractor::new(
        extractor_provider,
        EventExtractorConfig {
            contracts: extractor_contracts,
            chunk_size: config.event_chunk_size,
            block_batch_size: config.event_block_batch_size,
            retry_policy: RetryPolicy::default(),
            ignore_saved_state: config.ignore_saved_state,
            rpc_parallelism: config.rpc_parallelism,
        },
    ));

    if matches!(backend, StorageBackend::Sqlite) {
        tokio::fs::create_dir_all(db_dir).await?;
    }

    match backend {
        StorageBackend::Postgres => {
            run_with_postgres(
                &config,
                &storage_database_url,
                engine_database_url,
                contracts,
                token_targets,
                token_db_setup,
                provider,
                extractor,
            )
            .await?;
        }
        StorageBackend::Sqlite => {
            run_with_sqlite(
                &config,
                &storage_database_url,
                engine_database_url,
                contracts,
                token_targets,
                token_db_setup,
                provider,
                extractor,
            )
            .await?;
        }
    }

    tracing::info!("Torii shutdown complete");
    Ok(())
}

async fn run_with_postgres(
    config: &Config,
    storage_database_url: &str,
    engine_database_url: String,
    contracts: Vec<Felt>,
    token_targets: TokenTargets,
    token_db_setup: Option<TokenDbSetup>,
    provider: StarknetProvider,
    extractor: Box<dyn Extractor>,
) -> Result<()> {
    let token_provider = Arc::new(provider.clone());
    let max_db_connections = config.max_db_connections.unwrap_or(5);
    let pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(max_db_connections)
            .connect(storage_database_url)
            .await?,
    );

    let decoder = DojoDecoder::<PgStore<_>, _>::new(pool.clone(), provider);
    let sink = IntrospectPgDb::new(pool.clone(), SchemaMode::Address);
    decoder.store.initialize().await?;
    sink.initialize_introspect_pg_sink().await?;
    decoder.load_tables(&[]).await?;

    let decoder: Arc<dyn torii::etl::Decoder> = Arc::new(decoder);

    let ecs_sink = EcsSink::new(storage_database_url, config.max_db_connections).await?;
    let ecs_grpc_service = ecs_sink.get_grpc_service_impl();

    let reflection_builder = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(ECS_DESCRIPTOR_SET);

    let torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .command_bus_queue_size(TOKEN_COMMAND_QUEUE_SIZE)
        .with_custom_reflection(true)
        .etl_concurrency(EtlConcurrencyConfig {
            max_prefetch_batches: config.max_prefetch_batches,
        })
        .engine_database_url(engine_database_url)
        .with_extractor(extractor)
        .add_decoder(decoder)
        .add_sink_boxed(Box::new(ecs_sink))
        .add_sink_boxed(Box::new(sink));

    let (torii_config, reflection_builder, token_services, _token_uri_services) =
        configure_token_support(
            &token_targets,
            token_db_setup.as_ref(),
            token_provider,
            torii_config,
            reflection_builder,
        )
        .await?;

    let reflection = reflection_builder
        .build_v1()
        .expect("failed to build reflection service");

    let grpc_builder = tonic::transport::Server::builder()
        .accept_http1(true)
        .add_service(tonic_web::enable(WorldServer::new(
            (*ecs_grpc_service).clone(),
        )));
    let grpc_router = match (
        token_services.erc20,
        token_services.erc721,
        token_services.erc1155,
    ) {
        (Some(erc20), Some(erc721), Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(tonic_web::enable(reflection)),
        (Some(erc20), Some(erc721), None) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(tonic_web::enable(reflection)),
        (Some(erc20), None, Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(tonic_web::enable(reflection)),
        (None, Some(erc721), Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(tonic_web::enable(reflection)),
        (Some(erc20), None, None) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(reflection)),
        (None, Some(erc721), None) => grpc_builder
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(tonic_web::enable(reflection)),
        (None, None, Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(tonic_web::enable(reflection)),
        (None, None, None) => grpc_builder.add_service(tonic_web::enable(reflection)),
    };

    let torii_config = apply_contract_mappings(
        torii_config.with_grpc_router(grpc_router),
        &contracts,
        &token_targets,
    );

    tracing::info!("Torii configured, starting ETL pipeline...");
    tracing::info!("gRPC service available on port {}", config.port);
    tracing::info!("  - torii.Torii (core subscriptions and metrics endpoint)");
    tracing::info!("  - world.World (legacy ECS gRPC service)");
    if !token_targets.erc20.is_empty() {
        tracing::info!("  - torii.sinks.erc20.Erc20");
    }
    if !token_targets.erc721.is_empty() {
        tracing::info!("  - torii.sinks.erc721.Erc721");
    }
    if !token_targets.erc1155.is_empty() {
        tracing::info!("  - torii.sinks.erc1155.Erc1155");
    }

    torii::run(torii_config.build())
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;
    Ok(())
}

async fn run_with_sqlite(
    config: &Config,
    storage_database_url: &str,
    engine_database_url: String,
    contracts: Vec<Felt>,
    token_targets: TokenTargets,
    token_db_setup: Option<TokenDbSetup>,
    provider: StarknetProvider,
    extractor: Box<dyn Extractor>,
) -> Result<()> {
    let token_provider = Arc::new(provider.clone());
    let options = sqlite_connect_options(storage_database_url)?;
    let max_db_connections = match config.max_db_connections {
        Some(limit) => limit.max(1),
        None if is_sqlite_memory_path(storage_database_url) => 1,
        None => 1,
    };
    let pool = Arc::new(
        SqlitePoolOptions::new()
            .max_connections(max_db_connections)
            .connect_with(options)
            .await?,
    );

    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(pool.as_ref())
        .await?;
    sqlx::query("PRAGMA synchronous=NORMAL")
        .execute(pool.as_ref())
        .await?;
    sqlx::query("PRAGMA foreign_keys=ON")
        .execute(pool.as_ref())
        .await?;

    let decoder = DojoDecoder::<SqliteStore<_>, _>::new(pool.clone(), provider);
    decoder.store.initialize().await?;
    decoder.load_tables(&[]).await?;

    let decoder: Arc<dyn torii::etl::Decoder> = Arc::new(decoder);

    let ecs_sink = EcsSink::new(storage_database_url, config.max_db_connections).await?;
    let ecs_grpc_service = ecs_sink.get_grpc_service_impl();

    let reflection_builder = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(ECS_DESCRIPTOR_SET);

    let torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .command_bus_queue_size(TOKEN_COMMAND_QUEUE_SIZE)
        .with_custom_reflection(true)
        .etl_concurrency(EtlConcurrencyConfig {
            max_prefetch_batches: config.max_prefetch_batches,
        })
        .engine_database_url(engine_database_url)
        .with_extractor(extractor)
        .add_decoder(decoder)
        .add_sink_boxed(Box::new(ecs_sink))
        .add_sink_boxed(Box::new(IntrospectSqliteDb::new(pool.clone(), ())));

    let (torii_config, reflection_builder, token_services, _token_uri_services) =
        configure_token_support(
            &token_targets,
            token_db_setup.as_ref(),
            token_provider,
            torii_config,
            reflection_builder,
        )
        .await?;

    let reflection = reflection_builder
        .build_v1()
        .expect("failed to build reflection service");

    let grpc_builder = tonic::transport::Server::builder()
        .accept_http1(true)
        .add_service(tonic_web::enable(WorldServer::new(
            (*ecs_grpc_service).clone(),
        )));
    let grpc_router = match (
        token_services.erc20,
        token_services.erc721,
        token_services.erc1155,
    ) {
        (Some(erc20), Some(erc721), Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(tonic_web::enable(reflection)),
        (Some(erc20), Some(erc721), None) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(tonic_web::enable(reflection)),
        (Some(erc20), None, Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(tonic_web::enable(reflection)),
        (None, Some(erc721), Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(tonic_web::enable(reflection)),
        (Some(erc20), None, None) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(reflection)),
        (None, Some(erc721), None) => grpc_builder
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(tonic_web::enable(reflection)),
        (None, None, Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(tonic_web::enable(reflection)),
        (None, None, None) => grpc_builder.add_service(tonic_web::enable(reflection)),
    };

    let torii_config = apply_contract_mappings(
        torii_config.with_grpc_router(grpc_router),
        &contracts,
        &token_targets,
    );

    tracing::info!("Torii configured, starting ETL pipeline...");
    tracing::info!("gRPC service available on port {}", config.port);
    tracing::info!("  - torii.Torii (core subscriptions and metrics endpoint)");
    tracing::info!("  - world.World (legacy ECS gRPC service)");
    if !token_targets.erc20.is_empty() {
        tracing::info!("  - torii.sinks.erc20.Erc20");
    }
    if !token_targets.erc721.is_empty() {
        tracing::info!("  - torii.sinks.erc721.Erc721");
    }
    if !token_targets.erc1155.is_empty() {
        tracing::info!("  - torii.sinks.erc1155.Erc1155");
    }

    torii::run(torii_config.build())
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;
    Ok(())
}
