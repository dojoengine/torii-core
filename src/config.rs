use crate::etl::decoder::{ContractFilter, DecoderId};
use crate::etl::identification::ContractIdentifier;
use crate::etl::{
    Decoder, Extractor, IdentificationRule, Sink, SyntheticExtractor, SyntheticExtractorAdapter,
};
use std::path::PathBuf;
use std::sync::Arc;

/// Configuration for Torii server with pluggable sinks and decoders.
#[derive(Debug, Clone)]
pub struct EtlConcurrencyConfig {
    pub max_prefetch_batches: usize,
}

impl Default for EtlConcurrencyConfig {
    fn default() -> Self {
        Self {
            max_prefetch_batches: 2,
        }
    }
}

impl EtlConcurrencyConfig {
    pub fn resolved_prefetch_batches(&self) -> usize {
        self.max_prefetch_batches.max(1)
    }
}

pub struct ToriiConfig {
    /// Port to listen on.
    pub port: u16,

    /// Host to bind to.
    pub host: String,

    /// Custom sinks to register (not yet initialized).
    pub sinks: Vec<Box<dyn Sink>>,

    /// Custom decoders to register.
    pub decoders: Vec<Arc<dyn Decoder>>,

    /// Optional pre-built gRPC router with sink services.
    ///
    /// Users can add their sink gRPC services to a router and pass it here.
    /// Torii will add the core service to this router. This works around Rust's
    /// type system limitations while keeping all services on the same port.
    ///
    /// If None, Torii creates a fresh router with only core services.
    pub partial_grpc_router: Option<tonic::transport::server::Router>,

    /// Whether the user has already added reflection to the gRPC router
    ///
    /// If true, Torii will skip adding reflection services (to avoid conflicts).
    pub custom_reflection: bool,

    /// ETL cycle interval in seconds.
    pub cycle_interval: u64,

    /// Events per cycle.
    pub events_per_cycle: usize,

    /// Sample events for testing (provided by sinks).
    pub sample_events: Vec<starknet::core::types::EmittedEvent>,

    /// Extractor for fetching blockchain events.
    ///
    /// If None, a SampleExtractor will be used for testing.
    pub extractor: Option<Box<dyn Extractor>>,

    /// Root directory for all databases (engine, sinks, registry).
    ///
    /// Defaults to current directory if not specified.
    pub database_root: PathBuf,

    /// Optional explicit engine database URL/path.
    ///
    /// If set, Torii uses this value directly when creating `EngineDb`.
    /// This can be a PostgreSQL URL (`postgres://...`) or SQLite path/URL.
    pub engine_database_url: Option<String>,

    /// Contract filter (explicit mappings + blacklist).
    pub contract_filter: ContractFilter,

    /// Identification rules for auto-discovery of contract types.
    ///
    /// When a contract is encountered that's not in the explicit mappings,
    /// these rules are used to identify the contract type by inspecting its ABI.
    /// The identified mapping is then cached for future events.
    ///
    /// NOTE: Rules alone don't enable auto-identification. You must also provide
    /// a registry cache via `with_registry_cache()` for identification to work.
    pub identification_rules: Vec<Box<dyn IdentificationRule>>,

    /// Optional shared registry cache from ContractRegistry.
    ///
    /// If provided, the DecoderContext will use this cache to look up
    /// contract→decoder mappings for contracts not in explicit mappings.
    /// The cache is typically populated by a ContractRegistry running batch
    /// identification before decoding.
    pub registry_cache: Option<
        Arc<
            tokio::sync::RwLock<
                std::collections::HashMap<starknet::core::types::Felt, Vec<DecoderId>>,
            >,
        >,
    >,

    /// Optional contract identifier for runtime identification.
    ///
    /// If provided (via `with_contract_identifier()`), unknown contracts
    /// will be identified by fetching their ABIs during the ETL loop.
    /// This enables auto-discovery of token contracts without explicit mapping.
    pub contract_identifier: Option<Arc<dyn ContractIdentifier>>,

    /// Graceful shutdown timeout in seconds (default: 30).
    ///
    /// When a shutdown signal is received, the system will wait up to this
    /// duration for the current ETL batch to complete before forcing shutdown.
    pub shutdown_timeout: u64,

    /// ETL concurrency controls.
    pub etl_concurrency: EtlConcurrencyConfig,

    /// Background command handlers registered for the command bus.
    pub command_handlers: Vec<Box<dyn CommandHandler>>,

    /// Command bus queue size.
    pub command_bus_queue_size: usize,

    /// Optional TLS listener configuration.
    pub tls: Option<ToriiTlsConfig>,
}

impl ToriiConfig {
    pub fn builder() -> ToriiConfigBuilder {
        ToriiConfigBuilder::default()
    }
}

#[derive(Debug, Clone)]
pub struct ToriiTlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub alpn_protocols: Vec<Vec<u8>>,
}

impl ToriiTlsConfig {
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            alpn_protocols: vec![b"h2".to_vec(), b"http/1.1".to_vec()],
        }
    }

    pub fn with_alpn_protocols(mut self, alpn_protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = alpn_protocols;
        self
    }

    fn alpn_names(&self) -> Vec<String> {
        self.alpn_protocols
            .iter()
            .map(|protocol| String::from_utf8_lossy(protocol).into_owned())
            .collect()
    }
}

/// Builder for ToriiConfig.
#[derive(Default)]
pub struct ToriiConfigBuilder {
    port: Option<u16>,
    host: Option<String>,
    sinks: Vec<Box<dyn Sink>>,
    decoders: Vec<Arc<dyn Decoder>>,
    partial_grpc_router: Option<tonic::transport::server::Router>,
    custom_reflection: bool,
    cycle_interval: Option<u64>,
    events_per_cycle: Option<usize>,
    sample_events: Vec<starknet::core::types::EmittedEvent>,
    extractor: Option<Box<dyn Extractor>>,
    database_root: Option<PathBuf>,
    engine_database_url: Option<String>,
    contract_filter: Option<ContractFilter>,
    identification_rules: Vec<Box<dyn IdentificationRule>>,
    registry_cache: Option<
        Arc<
            tokio::sync::RwLock<
                std::collections::HashMap<starknet::core::types::Felt, Vec<DecoderId>>,
            >,
        >,
    >,
    contract_identifier: Option<Arc<dyn ContractIdentifier>>,
    shutdown_timeout: Option<u64>,
    etl_concurrency: Option<EtlConcurrencyConfig>,
    command_handlers: Vec<Box<dyn CommandHandler>>,
    command_bus_queue_size: Option<usize>,
    tls: Option<ToriiTlsConfig>,
}

impl ToriiConfigBuilder {
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn host(mut self, host: String) -> Self {
        self.host = Some(host);
        self
    }

    /// Adds a sink (which may also provide a gRPC service).
    ///
    /// Sinks should NOT be wrapped in Arc yet - they will be initialized and wrapped by Torii.
    pub fn add_sink_boxed(mut self, sink: Box<dyn Sink>) -> Self {
        self.sinks.push(sink);
        self
    }

    /// Adds multiple sinks at once.
    pub fn with_sinks_boxed(mut self, sinks: Vec<Box<dyn Sink>>) -> Self {
        self.sinks.extend(sinks);
        self
    }

    /// Adds a decoder.
    pub fn add_decoder(mut self, decoder: Arc<dyn Decoder>) -> Self {
        self.decoders.push(decoder);
        self
    }

    /// Adds multiple decoders at once.
    pub fn with_decoders(mut self, decoders: Vec<Arc<dyn Decoder>>) -> Self {
        self.decoders.extend(decoders);
        self
    }

    /// Sets a pre-built gRPC router with sink services.
    ///
    /// Users should add their sink gRPC services to a router and pass it here.
    /// Torii will add the core Torii service to this router before starting.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tonic::transport::Server;
    ///
    /// let router = Server::builder()
    ///     .accept_http1(true)
    ///     .add_service(tonic_web::enable(SqlSinkServer::new(service)));
    ///
    /// let config = ToriiConfig::builder()
    ///     .with_grpc_router(router)
    ///     .build();
    /// ```
    pub fn with_grpc_router(mut self, router: tonic::transport::server::Router) -> Self {
        self.partial_grpc_router = Some(router);
        self
    }

    /// Indicate that the gRPC router already includes reflection services
    ///
    /// Set this to true if you've already added reflection services to your
    /// gRPC router (with all sink descriptor sets). Torii will skip adding
    /// reflection to avoid route conflicts.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let reflection = tonic_reflection::server::Builder::configure()
    ///     .register_encoded_file_descriptor_set(TORII_DESCRIPTOR_SET)
    ///     .register_encoded_file_descriptor_set(SQL_SINK_DESCRIPTOR_SET)
    ///     .build_v1()?;
    ///
    /// let router = Server::builder()
    ///     .add_service(reflection);
    ///
    /// let config = ToriiConfig::builder()
    ///     .with_grpc_router(router)
    ///     .with_custom_reflection(true)  // Skip Torii's reflection
    ///     .build();
    /// ```
    pub fn with_custom_reflection(mut self, custom: bool) -> Self {
        self.custom_reflection = custom;
        self
    }

    /// Sets the ETL cycle interval in seconds.
    pub fn cycle_interval(mut self, seconds: u64) -> Self {
        self.cycle_interval = Some(seconds);
        self
    }

    /// Sets the number of events per cycle.
    pub fn events_per_cycle(mut self, count: usize) -> Self {
        self.events_per_cycle = Some(count);
        self
    }

    /// Adds sample events for testing.
    pub fn with_sample_events(mut self, events: Vec<starknet::core::types::EmittedEvent>) -> Self {
        self.sample_events.extend(events);
        self
    }

    /// Sets the extractor for fetching blockchain events.
    ///
    /// If not set, a SampleExtractor will be used for testing.
    pub fn with_extractor(mut self, extractor: Box<dyn Extractor>) -> Self {
        self.extractor = Some(extractor);
        self
    }

    /// Sets a synthetic extractor wrapped as a regular ETL extractor.
    ///
    /// This is intended for tests and local deterministic runs where the full
    /// ingestion pipeline should execute without a live provider.
    pub fn with_synthetic_extractor<T>(mut self, extractor: T) -> Self
    where
        T: SyntheticExtractor + 'static,
    {
        self.extractor = Some(Box::new(SyntheticExtractorAdapter::new(extractor)));
        self
    }

    /// Sets the root directory for all databases.
    ///
    /// All Torii databases (engine, sinks, registry) will be placed in this directory.
    /// Defaults to current directory if not specified.
    pub fn database_root(mut self, path: impl Into<PathBuf>) -> Self {
        self.database_root = Some(path.into());
        self
    }

    /// Sets an explicit engine database URL/path.
    ///
    /// Examples:
    /// - `postgres://user:pass@localhost:5432/torii`
    /// - `sqlite::memory:`
    /// - `./torii-data/engine.db`
    pub fn engine_database_url(mut self, url: impl Into<String>) -> Self {
        self.engine_database_url = Some(url.into());
        self
    }

    /// Sets the contract filter (mappings + blacklist).
    pub fn with_contract_filter(mut self, filter: ContractFilter) -> Self {
        self.contract_filter = Some(filter);
        self
    }

    /// Add explicit contract→decoder mapping (most efficient).
    ///
    /// Events from this contract will ONLY be tried with the specified decoders.
    /// This provides O(k) performance where k is the number of mapped decoders.
    pub fn map_contract(
        mut self,
        contract: starknet::core::types::Felt,
        decoder_ids: Vec<DecoderId>,
    ) -> Self {
        self.contract_filter
            .get_or_insert_with(ContractFilter::new)
            .mappings
            .insert(contract, decoder_ids);
        self
    }

    /// Add contract to blacklist (fast discard).
    ///
    /// Events from this contract will be discarded immediately (O(1) check).
    pub fn blacklist_contract(mut self, contract: starknet::core::types::Felt) -> Self {
        self.contract_filter
            .get_or_insert_with(ContractFilter::new)
            .blacklist
            .insert(contract);
        self
    }

    /// Add multiple contracts to blacklist.
    ///
    /// Events from these contracts will be discarded immediately (O(1) check).
    pub fn blacklist_contracts(mut self, contracts: Vec<starknet::core::types::Felt>) -> Self {
        self.contract_filter
            .get_or_insert_with(ContractFilter::new)
            .blacklist
            .extend(contracts);
        self
    }

    /// Add identification rule for auto-discovery.
    ///
    /// Identification rules are used to automatically identify contract types
    /// by inspecting their ABI. When a contract is encountered that's not in
    /// the explicit mappings, the registry will:
    /// 1. Fetch the contract's ABI from the chain
    /// 2. Run all identification rules
    /// 3. Cache the contract→decoder mapping for future events
    ///
    /// NOTE: Rules alone don't enable auto-identification. You must also provide
    /// a registry cache via `with_registry_cache()` for identification to work.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = ToriiConfig::builder()
    ///     .with_identification_rule(Box::new(Erc20Rule::new()))
    ///     .with_identification_rule(Box::new(Erc721Rule::new()))
    ///     .build();
    /// ```
    pub fn with_identification_rule(mut self, rule: Box<dyn IdentificationRule>) -> Self {
        self.identification_rules.push(rule);
        self
    }

    /// Set the shared registry cache from a ContractRegistry.
    ///
    /// When a registry cache is provided, the DecoderContext will:
    /// - Look up contract→decoder mappings from the cache
    /// - Skip events from contracts not in the cache (they should have been identified before decode)
    ///
    /// Without a registry cache, events from unmapped contracts are tried against all decoders.
    ///
    /// # Usage Pattern
    ///
    /// ```rust,ignore
    /// // Create registry with your provider and rules
    /// let registry = ContractRegistry::new(provider, engine_db)
    ///     .with_rule(Box::new(Erc20Rule::new()))
    ///     .with_rule(Box::new(Erc721Rule::new()));
    ///
    /// // Load cached mappings from database
    /// registry.load_from_db().await?;
    ///
    /// // Pass the cache to config
    /// let config = ToriiConfig::builder()
    ///     .with_registry_cache(registry.shared_cache())
    ///     .build();
    /// ```
    pub fn with_registry_cache(
        mut self,
        cache: Arc<
            tokio::sync::RwLock<
                std::collections::HashMap<starknet::core::types::Felt, Vec<DecoderId>>,
            >,
        >,
    ) -> Self {
        self.registry_cache = Some(cache);
        self
    }

    /// Set a contract identifier for runtime identification.
    ///
    /// When a contract identifier is provided, unknown contracts in each batch
    /// will be automatically identified by fetching their ABIs and running
    /// identification rules. This enables auto-discovery without explicit mapping.
    ///
    /// This method automatically extracts the registry cache for the DecoderContext.
    ///
    /// # Usage Pattern
    ///
    /// ```rust,ignore
    /// // Create registry with your provider and rules
    /// let registry = Arc::new(ContractRegistry::new(provider, engine_db)
    ///     .with_rule(Box::new(Erc20Rule::new()))
    ///     .with_rule(Box::new(Erc721Rule::new())));
    ///
    /// // Load cached mappings from database
    /// registry.load_from_db().await?;
    ///
    /// // Pass the full registry (enables runtime identification)
    /// let config = ToriiConfig::builder()
    ///     .with_contract_identifier(registry)
    ///     .build();
    /// ```
    pub fn with_contract_identifier(mut self, identifier: Arc<dyn ContractIdentifier>) -> Self {
        // Extract cache for DecoderContext
        self.registry_cache = Some(identifier.shared_cache());
        self.contract_identifier = Some(identifier);
        self
    }

    /// Sets the graceful shutdown timeout in seconds.
    ///
    /// When a shutdown signal (SIGINT/SIGTERM) is received, the system will wait
    /// up to this duration for the current ETL batch to complete before forcing
    /// shutdown. Default is 30 seconds.
    pub fn shutdown_timeout(mut self, seconds: u64) -> Self {
        self.shutdown_timeout = Some(seconds);
        self
    }

    /// Sets ETL prefetch and decode concurrency.
    pub fn etl_concurrency(mut self, config: EtlConcurrencyConfig) -> Self {
        self.etl_concurrency = Some(config);
        self
    }

    pub fn with_command_handler(mut self, handler: Box<dyn CommandHandler>) -> Self {
        self.command_handlers.push(handler);
        self
    }

    pub fn with_command_handlers(mut self, handlers: Vec<Box<dyn CommandHandler>>) -> Self {
        self.command_handlers.extend(handlers);
        self
    }

    pub fn command_bus_queue_size(mut self, size: usize) -> Self {
        self.command_bus_queue_size = Some(size.max(1));
        self
    }

    pub fn with_tls(mut self, tls: ToriiTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Builds the Torii configuration.
    ///
    /// # Panics
    ///
    /// Panics if the contract filter configuration is invalid (e.g., a contract appears
    /// in both the mapping and blacklist).
    pub fn build(self) -> ToriiConfig {
        let contract_filter = self.contract_filter.unwrap_or_default();

        // Validate contract filter
        if let Err(e) = contract_filter.validate() {
            panic!("Invalid contract filter configuration: {e}");
        }

        ToriiConfig {
            port: self.port.unwrap_or(8080),
            host: self.host.unwrap_or_else(|| "0.0.0.0".to_string()),
            sinks: self.sinks,
            decoders: self.decoders,
            partial_grpc_router: self.partial_grpc_router,
            custom_reflection: self.custom_reflection,
            cycle_interval: self.cycle_interval.unwrap_or(3),
            events_per_cycle: self.events_per_cycle.unwrap_or(5),
            sample_events: self.sample_events,
            extractor: self.extractor,
            database_root: self.database_root.unwrap_or_else(|| PathBuf::from(".")),
            engine_database_url: self.engine_database_url,
            contract_filter,
            identification_rules: self.identification_rules,
            registry_cache: self.registry_cache,
            contract_identifier: self.contract_identifier,
            shutdown_timeout: self.shutdown_timeout.unwrap_or(30),
            etl_concurrency: self.etl_concurrency.unwrap_or_default(),
            command_handlers: self.command_handlers,
            command_bus_queue_size: self.command_bus_queue_size.unwrap_or(4096),
            tls: self.tls,
        }
    }
}
