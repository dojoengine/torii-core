//! Torii - Modular blockchain indexer.
//!
//! This library aims at providing a modular and high-performance blockchain indexer.
//! The current implementation is still WIP, but gives a good idea of the architecture and the capabilities.

pub mod etl;
pub mod grpc;
pub mod http;

// Include generated protobuf code
pub mod proto {
    pub mod torii {
        tonic::include_proto!("torii");
    }
}

// Re-export commonly used types for external sink authors
pub use async_trait::async_trait;
pub use axum;
pub use tokio;
pub use tonic;

// Re-export UpdateType for sink implementations
pub use grpc::UpdateType;

use axum::Router as AxumRouter;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tower_http::cors::{Any as CorsAny, CorsLayer};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use etl::extractor::Extractor;
use etl::sink::{EventBus, Sink};
use etl::{Decoder, MultiDecoder, MultiSink, SampleExtractor};
use grpc::{create_grpc_service, GrpcState, SubscriptionManager};
use http::create_http_router;

// Include the file descriptor set generated at build time.
// This is also exported publicly so external sink authors can use it for reflection.
const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../target/descriptor.bin");

/// Torii core gRPC service descriptor set (for reflection).
///
/// External sink authors should use this when building custom reflection services
/// that include both core Torii and their sink services.
///
/// # Example
///
/// ```rust,ignore
/// use torii::TORII_DESCRIPTOR_SET;
///
/// let reflection = tonic_reflection::server::Builder::configure()
///     .register_encoded_file_descriptor_set(TORII_DESCRIPTOR_SET)
///     .register_encoded_file_descriptor_set(my_sink::FILE_DESCRIPTOR_SET)
///     .build_v1()?;
/// ```
///
/// This descriptor set is generated at build time from `proto/torii.proto`
/// by the `build.rs` script. See `build.rs` for details.
pub const TORII_DESCRIPTOR_SET: &[u8] = FILE_DESCRIPTOR_SET;

/// Configuration for Torii server with pluggable sinks and decoders.
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
}

impl ToriiConfig {
    pub fn builder() -> ToriiConfigBuilder {
        ToriiConfigBuilder::default()
    }
}

/// Builder for ToriiConfig.
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
}

impl Default for ToriiConfigBuilder {
    fn default() -> Self {
        Self {
            port: None,
            host: None,
            sinks: Vec::new(),
            decoders: Vec::new(),
            partial_grpc_router: None,
            custom_reflection: false,
            cycle_interval: None,
            events_per_cycle: None,
            sample_events: Vec::new(),
        }
    }
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

    /// Builds the Torii configuration.
    pub fn build(self) -> ToriiConfig {
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
        }
    }
}

/// Starts the Torii server with custom configuration.
///
/// TODO: this function is just too big. But it has the whole workflow.
/// This will be split into smaller functions in the future with associated configuration for each step.
pub async fn run(config: ToriiConfig) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "torii=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!(target: "torii::main", "Starting Torii with {} sink(s) and {} decoder(s)",
        config.sinks.len(), config.decoders.len());

    let subscription_manager = Arc::new(SubscriptionManager::new());
    let event_bus = Arc::new(EventBus::new(subscription_manager.clone()));

    let mut initialized_sinks: Vec<Arc<dyn Sink>> = Vec::new();

    for mut sink in config.sinks {
        // Box is used for sinks since we need to call initialize (mutable reference).
        sink.initialize(event_bus.clone()).await?;
        // Convert Box<dyn Sink> to Arc<dyn Sink> since now we can use it immutably.
        initialized_sinks.push(Arc::from(sink));
    }

    let multi_sink = Arc::new(MultiSink::new(initialized_sinks));
    let multi_decoder = Arc::new(MultiDecoder::new(config.decoders));

    let topics = multi_sink.topics();

    let grpc_state = GrpcState::new(subscription_manager.clone(), topics);
    let grpc_service = create_grpc_service(grpc_state);

    let has_user_grpc_services = config.partial_grpc_router.is_some();
    let mut grpc_router = if let Some(partial_router) = config.partial_grpc_router {
        tracing::info!(target: "torii::main", "Using user-provided gRPC router with sink services");
        partial_router.add_service(tonic_web::enable(grpc_service))
    } else {
        Server::builder()
            // Accept HTTP/1.1 requests required for gRPC-Web to work.
            .accept_http1(true)
            .add_service(tonic_web::enable(grpc_service))
    };

    if !config.custom_reflection {
        let reflection_v1 = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        let reflection_v1alpha = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1alpha()?;

        grpc_router = grpc_router
            .add_service(tonic_web::enable(reflection_v1))
            .add_service(tonic_web::enable(reflection_v1alpha));

        tracing::info!(target: "torii::main", "Added reflection services (core descriptors only)");
    } else {
        tracing::info!(target: "torii::main", "Using custom reflection services (user-provided)");
    }

    let sinks_routes = multi_sink.build_routes();
    let http_router = create_http_router().merge(sinks_routes);

    let cors = CorsLayer::new()
        .allow_origin(CorsAny)
        .allow_methods(CorsAny)
        .allow_headers(CorsAny)
        .expose_headers(vec![
            axum::http::HeaderName::from_static("grpc-status"),
            axum::http::HeaderName::from_static("grpc-message"),
            axum::http::HeaderName::from_static("grpc-status-details-bin"),
            axum::http::HeaderName::from_static("x-grpc-web"),
            axum::http::HeaderName::from_static("content-type"),
        ]);

    // Until some compatibility issues are resolved with axum, we need to allow this deprecated code.
    // See: https://github.com/hyperium/tonic/issues/1964.
    #[allow(warnings, deprecated)]
    let app = AxumRouter::new()
        .merge(grpc_router.into_router())
        .merge(http_router)
        .layer(cors);

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    tracing::info!(target: "torii::main", "Server listening on {}", addr);

    tracing::info!(target: "torii::main", "gRPC Services:");
    tracing::info!(target: "torii::main", "   torii.Torii - Core service");
    if has_user_grpc_services {
        tracing::info!(target: "torii::main", "   + User-provided sink gRPC services");
    }

    let engine_db_config = etl::engine_db::EngineDbConfig {
        path: ":memory:".to_string(),
    };
    let engine_db = etl::EngineDb::new(engine_db_config).await?;
    let engine_db = Arc::new(engine_db);

    // Setup and start the ETL pipeline.
    let etl_multi_sink = multi_sink.clone();
    let etl_multi_decoder = multi_decoder.clone();
    let etl_engine_db = engine_db.clone();
    let cycle_interval = config.cycle_interval;
    let events_per_cycle = config.events_per_cycle;
    let sample_events = config.sample_events;

    tokio::spawn(async move {
        tracing::info!(target: "torii::etl", "Starting ETL pipeline...");

        // Wait a bit for the server to be ready.
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        if sample_events.is_empty() {
            tracing::warn!(target: "torii::etl", "No sample events provided, ETL loop will idle");
            return;
        }

        tracing::info!(
            target: "torii::etl",
            "Loaded {} sample event types (will cycle through them)",
            sample_events.len()
        );

        let mut extractor = SampleExtractor::new(sample_events, events_per_cycle);

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(cycle_interval));

        loop {
            interval.tick().await;

            // Extract the events from the source.
            let batch = match extractor.extract(None, &etl_engine_db).await {
                Ok(batch) => batch,
                Err(e) => {
                    tracing::error!(target: "torii::etl", "Extract failed: {}", e);
                    continue;
                }
            };

            if batch.is_empty() {
                continue;
            }

            tracing::info!(
                target: "torii::etl",
                "Extracted {} events",
                batch.len()
            );

            // Update the engine DB stats for now here. Temporary.
            let latest_block = batch.blocks.keys().max().copied().unwrap_or(0);
            if let Err(e) = etl_engine_db
                .update_head(latest_block, batch.len() as u64)
                .await
            {
                tracing::warn!(target: "torii::etl", "Failed to update engine DB: {}", e);
            }

            // Transform the events into envelopes.
            let envelopes = match etl_multi_decoder.decode(&batch.events).await {
                Ok(envelopes) => envelopes,
                Err(e) => {
                    tracing::error!(target: "torii::etl", "Decode failed: {}", e);
                    continue;
                }
            };

            // Load the envelopes into the sinks.
            if let Err(e) = etl_multi_sink.process(&envelopes, &batch).await {
                tracing::error!(target: "torii::etl", "Sink processing failed: {}", e);
                continue;
            }

            tracing::info!(target: "torii::etl", "ETL cycle complete");
        }
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
