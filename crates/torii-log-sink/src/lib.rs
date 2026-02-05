pub mod api;
pub mod decoder;
pub mod grpc_service;

// Include generated protobuf code
pub mod proto {
    include!("generated/torii.sinks.log.rs");
}

// File descriptor set for gRPC reflection
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/log_descriptor.bin");

use async_trait::async_trait;
use prost::Message;
use prost_types::Any;
use std::sync::Arc;

use torii::axum::{routing::get, Router};
use torii::etl::{
    envelope::{Envelope, TypeId},
    extractor::ExtractionBatch,
    sink::{EventBus, Sink, TopicInfo},
};
use torii::grpc::UpdateType;

pub use decoder::{LogDecoder, LogEntry};
pub use grpc_service::LogSinkService;
pub use proto::{LogEntry as ProtoLogEntry, LogUpdate};

/// LogSink collects and stores log entries from events
///
/// This sink demonstrates all three extension points:
/// 1. **EventBus**: Publishes to central topic-based subscriptions (via `torii.Torii/Subscribe`)
/// 2. **gRPC Service**: Provides QueryLogs and SubscribeLogs RPCs
/// 3. **REST HTTP**: Exposes `/logs` and `/logs/count` endpoints
pub struct LogSink {
    event_bus: Option<Arc<EventBus>>,
    /// Internal gRPC service (self-contained with broadcast channel)
    grpc_service: Arc<LogSinkService>,
    /// Log counter for unique IDs
    log_counter: Arc<std::sync::atomic::AtomicU64>,
}

impl LogSink {
    /// Creates a new LogSink.
    ///
    /// # Arguments
    /// * `max_logs` - Maximum number of logs to keep in memory (default: 100)
    pub fn new(max_logs: usize) -> Self {
        let grpc_service = Arc::new(LogSinkService::new(max_logs));

        tracing::info!(
            target: "torii::sinks::log",
            "LogSink initialized (max_logs: {})",
            max_logs
        );

        Self {
            event_bus: None,
            grpc_service,
            log_counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Gets a clone of the gRPC service implementation.
    ///
    /// This allows users to add the log sink's gRPC service to their tonic router
    /// before passing it to Torii. The service is cloneable and thread-safe.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use torii_log_sink::{LogSink, proto::log_sink_server::LogSinkServer};
    /// use tonic::transport::Server;
    ///
    /// let log_sink = LogSink::new(100);
    /// let service = log_sink.get_grpc_service_impl();
    ///
    /// // Build gRPC router with sink services
    /// let grpc_router = Server::builder()
    ///     .accept_http1(true)
    ///     .add_service(tonic_web::enable(LogSinkServer::new((*service).clone())));
    ///
    /// // Pass to Torii
    /// let config = ToriiConfig::builder()
    ///     .add_sink_boxed(Box::new(log_sink))
    ///     .with_grpc_router(grpc_router)
    ///     .build();
    /// ```
    pub fn get_grpc_service_impl(&self) -> Arc<LogSinkService> {
        self.grpc_service.clone()
    }
}

#[async_trait]
impl Sink for LogSink {
    fn name(&self) -> &'static str {
        "log"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("log.entry")]
    }

    async fn process(
        &self,
        envelopes: &[Envelope],
        _batch: &ExtractionBatch,
    ) -> anyhow::Result<()> {
        for envelope in envelopes {
            if envelope.type_id == TypeId::new("log.entry") {
                if let Some(log_entry) = envelope.downcast_ref::<LogEntry>() {
                    // Generate unique ID.
                    let log_id = self
                        .log_counter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                    let proto_log = ProtoLogEntry {
                        id: log_id,
                        message: log_entry.message.clone(),
                        timestamp: chrono::Utc::now().timestamp(),
                        block_number: log_entry.block_number,
                        event_key: log_entry.event_key.clone(),
                    };

                    tracing::info!(
                        target: "torii::sinks::log",
                        "Log entry #{}: {} (block: {})",
                        log_id,
                        log_entry.message,
                        log_entry.block_number
                    );

                    self.grpc_service.log_store().add_log(proto_log.clone());

                    // Broadcast to EventBus subscribers (central subscription).
                    if let Some(event_bus) = &self.event_bus {
                        let mut buf = Vec::new();
                        proto_log.encode(&mut buf)?;
                        let any = Any {
                            type_url: "type.googleapis.com/torii.sinks.log.LogEntry".to_string(),
                            value: buf,
                        };

                        event_bus.publish_protobuf(
                            "logs",
                            "log.entry",
                            &any,
                            &proto_log,
                            UpdateType::Created,
                            |_log: &ProtoLogEntry,
                             _filters: &std::collections::HashMap<String, String>| {
                                // For now, no filtering - all logs match
                                true
                            },
                        );
                    }

                    // Broadcast to gRPC subscribers (sink-specific subscription).
                    let update = LogUpdate {
                        log: Some(proto_log),
                        timestamp: chrono::Utc::now().timestamp(),
                    };
                    let _ = self.grpc_service.update_tx.send(update);
                }
            }
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![TopicInfo::new(
            "logs",
            // No filters for now.
            vec![],
            "Real-time log entries from decoded events",
        )]
    }

    fn build_routes(&self) -> Router {
        let state = api::LogSinkState {
            log_store: self.grpc_service.log_store().clone(),
        };

        Router::new()
            .route("/logs", get(api::logs_handler))
            .route("/logs/count", get(api::logs_count_handler))
            .with_state(state)
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        _context: &torii::etl::sink::SinkContext,
    ) -> anyhow::Result<()> {
        self.event_bus = Some(event_bus);
        tracing::info!(target: "torii::sinks::log", "LogSink initialized with event bus");
        Ok(())
    }
}
