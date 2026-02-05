pub mod api;
pub mod decoder;
pub mod grpc_service;
pub mod samples;

// Include generated protobuf code
pub mod proto {
    include!("generated/torii.sinks.sql.rs");
}

// File descriptor set for gRPC reflection
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/sql_descriptor.bin");

use async_trait::async_trait;
use axum::{
    routing::{get, post},
    Router,
};
use prost::Message;
use prost_types::Any;
use sqlx::sqlite::SqlitePool;
use std::sync::Arc;

use torii::etl::{
    envelope::{Envelope, TypeId},
    extractor::ExtractionBatch,
    sink::{EventBus, Sink, TopicInfo},
};
use torii::grpc::UpdateType;
use starknet::core::types::EmittedEvent;

pub use decoder::{SqlDecoder, SqlInsert, SqlUpdate};
pub use grpc_service::SqlSinkService;
pub use proto::{SqlOperation as ProtoSqlOperation, SqlOperationUpdate};

/// SqlSink stores data in SQLite and exposes SQL query endpoints.
///
/// This sink demonstrates all three extension points:
/// 1. **EventBus**: Publishes to central topic-based subscriptions
/// 2. **gRPC Service**: Provides Query, StreamQuery, GetSchema, and Subscribe RPCs
/// 3. **REST HTTP**: Exposes `/sql/query` and `/sql/events` endpoints
pub struct SqlSink {
    pool: Arc<SqlitePool>,
    event_bus: Option<Arc<EventBus>>,
    /// Internal gRPC service (self-contained with broadcast channel)
    grpc_service: Arc<SqlSinkService>,
}

impl SqlSink {
    /// Generates sample events for testing the SQL sink.
    pub fn generate_sample_events() -> Vec<EmittedEvent> {
        samples::generate_sample_events()
    }

    /// Gets a clone of the gRPC service implementation.
    ///
    /// This allows users to add the SQL sink's gRPC service to their tonic router
    /// before passing it to Torii. The service is cloneable and thread-safe.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use torii_sql_sink::{SqlSink, proto::sql_sink_server::SqlSinkServer};
    /// use tonic::transport::Server;
    ///
    /// let sql_sink = SqlSink::new("sqlite::memory:").await?;
    /// let service = sql_sink.get_grpc_service_impl();
    ///
    /// // Build gRPC router with sink services
    /// let grpc_router = Server::builder()
    ///     .accept_http1(true)
    ///     .add_service(tonic_web::enable(SqlSinkServer::new((*service).clone())));
    ///
    /// // Pass to Torii
    /// let config = ToriiConfig::builder()
    ///     .add_sink_boxed(Box::new(sql_sink))
    ///     .with_grpc_router(grpc_router)
    ///     .build();
    /// ```
    pub fn get_grpc_service_impl(&self) -> Arc<SqlSinkService> {
        self.grpc_service.clone()
    }

    pub async fn new(database_url: &str) -> anyhow::Result<Self> {
        let pool = SqlitePool::connect(database_url).await?;
        let pool = Arc::new(pool);

        // Create tables for SQL operations
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sql_operation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                value INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(pool.as_ref())
        .await?;

        // Create gRPC service internally (with its own broadcast channel)
        let grpc_service = Arc::new(SqlSinkService::new(pool.clone()));

        tracing::info!(
            target: "torii::sinks::sql",
            "SqlSink initialized with database: {}",
            database_url
        );

        Ok(Self {
            pool,
            event_bus: None,
            grpc_service,
        })
    }

    /// Filters function for SQL sink (optimized - works on decoded data).
    ///
    /// Supports filters:
    /// - "table": Filter by table name (e.g., "user", "order")
    /// - "operation": Filter by operation type (e.g., "insert", "update")
    /// - "value_gt": Value greater than
    /// - "value_lt": Value less than
    /// - "value_gte": Value greater than or equal
    /// - "value_lte": Value less than or equal
    /// - "value_eq": Value equal to
    fn matches_filters(
        operation: &ProtoSqlOperation,
        filters: &std::collections::HashMap<String, String>,
    ) -> bool {
        if filters.is_empty() {
            return true;
        }

        if let Some(table_filter) = filters.get("table") {
            if &operation.table != table_filter {
                return false;
            }
        }

        if let Some(operation_filter) = filters.get("operation") {
            if &operation.operation != operation_filter {
                return false;
            }
        }

        if let Some(value_gt) = filters.get("value_gt") {
            if let Ok(threshold) = value_gt.parse::<u64>() {
                if operation.value <= threshold {
                    return false;
                }
            }
        }

        if let Some(value_lt) = filters.get("value_lt") {
            if let Ok(threshold) = value_lt.parse::<u64>() {
                if operation.value >= threshold {
                    return false;
                }
            }
        }

        if let Some(value_gte) = filters.get("value_gte") {
            if let Ok(threshold) = value_gte.parse::<u64>() {
                if operation.value < threshold {
                    return false;
                }
            }
        }

        if let Some(value_lte) = filters.get("value_lte") {
            if let Ok(threshold) = value_lte.parse::<u64>() {
                if operation.value > threshold {
                    return false;
                }
            }
        }

        if let Some(value_eq) = filters.get("value_eq") {
            if let Ok(threshold) = value_eq.parse::<u64>() {
                if operation.value != threshold {
                    return false;
                }
            }
        }

        true
    }
}

#[async_trait]
impl Sink for SqlSink {
    fn name(&self) -> &str {
        "sql"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("sql.insert"), TypeId::new("sql.update")]
    }

    async fn process(
        &self,
        envelopes: &[Envelope],
        _batch: &ExtractionBatch,
    ) -> anyhow::Result<()> {
        for envelope in envelopes.iter() {
            if envelope.type_id == TypeId::new("sql.insert") {
                if let Some(insert) = envelope.downcast_ref::<SqlInsert>() {
                    sqlx::query(
                        r#"
                        INSERT INTO sql_operation (table_name, operation, value)
                        VALUES (?, ?, ?)
                        "#,
                    )
                    .bind(&insert.table)
                    .bind("insert")
                    .bind(insert.value as i64)
                    .execute(self.pool.as_ref())
                    .await?;

                    tracing::info!(
                        target: "torii::sinks::sql",
                        "SQL INSERT operation: table={}, value={}",
                        insert.table,
                        insert.value
                    );

                    let proto_msg = ProtoSqlOperation {
                        table: insert.table.clone(),
                        operation: "insert".to_string(),
                        value: insert.value,
                    };

                    // Broadcast to EventBus subscribers (central subscription).
                    if let Some(event_bus) = &self.event_bus {
                        let mut buf = Vec::new();
                        proto_msg.encode(&mut buf)?;
                        let any = Any {
                            type_url: "type.googleapis.com/torii.sinks.sql.SqlOperation"
                                .to_string(),
                            value: buf,
                        };

                        event_bus.publish_protobuf(
                            "sql",
                            "sql.insert",
                            &any,
                            &proto_msg,
                            UpdateType::Created,
                            Self::matches_filters,
                        );
                    }

                    // Broadcast to gRPC subscribers (sink-specific subscription).
                    let update = SqlOperationUpdate {
                        operation: Some(proto_msg),
                        timestamp: chrono::Utc::now().timestamp(),
                    };
                    let _ = self.grpc_service.update_tx.send(update);
                }
            }
            else if envelope.type_id == TypeId::new("sql.update") {
                if let Some(update) = envelope.downcast_ref::<SqlUpdate>() {
                    sqlx::query(
                        r#"
                        INSERT INTO sql_operation (table_name, operation, value)
                        VALUES (?, ?, ?)
                        "#,
                    )
                    .bind(&update.table)
                    .bind("update")
                    .bind(update.value as i64)
                    .execute(self.pool.as_ref())
                    .await?;

                    tracing::info!(
                        target: "torii::sinks::sql",
                        "SQL UPDATE operation: table={}, value={}",
                        update.table,
                        update.value
                    );

                    let proto_msg = ProtoSqlOperation {
                        table: update.table.clone(),
                        operation: "update".to_string(),
                        value: update.value,
                    };

                    // Broadcast to EventBus subscribers (central subscription).
                    if let Some(event_bus) = &self.event_bus {
                        let mut buf = Vec::new();
                        proto_msg.encode(&mut buf)?;
                        let any = Any {
                            type_url: "type.googleapis.com/torii.sinks.sql.SqlOperation"
                                .to_string(),
                            value: buf,
                        };

                        event_bus.publish_protobuf(
                            "sql",
                            "sql.update",
                            &any,
                            &proto_msg,
                            UpdateType::Updated,
                            Self::matches_filters,
                        );
                    }

                    // Broadcast to gRPC subscribers (sink-specific subscription).
                    let sql_update = SqlOperationUpdate {
                        operation: Some(proto_msg),
                        timestamp: chrono::Utc::now().timestamp(),
                    };
                    let _ = self.grpc_service.update_tx.send(sql_update);
                }
            }
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![TopicInfo::new(
            "sql",
            vec![
                "table".to_string(),
                "operation".to_string(),
                "value_gt".to_string(),
                "value_lt".to_string(),
                "value_gte".to_string(),
                "value_lte".to_string(),
                "value_eq".to_string(),
            ],
            "SQL operations (insert, update) with support for table, operation, and value-based filtering",
        )]
    }

    fn build_routes(&self) -> Router {
        let state = api::SqlSinkState {
            pool: self.pool.clone(),
        };

        Router::new()
            .route("/sql/query", post(api::sql_query_handler))
            .route("/sql/events", get(api::sql_events_handler))
            .with_state(state)
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        _context: &torii::etl::sink::SinkContext,
    ) -> anyhow::Result<()> {
        self.event_bus = Some(event_bus);
        tracing::info!(target: "torii::sinks::sql", "SqlSink initialized with event bus");
        Ok(())
    }
}
