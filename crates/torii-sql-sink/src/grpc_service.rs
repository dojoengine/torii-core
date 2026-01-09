use async_trait::async_trait;
use futures::stream::Stream;
use sqlx::sqlite::{SqlitePool, SqliteRow};
use sqlx::{Column, Row};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};

use crate::proto::{
    sql_sink_server::SqlSink as SqlSinkTrait, GetSchemaRequest, GetSchemaResponse, QueryRequest,
    QueryResponse, QueryRow, SqlOperation, SqlOperationUpdate, SqlSubscribeRequest, TableSchema,
};

/// gRPC service implementation for SqlSink
#[derive(Clone)]
pub struct SqlSinkService {
    pool: Arc<SqlitePool>,
    /// Broadcast channel for real-time SQL operation updates
    pub update_tx: broadcast::Sender<SqlOperationUpdate>,
}

impl SqlSinkService {
    /// Creates a new SqlSinkService.
    pub fn new(pool: Arc<SqlitePool>) -> Self {
        // Create broadcast channel with capacity for 1000 pending updates
        let (tx, _rx) = broadcast::channel(1000);

        Self {
            pool,
            update_tx: tx,
        }
    }

    /// Broadcasts a SQL operation to all subscribers.
    pub fn broadcast_operation(&self, operation: SqlOperation) {
        let update = SqlOperationUpdate {
            operation: Some(operation),
            timestamp: chrono::Utc::now().timestamp(),
        };

        // Send to all subscribers (ignore if no receivers).
        let _ = self.update_tx.send(update);
    }

    /// Helper to convert a SQLite row to a QueryRow proto message.
    fn row_to_proto(row: &SqliteRow) -> Result<QueryRow, Status> {
        let mut columns = std::collections::HashMap::new();

        for (idx, col) in row.columns().iter().enumerate() {
            let col_name = col.name().to_string();

            // Try to get the value as various types and convert to string
            let value = if let Ok(v) = row.try_get::<i64, _>(idx) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<String, _>(idx) {
                v
            } else if let Ok(v) = row.try_get::<f64, _>(idx) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<bool, _>(idx) {
                v.to_string()
            } else {
                // If we can't decode it, use NULL.
                "NULL".to_string()
            };

            columns.insert(col_name, value);
        }

        Ok(QueryRow { columns })
    }
}

#[async_trait]
impl SqlSinkTrait for SqlSinkService {
    /// Executes a SQL query and return all results.
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let req = request.into_inner();
        let query = req.query;

        tracing::info!(target: "torii::sql_sink::grpc", "Executing query: {}", query);

        let query_with_limit = if let Some(limit) = req.limit {
            format!("{} LIMIT {}", query, limit)
        } else {
            query
        };

        let rows = sqlx::query(&query_with_limit)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| Status::invalid_argument(format!("Query failed: {}", e)))?;

        let proto_rows: Vec<QueryRow> = rows
            .iter()
            .map(Self::row_to_proto)
            .collect::<Result<Vec<_>, _>>()?;

        let total_rows = proto_rows.len() as i32;

        tracing::info!(
            target: "torii::sql_sink::grpc",
            "Query returned {} rows",
            total_rows
        );

        Ok(Response::new(QueryResponse {
            rows: proto_rows,
            total_rows,
        }))
    }

    /// Stream query results row-by-row (for large result sets)
    type StreamQueryStream = Pin<Box<dyn Stream<Item = Result<QueryRow, Status>> + Send>>;

    /// Streams query results row-by-row (for large result sets).
    async fn stream_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::StreamQueryStream>, Status> {
        let req = request.into_inner();
        let query = req.query;

        tracing::info!(
            target: "torii::sql_sink::grpc",
            "Starting streaming query: {}",
            query
        );

        let query_with_limit = if let Some(limit) = req.limit {
            format!("{} LIMIT {}", query, limit)
        } else {
            query
        };

        let pool = self.pool.clone();

        let stream = async_stream::try_stream! {
            let mut rows = sqlx::query(&query_with_limit).fetch(pool.as_ref());

            use futures::TryStreamExt;

            let mut row_count = 0;

            while let Some(row) = rows.try_next().await.map_err(|e| {
                Status::internal(format!("Stream error: {}", e))
            })? {
                row_count += 1;
                let proto_row = SqlSinkService::row_to_proto(&row)?;
                tracing::debug!(
                    target: "torii::sql_sink::grpc",
                    "Yielding row {}: {:?}",
                    row_count,
                    proto_row.columns.keys().collect::<Vec<_>>()
                );
                yield proto_row;
            }

            tracing::info!(
                target: "torii::sql_sink::grpc",
                "Streaming query completed ({} rows)",
                row_count
            );
        };

        Ok(Response::new(Box::pin(stream)))
    }

    /// Gets database schema information.
    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let req = request.into_inner();

        tracing::info!(
            target: "torii::sql_sink::grpc",
            "Getting schema for table: {:?}",
            req.table_name
        );

        let table_query = if let Some(table_name) = req.table_name {
            format!(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='{}'",
                table_name
            )
        } else {
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                .to_string()
        };

        let table_rows = sqlx::query(&table_query)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| Status::internal(format!("Failed to fetch tables: {}", e)))?;

        let mut tables = Vec::new();

        for table_row in table_rows {
            let table_name: String = table_row
                .try_get(0)
                .map_err(|e| Status::internal(format!("Failed to read table name: {}", e)))?;

            let column_query = format!("PRAGMA table_info({})", table_name);
            let column_rows = sqlx::query(&column_query)
                .fetch_all(self.pool.as_ref())
                .await
                .map_err(|e| Status::internal(format!("Failed to fetch columns: {}", e)))?;

            let mut columns = std::collections::HashMap::new();
            for col_row in column_rows {
                let col_name: String = col_row
                    .try_get(1)
                    .map_err(|e| Status::internal(format!("Failed to read column name: {}", e)))?;
                let col_type: String = col_row
                    .try_get(2)
                    .map_err(|e| Status::internal(format!("Failed to read column type: {}", e)))?;
                columns.insert(col_name, col_type);
            }

            tables.push(TableSchema {
                name: table_name,
                columns,
            });
        }

        tracing::info!(
            target: "torii::sql_sink::grpc",
            "Schema query returned {} tables",
            tables.len()
        );

        Ok(Response::new(GetSchemaResponse { tables }))
    }

    /// Subscribes to real-time SQL operations.
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<SqlOperationUpdate, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<SqlSubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();

        tracing::info!(
            target: "torii::sql_sink::grpc",
            "New subscription from client: {} (table: {:?}, operation: {:?})",
            req.client_id,
            req.table,
            req.operation
        );

        let mut rx = self.update_tx.subscribe();

        let stream = async_stream::try_stream! {
            tracing::info!(
                target: "torii::sql_sink::grpc",
                "Subscription stream started for client: {}",
                req.client_id
            );

            loop {
                match rx.recv().await {
                    Ok(update) => {
                        let operation = update.operation.as_ref();

                        // Apply filters.
                        if let Some(op) = operation {
                            if let Some(ref table_filter) = req.table {
                                if &op.table != table_filter {
                                    continue;
                                }
                            }

                            if let Some(ref op_filter) = req.operation {
                                if &op.operation != op_filter {
                                    continue;
                                }
                            }
                        }

                        tracing::debug!(
                            target: "torii::sql_sink::grpc",
                            "Sending update to client {}: {:?}",
                            req.client_id,
                            operation.map(|o| format!("{}.{}", o.table, o.operation))
                        );

                        yield update;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            target: "torii::sql_sink::grpc",
                            "Client {} lagged, skipped {} updates",
                            req.client_id,
                            skipped
                        );

                        // Continue receiving.
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::info!(
                            target: "torii::sql_sink::grpc",
                            "Broadcast channel closed for client {}",
                            req.client_id
                        );
                        break;
                    }
                }
            }

            tracing::info!(
                target: "torii::sql_sink::grpc",
                "Subscription stream ended for client: {}",
                req.client_id
            );
        };

        Ok(Response::new(Box::pin(stream)))
    }
}
