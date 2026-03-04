//! HTTP API handlers for the SQL sink
//!
//! This module contains the HTTP endpoint handlers that expose SQL query functionality
//! and event retrieval endpoints.

use axum::{extract::State, http::StatusCode, response::Json};
use serde::{Deserialize, Serialize};
use sqlx::{Column, Row};
use std::sync::Arc;

use crate::DbBackend;

/// Shared state for SQL sink routes
#[derive(Clone)]
pub struct SqlSinkState {
    pub(crate) pool: Arc<sqlx::Pool<sqlx::Any>>,
    pub(crate) backend: DbBackend,
}

/// Request body for SQL query endpoint
#[derive(Deserialize)]
pub struct SqlQueryRequest {
    pub query: String,
}

/// Response for SQL query endpoints
#[derive(Serialize)]
pub struct SqlQueryResponse {
    pub rows: Vec<serde_json::Value>,
    pub count: usize,
}

/// POST /sql/query - Execute raw SQL query.
///
/// Allows clients to execute arbitrary SQL queries against the sink's database.
/// This is useful for exploring the data and debugging.
///
/// **Warning**: In production, this should be secured or disabled.
pub async fn sql_query_handler(
    State(state): State<SqlSinkState>,
    Json(req): Json<SqlQueryRequest>,
) -> Result<Json<SqlQueryResponse>, (StatusCode, String)> {
    tracing::info!(target: "torii::sinks::sql::api", "Executing query: {}", req.query);

    let rows = sqlx::query(&req.query)
        .fetch_all(state.pool.as_ref())
        .await
        .map_err(|e| {
            tracing::error!(target: "torii::sinks::sql::api", "Query error: {}", e);
            (StatusCode::BAD_REQUEST, format!("Query failed: {e}"))
        })?;

    let results: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|row| {
            let mut map = serde_json::Map::new();

            // Extract all columns dynamically
            for (idx, column) in row.columns().iter().enumerate() {
                let name = column.name();

                // Try different types
                if let Ok(value) = row.try_get::<i64, _>(idx) {
                    map.insert(name.to_string(), serde_json::json!(value));
                } else if let Ok(value) = row.try_get::<String, _>(idx) {
                    map.insert(name.to_string(), serde_json::json!(value));
                } else if let Ok(value) = row.try_get::<f64, _>(idx) {
                    map.insert(name.to_string(), serde_json::json!(value));
                }
            }

            serde_json::Value::Object(map)
        })
        .collect();

    let count = results.len();

    Ok(Json(SqlQueryResponse {
        rows: results,
        count,
    }))
}

/// GET /sql/events - List all SQL operations.
///
/// Returns the most recent SQL operations (inserts, updates) from the sink's database.
/// Limited to 100 most recent operations.
pub async fn sql_events_handler(
    State(state): State<SqlSinkState>,
) -> Result<Json<SqlQueryResponse>, (StatusCode, String)> {
    tracing::info!(target: "torii::sinks::sql::api", "Fetching all SQL operations");

    let query = match state.backend {
        DbBackend::Sqlite => "SELECT * FROM sql_operation ORDER BY created_at DESC LIMIT 100",
        DbBackend::Postgres => {
            "SELECT * FROM sql_sink.sql_operation ORDER BY created_at DESC LIMIT 100"
        }
    };

    let rows = sqlx::query(query)
        .fetch_all(state.pool.as_ref())
        .await
        .map_err(|e| {
            tracing::error!(target: "torii::sinks::sql::api", "Query error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch operations: {e}"),
            )
        })?;

    let results: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|row| {
            let mut map = serde_json::Map::new();

            if let Ok(id) = row.try_get::<i64, _>("id") {
                map.insert("id".to_string(), serde_json::json!(id));
            }
            if let Ok(table_name) = row.try_get::<String, _>("table_name") {
                map.insert("table_name".to_string(), serde_json::json!(table_name));
            }
            if let Ok(operation) = row.try_get::<String, _>("operation") {
                map.insert("operation".to_string(), serde_json::json!(operation));
            }
            if let Ok(value) = row.try_get::<i64, _>("value") {
                map.insert("value".to_string(), serde_json::json!(value));
            }

            serde_json::Value::Object(map)
        })
        .collect();

    let count = results.len();

    Ok(Json(SqlQueryResponse {
        rows: results,
        count,
    }))
}
