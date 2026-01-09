use torii::axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::grpc_service::LogStore;
use crate::proto::LogEntry as ProtoLogEntry;

/// Shared state for HTTP handlers
#[derive(Clone)]
pub struct LogSinkState {
    pub log_store: LogStore,
}

/// Query parameters for GET /logs
#[derive(Deserialize)]
pub struct LogsQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    5
}

/// Log entry for JSON response
#[derive(Serialize)]
pub struct LogEntryJson {
    id: u64,
    message: String,
    timestamp: i64,
    block_number: u64,
    event_key: String,
}

impl From<ProtoLogEntry> for LogEntryJson {
    fn from(log: ProtoLogEntry) -> Self {
        Self {
            id: log.id,
            message: log.message,
            timestamp: log.timestamp,
            block_number: log.block_number,
            event_key: log.event_key,
        }
    }
}

/// GET /logs - Returns recent logs.
///
/// Query parameters:
/// - limit: Number of logs to return (default: 5, max: 100)
pub async fn logs_handler(
    State(state): State<LogSinkState>,
    Query(query): Query<LogsQuery>,
) -> impl IntoResponse {
    let limit = query.limit.min(100);
    let logs = state.log_store.get_recent(limit);

    let json_logs: Vec<LogEntryJson> = logs.into_iter().map(LogEntryJson::from).collect();

    tracing::debug!(
        target: "torii::sinks::log::api",
        "GET /logs: returning {} logs",
        json_logs.len()
    );

    (StatusCode::OK, Json(json_logs))
}

/// GET /logs/count - Returns total number of logs.
pub async fn logs_count_handler(State(state): State<LogSinkState>) -> impl IntoResponse {
    let count = state.log_store.get_recent(usize::MAX).len();

    tracing::debug!(
        target: "torii::sinks::log::api",
        "GET /logs/count: {}",
        count
    );

    (StatusCode::OK, Json(serde_json::json!({ "count": count })))
}
