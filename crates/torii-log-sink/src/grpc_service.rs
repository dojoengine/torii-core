use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::proto::{
    log_sink_server::LogSink as LogSinkTrait, LogEntry as ProtoLogEntry, LogUpdate,
    QueryLogsRequest, QueryLogsResponse,
};

/// Internal log store (shared between sink and gRPC service)
#[derive(Clone)]
pub struct LogStore {
    logs: Arc<RwLock<VecDeque<ProtoLogEntry>>>,
    max_logs: usize,
}

impl LogStore {
    pub fn new(max_logs: usize) -> Self {
        Self {
            logs: Arc::new(RwLock::new(VecDeque::new())),
            max_logs,
        }
    }

    pub fn add_log(&self, log: ProtoLogEntry) {
        let mut logs = self.logs.write().unwrap();
        logs.push_back(log);
        if logs.len() > self.max_logs {
            logs.pop_front();
        }
    }

    pub fn get_recent(&self, limit: usize) -> Vec<ProtoLogEntry> {
        let logs = self.logs.read().unwrap();
        logs.iter()
            .rev()
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }
}

/// gRPC service implementation for LogSink
#[derive(Clone)]
pub struct LogSinkService {
    log_store: LogStore,
    /// Broadcast channel for real-time updates
    pub update_tx: broadcast::Sender<LogUpdate>,
}

impl LogSinkService {
    pub fn new(max_logs: usize) -> Self {
        let (update_tx, _) = broadcast::channel(1000);
        Self {
            log_store: LogStore::new(max_logs),
            update_tx,
        }
    }

    pub fn log_store(&self) -> &LogStore {
        &self.log_store
    }
}

#[tonic::async_trait]
impl LogSinkTrait for LogSinkService {
    async fn query_logs(
        &self,
        request: Request<QueryLogsRequest>,
    ) -> Result<Response<QueryLogsResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            5
        } else {
            req.limit as usize
        };

        let logs = self.log_store.get_recent(limit);

        tracing::debug!(
            target: "torii::sinks::log",
            "QueryLogs: returning {} logs (limit: {})",
            logs.len(),
            limit
        );

        Ok(Response::new(QueryLogsResponse { logs }))
    }

    type SubscribeLogsStream =
        Pin<Box<dyn Stream<Item = Result<LogUpdate, Status>> + Send + 'static>>;

    async fn subscribe_logs(
        &self,
        request: Request<QueryLogsRequest>,
    ) -> Result<Response<Self::SubscribeLogsStream>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 { 5 } else { req.limit };

        // Send recent logs first.
        let recent_logs = self.log_store.get_recent(limit as usize);
        let mut initial_updates = Vec::new();
        for log in recent_logs {
            initial_updates.push(Ok(LogUpdate {
                log: Some(log.clone()),
                timestamp: chrono::Utc::now().timestamp(),
            }));
        }

        // Subscribe to new updates.
        let rx = self.update_tx.subscribe();
        let stream = BroadcastStream::new(rx);

        // Capture count before moving.
        let initial_count = initial_updates.len();

        let combined_stream =
            tokio_stream::iter(initial_updates).chain(stream.map(|result| {
                result.map_err(|e| Status::internal(format!("Broadcast error: {e}")))
            }));

        tracing::info!(
            target: "torii::sinks::log",
            "New subscription to logs (sent {} recent logs)",
            initial_count
        );

        Ok(Response::new(Box::pin(combined_stream)))
    }
}
