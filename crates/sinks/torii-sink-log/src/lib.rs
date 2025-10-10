use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use torii_core::{Batch, Sink, SinkFactory, SinkRegistry};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogSinkConfig {
    #[serde(default)]
    pub label: Option<String>,
}

pub struct LogSink {
    label: String,
}

impl LogSink {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
        }
    }
}

#[async_trait]
impl Sink for LogSink {
    fn label(&self) -> &str {
        &self.label
    }

    async fn handle_batch(&self, batch: Batch) -> Result<()> {
        tracing::info!(sink = %self.label, size = batch.items.len(), "processing batch");
        for env in batch.items {
            tracing::info!(
                sink = %self.label,
                type_id = %env.type_id,
                "dispatched envelope"
            );
        }
        Ok(())
    }
}

pub struct LogSinkFactory;

#[async_trait]
impl SinkFactory for LogSinkFactory {
    fn kind(&self) -> &'static str {
        "log"
    }

    async fn create(&self, name: &str, config: Value) -> Result<Arc<dyn Sink>> {
        let cfg: LogSinkConfig = if config.is_null() {
            LogSinkConfig::default()
        } else {
            serde_json::from_value(config)?
        };
        let label = cfg.label.unwrap_or_else(|| name.to_string());
        Ok(Arc::new(LogSink::new(label)) as Arc<dyn Sink>)
    }
}

pub fn register(registry: &mut SinkRegistry) {
    registry.register_factory(Arc::new(LogSinkFactory));
}
