//! Helpers for building sink registries from configuration.

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use serde_json::Value;
use torii_core::{Sink, SinkRegistry};

pub use torii_sink_log::LogSinkConfig;
pub use torii_sink_sqlite::SqliteSinkConfig;

/// Kind string for the built-in log sink.
pub const KIND_LOG: &str = "log";
/// Kind string for the built-in SQLite sink.
pub const KIND_SQLITE: &str = "sqlite";

/// Build a [`SinkRegistry`] from configuration.
///
/// When wiring a new sink, declare a constant above and extend the match statement below.
pub async fn from_config(entries: &HashMap<String, Value>) -> Result<SinkRegistry> {
    let mut registry = SinkRegistry::new();
    tracing::info!(
        target: "torii_registry",
        count = entries.len(),
        "loading sinks from config"
    );

    for (name, value) in entries.iter() {
        let (kind, cfg_value) = super::extract_kind(name, value)?;
        tracing::debug!(
            target: "torii_registry",
            name,
            kind,
            "registering sink entry"
        );

        match kind.as_str() {
            KIND_LOG => {
                let cfg: LogSinkConfig = serde_json::from_value(cfg_value)?;
                let label = cfg.label.unwrap_or_else(|| name.to_string());
                let sink = Arc::new(torii_sink_log::LogSink::new(label)) as Arc<dyn Sink>;
                registry.register_sink(sink);
            }
            KIND_SQLITE => {
                let cfg: SqliteSinkConfig = serde_json::from_value(cfg_value)?;
                let label = cfg.label.clone().unwrap_or_else(|| name.to_string());
                let sink = torii_sink_sqlite::SqliteSink::connect(
                    label,
                    &cfg.database_url,
                    cfg.max_connections,
                )
                .await?;
                registry.register_sink(Arc::new(sink));
            }
            other => return Err(anyhow!("unknown sink kind: {other}")),
        }
    }

    Ok(registry)
}
