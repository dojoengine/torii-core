//! Helpers for building sink registries from configuration.

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use torii_core::{ContractConfig, FieldElement, Sink, SinkRegistry};

pub use torii_sink_log::LogSinkConfig;
pub use torii_sink_sqlite::SqliteSinkConfig;

/// Kind string for the built-in log sink.
pub const KIND_LOG: &str = "log";
/// Kind string for the built-in SQLite sink.
pub const KIND_SQLITE: &str = "sqlite";

/// Build a [`SinkRegistry`] from configuration.
///
/// When wiring a new sink, declare a constant above and extend the match statement below.
pub async fn from_config(
    entries: &HashMap<String, Value>,
    contracts: &HashMap<String, ContractConfig>,
) -> Result<SinkRegistry> {
    let mut registry = SinkRegistry::new();
    tracing::info!(
        target: "torii_registry",
        count = entries.len(),
        "loading sinks from config"
    );

    let contract_labels = build_contract_labels(contracts)?;

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
                    contract_labels.clone(),
                )
                .await?;
                registry.register_sink(Arc::new(sink));
            }
            other => return Err(anyhow!("unknown sink kind: {other}")),
        }
    }

    Ok(registry)
}

fn build_contract_labels(
    contracts: &HashMap<String, ContractConfig>,
) -> Result<HashMap<FieldElement, String>> {
    let mut by_address: HashMap<FieldElement, (String, String)> = HashMap::new();
    let mut used_labels: HashMap<String, String> = HashMap::new();

    for (name, cfg) in contracts {
        let normalized = sanitize_contract_label(name);
        if let Some(existing) = used_labels.insert(normalized.clone(), name.clone()) {
            anyhow::bail!(
                "contracts '{existing}' and '{name}' normalize to the same identifier '{normalized}'"
            );
        }

        let address = FieldElement::from_hex(&cfg.address)
            .with_context(|| format!("contract '{name}' has invalid address: {}", cfg.address))?;

        if let Some((_, existing_name)) = by_address.get(&address) {
            anyhow::bail!(
                "contracts '{existing_name}' and '{name}' both reference address {}",
                cfg.address
            );
        }

        by_address.insert(address, (normalized, name.clone()));
    }

    Ok(by_address
        .into_iter()
        .map(|(address, (label, _))| (address, label))
        .collect())
}

fn sanitize_contract_label(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut chars = name.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '(' | '<' => break,
            c if c.is_ascii_alphanumeric() || c == '_' => result.push(c.to_ascii_lowercase()),
            _ => result.push('_'),
        }
    }
    if result.is_empty() {
        "_".to_string()
    } else {
        result
    }
}
