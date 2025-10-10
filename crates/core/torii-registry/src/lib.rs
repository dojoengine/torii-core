//! Utilities for wiring decoders, fetchers, and sinks into registries from configuration.

pub mod decoders;
pub mod fetchers;
pub mod sinks;

use anyhow::{anyhow, Result};
use serde_json::Value;

/// Pull the `type` field out of a configuration table while returning the remainder.
fn extract_kind(name: &str, value: &Value) -> Result<(String, Value)> {
    let mut map = value
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("{name} config must be a table"))?;

    let kind = map
        .remove("type")
        .and_then(|v| v.as_str().map(|s| s.to_owned()))
        .unwrap_or_else(|| name.to_string());
    Ok((kind, Value::Object(map)))
}
