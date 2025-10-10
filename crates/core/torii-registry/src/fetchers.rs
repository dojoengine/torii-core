//! Helpers for building fetchers from configuration.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde_json::Value;
use torii_core::Fetcher;
use torii_fetcher_jsonrpc::{JsonRpcFetcher, JsonRpcFetcherConfig};

/// Kind string for the built-in JSON-RPC fetcher.
pub const KIND_JSONRPC: &str = "jsonrpc";

/// Build a fetcher from configuration.
///
/// When adding new fetcher implementations, declare a new constant above and extend the match
/// statement below.
pub async fn from_config(value: &Value) -> Result<Arc<dyn Fetcher>> {
    let (kind, cfg_value) = super::extract_kind("fetcher", value)?;
    match kind.as_str() {
        KIND_JSONRPC => {
            let cfg: JsonRpcFetcherConfig = serde_json::from_value(cfg_value)?;
            tracing::info!(target: "torii_registry", kind, "building fetcher from config");
            let fetcher = JsonRpcFetcher::new(cfg)?;
            Ok(Arc::new(fetcher))
        }
        other => Err(anyhow!("unknown fetcher kind: {other}")),
    }
}
