//! Core runtime primitives for Torii Starknet indexer.

use std::{collections::HashMap, path::Path};

use anyhow::Result;
use config::{Config, File};
use serde::Deserialize;
use serde_json::Value as JsonValue;

mod decoder;
mod event;
mod fetcher;
pub mod format;
mod registry;
mod runtime;
mod sink;
mod types;

pub use starknet::core::types::Felt as FieldElement;

pub use decoder::{Decoder, DecoderFactory};
pub use event::{DynEvent, Event};
pub use fetcher::{FetchOptions, Fetcher};
pub use registry::DecoderRegistry;
pub use runtime::{run_once_batch, run_once_batch_with_config, RuntimeConfig};
pub use sink::{Sink, SinkFactory, SinkRegistry};
pub use types::{
    type_id_from_url, Batch, Body, ContentType, ContractBinding, ContractFilter, DecoderFilter,
    Envelope, FetchOutcome, FetchPlan, FetcherCursor,
};

/// Torii configuration struct, usually expected in the a `torii.toml` file.
#[derive(Debug, Clone, Deserialize)]
pub struct ToriiConfig {
    #[serde(default)]
    pub fetcher: Option<JsonValue>,
    #[serde(default)]
    pub decoders: HashMap<String, JsonValue>,
    #[serde(default)]
    pub sinks: HashMap<String, JsonValue>,
    #[serde(default)]
    pub contracts: HashMap<String, ContractConfig>,
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

impl ToriiConfig {
    /// Loads the configuration from a file.
    pub fn new(config_path: &str) -> Result<Self> {
        let config = Config::builder()
            .add_source(File::from(Path::new(config_path)))
            .build()?;
        Ok(config.try_deserialize()?)
    }
}

/// Declares which decoders should observe a Starknet contract.
#[derive(Debug, Clone, Deserialize)]
pub struct ContractConfig {
    pub address: String,
    #[serde(default)]
    pub decoders: Vec<String>,
    #[serde(default)]
    pub deployed_at_block: Option<u64>,
}
