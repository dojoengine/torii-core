//! Helpers for wiring decoder implementations into a [`DecoderRegistry`](torii_core::DecoderRegistry).

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde_json::Value;
use torii_core::{DecoderFactory, DecoderRegistry};

pub use torii_decoder_erc20::Erc20DecoderConfig;
pub use torii_decoder_erc20::Erc20DecoderFactory;
pub use torii_decoder_erc721::Erc721DecoderConfig;
pub use torii_decoder_erc721::Erc721DecoderFactory;
pub use torii_decoder_introspect::IntrospectDecoderConfig;
pub use torii_decoder_introspect::IntrospectDecoderFactory;

/// Kind string used for introspect decoders in configuration.
pub const KIND_INTROSPECT: &str = "introspect";
/// Kind string used for ERC-20 decoders in configuration.
pub const KIND_ERC20: &str = "erc20";
/// Kind string used for ERC-721 decoders in configuration.
pub const KIND_ERC721: &str = "erc721";

/// Build a [`DecoderRegistry`] from a configuration map.
///
/// When adding new decoders, define a constant for the new kind above and extend the match
/// statement below. This keeps the list of supported kinds easy to audit.
pub async fn from_config(entries: &HashMap<String, Value>) -> Result<DecoderRegistry> {
    let mut registry = DecoderRegistry::new();
    tracing::info!(
        target: "torii_registry",
        count = entries.len(),
        "loading decoders from config"
    );
    for (name, value) in entries.iter() {
        let (kind, cfg_value) = super::extract_kind(name, value)?;
        tracing::debug!(
            target: "torii_registry",
            name,
            kind,
            "registering decoder entry"
        );

        match kind.as_str() {
            KIND_INTROSPECT => {
                let factory = IntrospectDecoderFactory;
                let decoder = factory.create(cfg_value.clone()).await?;
                registry.register(decoder)?;
            }
            KIND_ERC20 => {
                let factory = Erc20DecoderFactory;
                let decoder = factory.create(cfg_value.clone()).await?;
                registry.register(decoder)?;
            }
            KIND_ERC721 => {
                let factory = Erc721DecoderFactory;
                let decoder = factory.create(cfg_value.clone()).await?;
                registry.register(decoder)?;
            }
            other => return Err(anyhow!("unknown decoder kind: {other}")),
        }
    }

    Ok(registry)
}
