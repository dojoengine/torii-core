//! Helpers for wiring decoder implementations into a [`DecoderRegistry`](torii_core::DecoderRegistry).

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use torii_core::{ContractConfig, DecoderFactory, DecoderRegistry, FieldElement};

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
pub async fn from_config(
    entries: &HashMap<String, Value>,
    contracts: &HashMap<String, ContractConfig>,
) -> Result<DecoderRegistry> {
    let mut registry = DecoderRegistry::new();
    tracing::info!(
        target: "torii_registry",
        count = entries.len(),
        "loading decoders from config"
    );

    let mut prepared = Vec::new();
    let mut enabled_kinds = HashSet::new();

    for (name, value) in entries.iter() {
        let (kind, cfg_value) = super::extract_kind(name, value)?;
        enabled_kinds.insert(kind.clone());
        prepared.push((name.clone(), kind, cfg_value));
    }

    let contracts_by_decoder = build_contract_index(contracts, &enabled_kinds)
        .context("invalid contract configuration")?;

    for (name, kind, cfg_value) in prepared {
        tracing::debug!(
            target: "torii_registry",
            name = %name,
            kind = %kind,
            "registering decoder entry"
        );

        let merged_cfg = merge_contracts(cfg_value, contracts_by_decoder.get(kind.as_str()))
            .with_context(|| format!("failed to build config for decoder '{name}'"))?;

        dbg!(&merged_cfg);

        match kind.as_str() {
            KIND_INTROSPECT => {
                let factory = IntrospectDecoderFactory;
                let decoder = factory.create(merged_cfg).await?;
                registry.register(decoder)?;
            }
            KIND_ERC20 => {
                let factory = Erc20DecoderFactory;
                let decoder = factory.create(merged_cfg).await?;
                registry.register(decoder)?;
            }
            KIND_ERC721 => {
                let factory = Erc721DecoderFactory;
                let decoder = factory.create(merged_cfg).await?;
                registry.register(decoder)?;
            }
            other => return Err(anyhow!("unknown decoder kind: {other}")),
        }
    }

    Ok(registry)
}

fn build_contract_index(
    contracts: &HashMap<String, ContractConfig>,
    enabled_kinds: &HashSet<String>,
) -> Result<HashMap<String, Vec<String>>> {
    let mut by_decoder: HashMap<String, Vec<String>> = HashMap::new();

    for (name, binding) in contracts {
        if binding.decoders.is_empty() {
            anyhow::bail!("contract '{name}' must specify at least one decoder");
        }

        let felt = FieldElement::from_hex(&binding.address).with_context(|| {
            format!("contract '{name}' has invalid address: {}", binding.address)
        })?;
        let canonical = format!("{:#066x}", felt);

        for decoder in &binding.decoders {
            if !enabled_kinds.contains(decoder) {
                anyhow::bail!(
                    "contract '{name}' references decoder '{decoder}' but it is not enabled"
                );
            }

            let entry = by_decoder.entry(decoder.clone()).or_default();
            if !entry.iter().any(|existing| existing == &canonical) {
                entry.push(canonical.clone());
            }
        }
    }

    Ok(by_decoder)
}

fn merge_contracts(cfg_value: Value, contracts: Option<&Vec<String>>) -> Result<Value> {
    if let Some(addresses) = contracts {
        let mut map = cfg_value
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow!("decoder config must be a table"))?;

        if addresses.is_empty() {
            return Ok(Value::Object(map));
        }

        match map.get_mut("contracts") {
            Some(Value::Array(existing)) => {
                let mut known = existing
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_owned))
                    .collect::<HashSet<_>>();
                for addr in addresses {
                    if known.insert(addr.clone()) {
                        existing.push(Value::String(addr.clone()));
                    }
                }
            }
            Some(_) => {
                anyhow::bail!("decoder contracts config must be an array of strings");
            }
            None => {
                map.insert(
                    "contracts".to_string(),
                    Value::Array(
                        addresses
                            .iter()
                            .cloned()
                            .map(Value::String)
                            .collect::<Vec<_>>(),
                    ),
                );
            }
        }

        Ok(Value::Object(map))
    } else {
        Ok(cfg_value)
    }
}
