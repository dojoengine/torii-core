use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use dojo_introspect_events::{
    DojoEvent, EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    StoreDelRecord, StoreSetRecord, StoreUpdateMember, StoreUpdateRecord,
};
use dojo_types_manager::{DojoManager, JsonStore};
use introspect_value::{Field, Value};
use serde::{Deserialize, Serialize};
use starknet::{core::types::EmittedEvent, providers::Provider};
use starknet_types_core::felt::Felt;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use torii_core::{Decoder, DecoderFactory, DecoderFilter, Envelope, FieldElement};
use torii_types_introspect::DECLARE_TABLE_ID;
mod builders;
use builders::DojoEventBuilder;

const DECODER_NAME: &str = "introspect";

/// Cairo selectors of the events to be processed by this decoder.

/// Configuration for the introspect decoder.
///
/// Currently, the only configuration is the list of contract addresses
/// to monitor.
/// We could potentially add all the selector configurable, to finely tune
/// which events to capture. But this may come later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntrospectDecoderConfig {
    #[serde(default)]
    pub contracts: Vec<String>,
    pub store_path: PathBuf,
}

/// Implementation of the introspect decoder.
struct IntrospectDecoder<P>
where
    P: Provider,
{
    pub filter: Option<DecoderFilter>,
    pub manager: DojoManager<JsonStore>,
    pub provider: P,
}

impl<P> IntrospectDecoder<P>
where
    P: Provider,
{
    /// Builds the decoder from a configuration.
    fn from_config(cfg: &IntrospectDecoderConfig) -> Result<Self> {
        if cfg.contracts.is_empty() {
            return Err(anyhow!(
                "introspect decoder requires at least one contract address in config"
            ));
        }

        let mut contract_addresses = HashSet::default();
        for (idx, contract_hex) in cfg.contracts.iter().enumerate() {
            let address = FieldElement::from_hex(contract_hex).with_context(|| {
                format!("invalid introspect contract address hex at index {idx}: {contract_hex}")
            })?;
            contract_addresses.insert(address);
        }

        // @ben: here, you can hardcode all the selectors to be processed by this decoder.
        // As mentioned in the comment in the `IntrospectDecoderConfig`, we may have all of this
        // in the config. But this is IMHO too heavy for the user to actually configure (at least for now).
        let mut selectors = HashSet::default();
        for selector in CAIRO_EVENT_SELECTORS {
            selectors.insert(selector);
        }

        Ok(Self {
            filter: DecoderFilter {
                contract_addresses,
                selectors,
            },
            manager: DojoManager::new(cfg.store_path),
        })
    }
}

#[async_trait]
impl<P> Decoder for IntrospectDecoder<P>
where
    P: Provider + Sync + Send,
{
    fn name(&self) -> &'static str {
        DECODER_NAME
    }

    fn filter(&self) -> &DecoderFilter {
        &self.filter
    }

    fn matches(&self, ev: &EmittedEvent) -> bool {
        if !self.filter.contract_addresses.is_empty()
            && !self.filter.contract_addresses.contains(&ev.from_address)
        {
            return false;
        }

        ev.keys
            .first()
            .map(|key| self.filter.selectors.contains(key))
            .unwrap_or(false)
    }

    fn type_ids(&self) -> &'static [u64] {
        const IDS: [u64; 2] = [DECLARE_TABLE_ID, SET_RECORD_ID];
        &IDS
    }

    async fn decode(&self, event: &EmittedEvent) -> Result<Envelope> {
        let selector = *event.keys.first().expect("event selector is required");
        // Felts are non structural types, so we can't use a match statement directly.
        // TODO: check if using hashmap would be better.
        if selector == ModelRegistered::SELECTOR {
            self.build_model_registered(event)
        } else if selector == ModelUpgraded::SELECTOR {
            self.build_model_upgraded(event)
        } else if selector == EventRegistered::SELECTOR {
            self.build_event_registered(event)
        } else if selector == EventUpgraded::SELECTOR {
            self.build_event_upgraded(event)
        } else if selector == StoreSetRecord::SELECTOR {
            self.build_set_record(event)
        } else if selector == StoreUpdateRecord::SELECTOR {
            self.build_update_record(event)
        } else if selector == StoreUpdateMember::SELECTOR {
            self.build_update_member(event)
        } else if selector == StoreDelRecord::SELECTOR {
            self.build_del_record(event)
        } else if selector == EventEmitted::SELECTOR {
            self.build_emit_event(event)
        } else {
            Err(anyhow!("invalid event selector: {selector}"))
        }
    }
}

pub struct IntrospectDecoderFactory;

#[async_trait]
impl DecoderFactory for IntrospectDecoderFactory {
    fn kind(&self) -> &'static str {
        DECODER_NAME
    }

    async fn create(&self, config: serde_json::Value) -> Result<Arc<dyn Decoder>> {
        let cfg: IntrospectDecoderConfig = serde_json::from_value(config)?;
        let decoder = IntrospectDecoder::from_config(&cfg)?;
        Ok(Arc::new(decoder))
    }
}
