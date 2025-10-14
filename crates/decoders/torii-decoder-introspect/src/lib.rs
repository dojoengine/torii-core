use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dojo_introspect_events::{
    DojoEvent, EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    StoreDelRecord, StoreSetRecord, StoreUpdateMember, StoreUpdateRecord,
};
use dojo_types_manager::{DojoManager, JsonStore};
use serde::{Deserialize, Serialize};
use starknet::{
    core::types::EmittedEvent,
    providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider, Url},
};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use torii_core::{Decoder, DecoderFactory, DecoderFilter, Envelope, Event, FieldElement};
use torii_types_introspect::{DeclareTableV1, DeleteRecordsV1, UpdateRecordFieldsV1};
mod builders;
use builders::DojoEventBuilder;

const DECODER_NAME: &str = "introspect";
const DOJO_CAIRO_EVENT_SELECTORS: [FieldElement; 8] = [
    ModelRegistered::SELECTOR,
    ModelUpgraded::SELECTOR,
    EventRegistered::SELECTOR,
    EventUpgraded::SELECTOR,
    StoreSetRecord::SELECTOR,
    StoreUpdateRecord::SELECTOR,
    StoreUpdateMember::SELECTOR,
    StoreDelRecord::SELECTOR,
];

const DOJO_EVENT_IDS: [u64; 3] = [
    DeclareTableV1::TYPE_ID,
    DeleteRecordsV1::TYPE_ID,
    UpdateRecordFieldsV1::TYPE_ID,
];

/// Cairo selectors of the events to be processed by this decoder.

/// Configuration for the introspect decoder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntrospectDecoderConfig {
    pub store_path: PathBuf,
    pub rpc_url: Url,
}

/// Implementation of the introspect decoder.
struct IntrospectDecoder<P>
where
    P: Provider,
{
    pub filter: DecoderFilter,
    pub manager: DojoManager<JsonStore>,
    pub provider: P,
}

impl IntrospectDecoder<JsonRpcClient<HttpTransport>> {
    /// Builds the decoder from a configuration.
    fn from_config(cfg: IntrospectDecoderConfig, contracts: Vec<FieldElement>) -> Result<Self> {
        if contracts.is_empty() {
            return Err(anyhow!(
                "introspect decoder requires at least one contract binding"
            ));
        }

        let mut contract_addresses = HashSet::default();
        for address in contracts {
            contract_addresses.insert(address);
        }

        let mut selectors = HashSet::default();
        for selector in DOJO_CAIRO_EVENT_SELECTORS {
            selectors.insert(selector);
        }
        let mut address_selectors = HashMap::new();
        for address in &contract_addresses {
            address_selectors
                .entry(*address)
                .or_insert_with(HashSet::new)
                .extend(selectors.iter().copied());
        }
        let provider = JsonRpcClient::new(HttpTransport::new(cfg.rpc_url));

        let store = JsonStore::new(cfg.store_path.as_path());

        let manager = DojoManager::new(store)?;
        let filter = DecoderFilter {
            contract_addresses,
            selectors,
            address_selectors,
        };

        Ok(Self {
            filter,
            manager,
            provider,
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
        &DOJO_EVENT_IDS
    }

    async fn decode(&self, event: &EmittedEvent) -> Result<Envelope> {
        let selector = *event.keys.first().expect("event selector is required");
        // Felts are non structural types, so we can't use a match statement directly.
        // TODO: check if using hashmap would be better.
        let result = if selector == ModelRegistered::SELECTOR {
            self.build_model_registered(event).await
        } else if selector == ModelUpgraded::SELECTOR {
            self.build_model_upgraded(event).await
        } else if selector == EventRegistered::SELECTOR {
            self.build_event_registered(event).await
        } else if selector == EventUpgraded::SELECTOR {
            self.build_event_upgraded(event).await
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
            return Err(anyhow!("invalid event selector: {selector}"));
        };
        match result {
            Ok(envelope) => Ok(envelope),
            Err(e) => Err(anyhow!("introspect decoder failed to decode event: {e}")),
        }
    }
}

pub struct IntrospectDecoderFactory;

#[async_trait]
impl DecoderFactory for IntrospectDecoderFactory {
    fn kind(&self) -> &'static str {
        DECODER_NAME
    }

    async fn create(
        &self,
        config: serde_json::Value,
        contracts: Vec<FieldElement>,
    ) -> Result<Arc<dyn Decoder>> {
        let cfg: IntrospectDecoderConfig = serde_json::from_value(config)?;
        let decoder = IntrospectDecoder::from_config(cfg, contracts)?;
        Ok(Arc::new(decoder))
    }
}
