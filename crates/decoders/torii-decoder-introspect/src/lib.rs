use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use dojo_introspect_events::{
    selectors, DojoEvent, EventEmitted, EventRegistered, EventUpgraded, ModelUpgraded,
    StoreDelRecord, StoreSetRecord, StoreUpdateMember, StoreUpdateRecord,
};
use dojo_types_manager::{DojoManager, JsonStore};
use introspect_value::ToValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use starknet::{core::types::EmittedEvent, macros::selector};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use torii_core::{Body, Decoder, DecoderFactory, DecoderFilter, Envelope, Event, FieldElement};
use torii_types_introspect::{
    DeclareTableV1, UpdateRecordFieldsV1, DECLARE_TABLE_ID, SET_RECORD_ID, UPDATE_RECORD_FIELDS_ID,
};

const DECODER_NAME: &str = "introspect";

/// Cairo selectors of the events to be processed by this decoder.
const DECLARE_TABLE_SELECTOR: FieldElement = selector!("DeclareTable");
const SET_RECORD_SELECTOR: FieldElement = selector!("SetRecord");

const CAIRO_EVENT_SELECTORS: [FieldElement; 2] = [DECLARE_TABLE_SELECTOR, SET_RECORD_SELECTOR];

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
struct IntrospectDecoder {
    filter: Option<DecoderFilter>,
    manager: DojoManager<JsonStore>,
}

impl IntrospectDecoder {
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
impl Decoder for IntrospectDecoder {
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
        if selector == selectors::ModelRegistered {
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

    async fn create(&self, config: Value) -> Result<Arc<dyn Decoder>> {
        let cfg: IntrospectDecoderConfig = serde_json::from_value(config)?;
        let decoder = IntrospectDecoder::from_config(&cfg)?;
        Ok(Arc::new(decoder))
    }
}

// @ben: The functions below must be organized in other files/modules to keep this lib.rs clean.
// Also, here I am using placeholder types, but this is where we want the `TypeDef` and parsing to happen.

/// Builds a declare table event.
fn build_declare_table(raw: &EmittedEvent) -> Envelope {
    let event = DeclareTableV1 {
        name: "erc20_balances".to_string(),
        fields: vec![
            ("contract".into(), "felt252".into()),
            ("owner".into(), "felt252".into()),
            ("balance".into(), "u128".into()),
        ],
        primary_key: vec!["contract".into(), "owner".into()],
    };

    Envelope {
        type_id: DECLARE_TABLE_ID,
        raw: Arc::new(raw.clone()),
        body: Body::Typed(Arc::new(event) as Arc<dyn Event>),
    }
}

trait DojoEventBuilder {
    fn build_model_registered(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_model_upgraded(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_event_registered(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_event_upgraded(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_set_record(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_update_record(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_update_member(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_del_record(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_emit_event(&self, raw: &EmittedEvent) -> Result<Envelope>;
}

impl DojoEventBuilder for IntrospectDecoder {
    fn build_set_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = StoreSetRecord::new(raw.keys[1..].to_vec(), raw.data.clone())
            .ok_or_else(|| anyhow!("failed to decode StoreSetRecord event"))?;
        let table = self
            .manager
            .get_table(event.selector)
            .ok_or_else(|| anyhow!("no table found for selector {}", event.selector.to_string()))?;
        let fields = table
            .parse_key_values(event.keys, event.values)
            .ok_or_else(|| {
                anyhow!(
                    "failed to parse key/values for table {}",
                    table.name.clone()
                )
            })?;
        let data = UpdateRecordFieldsV1::new(table.id, table.name.clone(), event.entity_id, fields);
        Ok(Envelope {
            type_id: UPDATE_RECORD_FIELDS_ID,
            raw: Arc::new(raw.clone()),
            body: Body::Typed(Arc::new(data) as Arc<dyn Event>),
        })
    }

    fn build_update_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = StoreUpdateRecord::new(raw.keys[1..].to_vec(), raw.data.clone())
            .ok_or_else(|| anyhow!("failed to decode StoreUpdateRecord event"))?;
        let table = self
            .manager
            .get_table(event.selector)
            .ok_or_else(|| anyhow!("no table found for selector {}", event.selector.to_string()))?;
        let fields = table
            .parse_values(event.values)
            .ok_or_else(|| anyhow!("failed to parse values for table {}", table.name.clone()))?;
        let data = UpdateRecordFieldsV1::new(table.id, table.name.clone(), event.entity_id, fields);
        Ok(Envelope {
            type_id: UPDATE_RECORD_FIELDS_ID,
            raw: Arc::new(raw.clone()),
            body: Body::Typed(Arc::new(data) as Arc<dyn Event>),
        })
    }

    fn build_update_member(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = StoreUpdateMember::new(raw.keys[1..].to_vec(), raw.data.clone())
            .ok_or_else(|| anyhow!("failed to decode StoreUpdateMember event"))?;
        let table = self
            .manager
            .get_table(event.selector)
            .ok_or_else(|| anyhow!("no table found for selector {}", event.selector.to_string()))?;
        let value = table
            .parse_field(event.selector, event.values)
            .ok_or_else(|| anyhow!("failed to parse field for table {}", table.name.clone()))?;
        let data =
            UpdateRecordFieldsV1::new(table.id, table.name.clone(), event.entity_id, vec![value]);
        Ok(Envelope {
            type_id: UPDATE_RECORD_FIELDS_ID,
            raw: Arc::new(raw.clone()),
            body: Body::Typed(Arc::new(data) as Arc<dyn Event>),
        })
    }

    fn build_del_record(&self, raw: &EmittedEvent) -> Result<Envelope> {}
}
