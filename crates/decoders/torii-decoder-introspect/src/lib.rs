use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use starknet::{core::types::EmittedEvent, macros::selector};
use torii_core::{Body, Decoder, DecoderFactory, DecoderFilter, Envelope, Event, FieldElement};
use torii_types_introspect::{DeclareTableV1, SetRecordV1, DECLARE_TABLE_ID, SET_RECORD_ID};

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
}

/// Implementation of the introspect decoder.
struct IntrospectDecoder {
    filter: DecoderFilter,
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
        let selector = event.keys.first().expect("event selector is required");

        // Felts are non structural types, so we can't use a match statement directly.
        // TODO: check if using hashmap would be better.
        if *selector == DECLARE_TABLE_SELECTOR {
            Ok(build_declare_table(event))
        } else if *selector == SET_RECORD_SELECTOR {
            Ok(build_set_record(event))
        } else {
            return Err(anyhow!("invalid event selector: {selector}"));
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

/// Builds a set record event.
fn build_set_record(raw: &EmittedEvent) -> Envelope {
    let event = SetRecordV1 {
        table: "erc20_balances".to_string(),
        cols: vec!["contract".into(), "owner".into(), "balance".into()],
        vals: vec![vec![raw.from_address], vec![raw.transaction_hash]],
        key_cols: vec!["contract".into(), "owner".into()],
    };

    Envelope {
        type_id: SET_RECORD_ID,
        raw: Arc::new(raw.clone()),
        body: Body::Typed(Arc::new(event) as Arc<dyn Event>),
    }
}
