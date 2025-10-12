use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use starknet::{core::types::EmittedEvent, macros::selector};
use torii_core::{Body, Decoder, DecoderFactory, DecoderFilter, DynEvent, Envelope, FieldElement};
use torii_types_erc721::{TransferV1, TRANSFER_ID};

const DECODER_NAME: &str = "erc721";

/// Cairo selector of the event to be processed by this decoder.
const TRANSFER_KEY: FieldElement = selector!("Transfer");

const CAIRO_EVENT_SELECTORS: [FieldElement; 1] = [TRANSFER_KEY];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Erc721DecoderConfig {
    pub contracts: Vec<String>,
}

struct Erc721Decoder {
    filter: DecoderFilter,
}

impl Erc721Decoder {
    fn from_config(cfg: Erc721DecoderConfig) -> Result<Self> {
        if cfg.contracts.is_empty() {
            return Err(anyhow!(
                "erc721 decoder requires at least one contract address in config"
            ));
        }

        let mut contract_addresses = HashSet::default();
        for (idx, contract_hex) in cfg.contracts.into_iter().enumerate() {
            let address = FieldElement::from_hex(&contract_hex).with_context(|| {
                format!("invalid erc721 contract hex at index {idx}: {contract_hex}")
            })?;
            contract_addresses.insert(address);
        }

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
impl Decoder for Erc721Decoder {
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

        ev.keys.first().map(|k| *k == TRANSFER_KEY).unwrap_or(false)
    }

    fn type_ids(&self) -> &'static [u64] {
        const IDS: [u64; 1] = [TRANSFER_ID];
        &IDS
    }

    async fn decode(&self, event: &EmittedEvent) -> Result<Envelope> {
        let selector = event.keys.first().expect("event selector is required");

        if *selector != TRANSFER_KEY {
            return Err(anyhow!("invalid event selector: {selector}"));
        }

        Ok(build_transfer(event))
    }
}

pub struct Erc721DecoderFactory;

#[async_trait]
impl DecoderFactory for Erc721DecoderFactory {
    fn kind(&self) -> &'static str {
        DECODER_NAME
    }

    async fn create(&self, config: Value) -> Result<Arc<dyn Decoder>> {
        let cfg: Erc721DecoderConfig = serde_json::from_value(config)?;
        let decoder = Erc721Decoder::from_config(cfg)?;
        Ok(Arc::new(decoder))
    }
}

fn build_transfer(raw: &EmittedEvent) -> Envelope {
    let event = TransferV1 {
        contract: raw.from_address,
        from: *raw.keys.get(1).unwrap_or(&FieldElement::ZERO),
        to: *raw.keys.get(2).unwrap_or(&FieldElement::ZERO),
        token_id: *raw.data.first().unwrap_or(&FieldElement::ZERO),
    };

    Envelope {
        type_id: TRANSFER_ID,
        raw: Arc::new(raw.clone()),
        body: Body::Typed(Arc::new(event) as Arc<dyn DynEvent>),
    }
}
