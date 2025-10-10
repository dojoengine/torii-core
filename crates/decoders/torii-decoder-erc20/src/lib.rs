use std::{collections::HashSet, convert::TryInto, sync::Arc};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use starknet::{core::types::EmittedEvent, macros::selector};
use torii_core::{Body, Decoder, DecoderFactory, DecoderFilter, Envelope, Event, FieldElement};
use torii_types_erc20::{TransferV1, TRANSFER_ID};

const DECODER_NAME: &str = "erc20";

/// Cairo selector of the event to be processed by this decoder.
const TRANSFER_KEY: FieldElement = selector!("Transfer");

const CAIRO_EVENT_SELECTORS: [FieldElement; 1] = [TRANSFER_KEY];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Erc20DecoderConfig {
    #[serde(default)]
    pub contracts: Vec<String>,
}

struct Erc20Decoder {
    filter: DecoderFilter,
}

impl Erc20Decoder {
    fn from_config(cfg: Erc20DecoderConfig) -> Result<Self> {
        if cfg.contracts.is_empty() {
            return Err(anyhow!(
                "erc20 decoder requires at least one contract address in config"
            ));
        }

        let mut contract_addresses = HashSet::default();
        for (idx, contract_hex) in cfg.contracts.into_iter().enumerate() {
            let address = FieldElement::from_hex(&contract_hex).with_context(|| {
                format!("invalid erc20 contract hex at index {idx}: {contract_hex}")
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
impl Decoder for Erc20Decoder {
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

pub struct Erc20DecoderFactory;

#[async_trait]
impl DecoderFactory for Erc20DecoderFactory {
    fn kind(&self) -> &'static str {
        DECODER_NAME
    }

    async fn create(&self, config: Value) -> Result<Arc<dyn Decoder>> {
        let cfg: Erc20DecoderConfig = serde_json::from_value(config)?;
        let decoder = Erc20Decoder::from_config(cfg)?;
        Ok(Arc::new(decoder))
    }
}

fn build_transfer(raw: &EmittedEvent) -> Envelope {
    let event = TransferV1 {
        contract: raw.from_address,
        from: *raw.keys.get(1).unwrap_or(&FieldElement::ZERO),
        to: *raw.keys.get(1).unwrap_or(&FieldElement::ZERO),
        amount: raw
            .data
            .first()
            .map(|value| {
                let bytes = value.to_bytes_be();
                let tail: [u8; 16] = bytes[16..]
                    .try_into()
                    .expect("slice with correct length for u128");
                u128::from_be_bytes(tail)
            })
            .unwrap_or(0),
    };

    Envelope {
        type_id: TRANSFER_ID,
        raw: Arc::new(raw.clone()),
        body: Body::Typed(Arc::new(event) as Arc<dyn Event>),
    }
}
