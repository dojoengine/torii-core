use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use starknet::{core::types::EmittedEvent, macros::selector};
use torii_core::{
    Body, ContractBinding, ContractFilter, Decoder, DecoderFactory, DecoderFilter, DynEvent,
    Envelope, FieldElement,
};
use torii_types_erc721::{TransferV1, TRANSFER_ID};

const DECODER_NAME: &str = "erc721";

/// Cairo selector of the event to be processed by this decoder.
const TRANSFER_KEY: FieldElement = selector!("Transfer");

const CAIRO_EVENT_SELECTORS: [FieldElement; 1] = [TRANSFER_KEY];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Erc721DecoderConfig {}

struct Erc721Decoder {
    filter: DecoderFilter,
}

impl Erc721Decoder {
    fn from_config(_cfg: Erc721DecoderConfig, contracts: Vec<ContractBinding>) -> Result<Self> {
        if contracts.is_empty() {
            return Err(anyhow!(
                "erc721 decoder requires at least one contract binding"
            ));
        }

        let mut contract_addresses = HashSet::default();
        let mut selectors = HashSet::default();
        for selector in CAIRO_EVENT_SELECTORS {
            selectors.insert(selector);
        }

        let mut address_selectors = HashMap::new();
        for binding in contracts {
            contract_addresses.insert(binding.address);
            let entry =
                address_selectors
                    .entry(binding.address)
                    .or_insert_with(|| ContractFilter {
                        selectors: HashSet::new(),
                        deployed_at_block: binding.deployed_at_block,
                    });

            entry.selectors.extend(selectors.iter().copied());
            if let Some(block) = binding.deployed_at_block {
                entry.deployed_at_block = match entry.deployed_at_block {
                    Some(existing) => Some(existing.min(block)),
                    None => Some(block),
                };
            }
        }

        Ok(Self {
            filter: DecoderFilter {
                contract_addresses,
                selectors,
                address_selectors,
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

    async fn create(
        &self,
        config: Value,
        contracts: Vec<ContractBinding>,
    ) -> Result<Arc<dyn Decoder>> {
        let cfg: Erc721DecoderConfig = serde_json::from_value(config)?;
        let decoder = Erc721Decoder::from_config(cfg, contracts)?;
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
