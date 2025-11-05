//! Fundamental data structures shared by decoders, sinks, and the runtime.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use starknet::core::types::EmittedEvent;

use crate::FieldElement;

/// Identifies how the envelope body is encoded when `Body::Bytes` is used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ContentType {
    /// JSON-encoded bytes.
    Json = 0,
    /// Binary data (exact format specified by the producer).
    Bincode = 1,
}

/// Envelope body payload.
#[derive(Clone)]
pub enum Body {
    /// Zero-copy typed payload (preferred for in-process sinks).
    ///
    /// `dyn` is preferred over an enum since `Arc`is required to
    /// be shared across threads (hence allocated on the heap).
    Typed(Arc<dyn crate::DynEvent>),
    /// Serialized payload (for plugins/archives).
    Bytes {
        /// Format for the serialized bytes.
        content_type: ContentType,
        /// Raw data.
        data: Bytes,
    },
    /// Indicates the sink should look at the raw Starknet event only.
    Empty,
}

/// Canonical representation for an indexed Starknet event.
///
/// For now, the raw event is still passed to have all the metadata
/// like block number block hash etc...
/// This might change in the future.
///
/// TODO: We also want distributed tracing, so we may generate a unique ID for the envelope
/// to ease the debugging process.
#[derive(Clone)]
pub struct Envelope {
    pub type_id: u64,
    pub raw: Arc<EmittedEvent>,
    pub body: Body,
}

impl Envelope {
    /// Attempt to view the body as a typed event of `E`.
    pub fn downcast<E: crate::Event>(&self) -> Option<&E> {
        match &self.body {
            Body::Typed(ev) if E::TYPE_ID == self.type_id => ev.as_any().downcast_ref::<E>(),
            _ => None,
        }
    }

    /// Return the serialized payload if present.
    pub fn bytes(&self) -> Option<(ContentType, &Bytes)> {
        match &self.body {
            Body::Bytes { content_type, data } => Some((*content_type, data)),
            _ => None,
        }
    }
}

/// Batch of envelopes broadcast to sinks.
#[derive(Clone, Default)]
pub struct Batch {
    pub items: Vec<Envelope>,
}

/// Derive a canonical 64-bit type identifier using the FNV-1a hash.
///
/// This function is `const` so the resulting ID can be computed at compile time from a type URL.
/// If one day we reach millions of types, we may need to use something like XXH3, but this may
/// not be `const` and once_cell::sync::Lazy may be needed.
pub const fn type_id_from_url(url: &str) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x00000100000001B3;

    let bytes = url.as_bytes();
    let mut hash = OFFSET;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(PRIME);
        i += 1;
    }
    hash
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ContractFilter {
    pub selectors: HashSet<FieldElement>,
    pub deployed_at_block: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct ContractBinding {
    pub address: FieldElement,
    pub deployed_at_block: Option<u64>,
}

/// Decoder supplied Starknet fetch filters.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DecoderFilter {
    pub contract_addresses: HashSet<FieldElement>,
    pub selectors: HashSet<FieldElement>,
    pub address_selectors: HashMap<FieldElement, ContractFilter>,
}

/// Union of all active decoder filters.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FetchPlan {
    pub contract_addresses: HashSet<FieldElement>,
    pub selectors: HashSet<FieldElement>,
    pub address_selectors: HashMap<FieldElement, ContractFilter>,
}

impl FetchPlan {
    /// Build a plan from an iterator of filters.
    pub fn union(filters: impl IntoIterator<Item = DecoderFilter>) -> Self {
        let mut plan = FetchPlan::default();
        for filter in filters {
            plan.contract_addresses
                .extend(filter.contract_addresses.into_iter());
            plan.selectors.extend(filter.selectors.into_iter());
            for (address, contract_filter) in filter.address_selectors.into_iter() {
                let entry = plan
                    .address_selectors
                    .entry(address)
                    .or_insert_with(ContractFilter::default);

                entry
                    .selectors
                    .extend(contract_filter.selectors.into_iter());

                entry.deployed_at_block =
                    match (entry.deployed_at_block, contract_filter.deployed_at_block) {
                        (Some(existing), Some(new)) => Some(existing.min(new)),
                        (None, Some(new)) => Some(new),
                        (Some(existing), None) => Some(existing),
                        (None, None) => None,
                    };
            }
        }
        plan
    }
}

/// Opaque cursor returned by fetchers to indicate progress between paginated requests.
///
/// Use a HashMap where the key is the address of the contract used in the filters of the fetcher.
/// And the value is the continuation token.
///
/// If future fetchers are allowing filtering with multiple addresses, then the Felt
/// could be a hash of the addresses or something that uniquely identifies the filter.
#[derive(Clone, Debug, Default)]
pub struct FetcherCursor {
    pub continuations: HashMap<FieldElement, Option<String>>,
}

impl FetcherCursor {
    pub fn new(continuations: HashMap<FieldElement, Option<String>>) -> Self {
        Self { continuations }
    }

    pub fn get_continuation(&self, address: &FieldElement) -> Option<String> {
        self.continuations.get(address).and_then(|c| c.clone())
    }

    pub fn set_continuation(&mut self, address: &FieldElement, continuation: Option<String>) {
        self.continuations.insert(address.clone(), continuation);
    }

    pub fn has_more(&self) -> bool {
        self.continuations
            .values()
            .any(|continuation| continuation.is_some())
    }
}

/// Result of a fetcher invocation, including the next cursor state.
#[derive(Clone, Debug, Default)]
pub struct FetchOutcome {
    pub events: Vec<EmittedEvent>,
    pub cursor: FetcherCursor,
}

impl FetchOutcome {
    pub fn new(events: Vec<EmittedEvent>, cursor: FetcherCursor) -> Self {
        Self { events, cursor }
    }
}
