//! Fundamental data structures shared by decoders, sinks, and the runtime.

use std::{collections::HashSet, sync::Arc};

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
    Typed(Arc<dyn crate::Event>),
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
    pub fn downcast<E: crate::StaticEvent>(&self) -> Option<&E> {
        match &self.body {
            Body::Typed(ev) if E::static_type_id() == self.type_id => {
                ev.as_any().downcast_ref::<E>()
            }
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

/// Decoder supplied Starknet fetch filters.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DecoderFilter {
    pub contract_addresses: HashSet<FieldElement>,
    pub selectors: HashSet<FieldElement>,
}

/// Union of all active decoder filters.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FetchPlan {
    pub contract_addresses: HashSet<FieldElement>,
    pub selectors: HashSet<FieldElement>,
}

impl FetchPlan {
    /// Build a plan from an iterator of filters.
    pub fn union(filters: impl IntoIterator<Item = DecoderFilter>) -> Self {
        let mut plan = FetchPlan::default();
        for filter in filters {
            plan.contract_addresses
                .extend(filter.contract_addresses.into_iter());
            plan.selectors.extend(filter.selectors.into_iter());
        }
        plan
    }
}
