//! Decoder traits and supporting abstractions.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use starknet::core::types::EmittedEvent;

use crate::{
    types::{ContractBinding, DecoderFilter, Envelope},
    FieldElement,
};

/// Runtime abstraction implemented by protocol decoders.
#[async_trait]
pub trait Decoder: Send + Sync {
    /// Human-readable identifier (used in logs and metrics).
    fn name(&self) -> &'static str;

    /// Fetch filter contributed by this decoder.
    fn filter(&self) -> &DecoderFilter;

    /// Cheap guard used before performing the full decode.
    fn matches(&self, ev: &EmittedEvent) -> bool;

    /// All type identifiers that this decoder may emit.
    fn type_ids(&self) -> &'static [u64];

    /// Decode a raw Starknet event into canonical envelopes.
    ///
    /// The decoder can expect the event to be valid, since the `matches` method is called before the `decode` method
    /// in the torii runtime.
    async fn decode(&self, raw: &EmittedEvent) -> Result<Envelope>;
}

/// Allows registering decoder implementations from configuration.
///
/// Since the async_trait requires a `Self` parameter, using the factory
/// allows easy initialization of the factory (usually without parameters),
/// and then creating the decoder.
///
/// # Example
/// ```
/// let factory = IntrospectDecoderFactory;
/// let decoder = factory.create(cfg_value.clone(), Vec::new()).await?;
/// ```
#[async_trait]
pub trait DecoderFactory: Send + Sync {
    /// Kind string used by configuration (`type = "…"`).
    fn kind(&self) -> &'static str;

    /// Construct a decoder from a configuration payload.
    async fn create(
        &self,
        config: Value,
        contracts: Vec<ContractBinding>,
    ) -> Result<Arc<dyn Decoder>>;
}
