pub mod context;

use async_trait::async_trait;
use starknet::core::types::EmittedEvent;

use super::envelope::Envelope;

pub use context::DecoderContext;

/// Decoder transforms blockchain events into typed envelopes
///
/// # Design
/// Decoders are responsible for:
/// - Examining raw blockchain events.
/// - Filtering events they're interested in (by contract address, event keys, etc.).
/// - Creating typed `Envelope` wrappers with specific `TypeId`s.
/// - **Populating envelope metadata** with event-specific data for sink access.
/// - Skipping events they don't recognize.
///
/// # Multi-Decoder Pattern
/// Multiple decoders can process the same events:
/// - One event may produce multiple envelopes (different sinks).
/// - One event may be skipped by all decoders (no envelopes).
/// - Each decoder is typically associated with a specific sink.
///
/// # Metadata Best Practice
///
/// **Important**: If sinks need access to original event data (like block number, transaction hash,
/// contract address, etc.), the decoder should add this information to the envelope's **metadata** (or body if it's relevant).
///
/// Why? Sinks should avoid iterating through `batch.events` (O(n) operation). Instead:
/// - Decoder extracts relevant event fields → envelope metadata
/// - Sink reads metadata → O(1) access
///
/// For block/transaction context, sinks can use the enriched batch HashMaps:
/// - `batch.blocks[&block_number]` - O(1) lookup for block timestamp, hash, etc.
/// - `batch.transactions[&tx_hash]` - O(1) lookup for sender, calldata, etc.
///
/// # Example
///
/// ```rust,ignore
/// use crate::etl::decoder::Decoder;
/// use crate::etl::envelope::{Envelope, TypeId, TypedBody};
/// use starknet::core::types::EmittedEvent;
/// use async_trait::async_trait;
/// use std::collections::HashMap;
///
/// pub struct MyDecoder {
///     contract_filters: Vec<starknet::core::types::Felt>,
/// }
///
/// impl MyDecoder {
///     fn is_interested(&self, event: &EmittedEvent) -> bool {
///         // Filter logic here
///         true
///     }
/// }
///
/// #[async_trait]
/// impl Decoder for MyDecoder {
///     fn decoder_name(&self) -> &str {
///         "my_decoder"
///     }
///
///     async fn decode_event(&self, event: &EmittedEvent) -> anyhow::Result<Vec<Envelope>> {
///         if !self.is_interested(event) {
///             return Ok(Vec::new());
///         }
///
///         // Extract only the data you need from the event
///         let body = MyEventType { /* decoded fields */ };
///
///         // Add event-specific data to metadata for sink access
///         let mut metadata = HashMap::new();
///         metadata.insert("block_number".to_string(),
///             event.block_number.unwrap_or(0).to_string());
///         metadata.insert("from_address".to_string(),
///             format!("{:#x}", event.from_address));
///
///         Ok(vec![Envelope::new("my_key", Box::new(body), metadata)])
///     }
/// }
/// ```
///
/// # Performance
///
/// **Zero-copy filtering**: Decoders receive `&EmittedEvent` (reference), allowing:
/// - Process events without cloning.
/// - Only extract data for events the decoder is interested in.
/// - Multiple decoders process the same events without memory duplication.
#[async_trait]
pub trait Decoder: Send + Sync {
    /// Returns the unique name of this decoder
    ///
    /// This name is used to generate a deterministic `DecoderId` that identifies
    /// this decoder in the contract registry. The name should be:
    /// - Unique across all decoders in the system
    /// - Stable (never change it, or contract mappings will break)
    /// - Lowercase and descriptive (e.g., "erc20", "erc721", "custom_game_events")
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// fn decoder_name(&self) -> &str {
    ///     "erc20" // DecoderId will be hash("erc20")
    /// }
    /// ```
    fn decoder_name(&self) -> &str;

    /// Decode a single event into typed envelopes
    ///
    /// This is the primary method that decoders should implement.
    /// Returns an empty Vec if the decoder is not interested in this event.
    ///
    /// # Arguments
    /// * `event` - Reference to the event to decode.
    ///
    /// # Returns
    /// Vector of envelopes produced from this event (empty if not interested).
    async fn decode_event(&self, event: &EmittedEvent) -> anyhow::Result<Vec<Envelope>>;

    /// Decode multiple events into typed envelopes (convenience method)
    ///
    /// Default implementation calls `decode_event` for each event.
    /// Override only if batch processing provides meaningful optimization.
    ///
    /// # Arguments
    /// * `events` - Reference to event slice.
    ///
    /// # Returns
    /// Vector of all envelopes produced from all events.
    async fn decode(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();
        for event in events {
            let envelopes = self.decode_event(event).await?;
            all_envelopes.extend(envelopes);
        }
        Ok(all_envelopes)
    }
}
