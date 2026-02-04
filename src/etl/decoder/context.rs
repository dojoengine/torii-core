//! DecoderContext manages multiple decoders with contract filtering.
//!
//! The DecoderContext routes events to all registered decoders, optionally
//! filtering by contract address (whitelist/blacklist).
//!
//! # Design
//!
//! - Decoders are identified by their `decoder_name()` (hashed to DecoderId)
//! - All events are passed to all decoders (no identification caching)
//! - Each decoder decides if it can decode an event (structural filtering)
//! - Optional contract filtering (whitelist/blacklist) for efficiency
//! - Deterministic ordering: decoders are always called in sorted DecoderId order

use async_trait::async_trait;
use starknet::core::types::EmittedEvent;
use std::collections::HashMap;
use std::sync::Arc;

use super::{ContractFilter, Decoder, DecoderId};
use crate::etl::engine_db::EngineDb;
use crate::etl::envelope::Envelope;

/// DecoderContext manages multiple decoders with contract filtering.
///
/// Routes all events to all decoders, with optional contract filtering for efficiency.
pub struct DecoderContext {
    /// Decoders indexed by their ID (hash of name)
    decoders: HashMap<DecoderId, Arc<dyn Decoder>>,

    /// EngineDb for ETL state persistence (cursor, not contract mappings)
    engine_db: Arc<EngineDb>,

    /// Contract filter (whitelist/blacklist/none)
    contract_filter: ContractFilter,
}

impl DecoderContext {
    /// Create a new DecoderContext
    ///
    /// # Arguments
    ///
    /// * `decoders` - List of decoders to manage
    /// * `engine_db` - Database for ETL state persistence
    /// * `contract_filter` - Whitelist/blacklist/none filter
    pub fn new(
        decoders: Vec<Arc<dyn Decoder>>,
        engine_db: Arc<EngineDb>,
        contract_filter: ContractFilter,
    ) -> Self {
        let decoder_map = Self::build_decoder_map(&decoders);

        let filter_desc = if contract_filter.mappings.is_empty() && contract_filter.blacklist.is_empty() {
            "none (auto-discovery for all contracts)".to_string()
        } else {
            let mut parts = Vec::new();
            if !contract_filter.mappings.is_empty() {
                parts.push(format!("{} mapped contracts", contract_filter.mappings.len()));
            }
            if !contract_filter.blacklist.is_empty() {
                parts.push(format!("{} blacklisted contracts", contract_filter.blacklist.len()));
            }
            parts.join(", ")
        };

        tracing::info!(
            target: "torii::etl::decoder_context",
            "Initialized DecoderContext with {} decoders and filter: {}",
            decoder_map.len(),
            filter_desc
        );

        Self {
            decoders: decoder_map,
            engine_db,
            contract_filter,
        }
    }

    /// Build decoder map from list (helper for constructor)
    fn build_decoder_map(decoders: &[Arc<dyn Decoder>]) -> HashMap<DecoderId, Arc<dyn Decoder>> {
        let mut decoder_map = HashMap::new();

        for decoder in decoders {
            let name = decoder.decoder_name();
            let id = DecoderId::new(name);

            if decoder_map.contains_key(&id) {
                panic!(
                    "Duplicate decoder name '{}' (id: {:?}). Decoder names must be unique!",
                    name, id
                );
            }

            tracing::debug!(
                target: "torii::etl::decoder_context",
                "Registered decoder '{}' with ID {:?}",
                name,
                id
            );

            decoder_map.insert(id, decoder.clone());
        }

        decoder_map
    }

    /// Get a decoder by its ID
    pub fn get_decoder(&self, id: &DecoderId) -> Option<&Arc<dyn Decoder>> {
        self.decoders.get(id)
    }

    /// Get all registered decoder IDs (sorted for determinism)
    pub fn decoder_ids(&self) -> Vec<DecoderId> {
        let mut ids: Vec<_> = self.decoders.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Get a reference to the engine database
    pub fn engine_db(&self) -> &Arc<EngineDb> {
        &self.engine_db
    }
}

#[async_trait]
impl Decoder for DecoderContext {
    fn decoder_name(&self) -> &str {
        "context"
    }

    async fn decode_event(&self, event: &EmittedEvent) -> anyhow::Result<Vec<Envelope>> {
        if !self.contract_filter.allows(event.from_address) {
            return Ok(Vec::new());
        }

        let mut all_envelopes = Vec::new();

        // Check for explicit mapping first.
        if let Some(decoder_ids) = self.contract_filter.get_decoders(event.from_address) {
            for decoder_id in decoder_ids {
                if let Some(decoder) = self.decoders.get(decoder_id) {
                    match decoder.decode_event(event).await {
                        Ok(envelopes) => {
                            if !envelopes.is_empty() {
                                tracing::trace!(
                                    target: "torii::etl::decoder_context",
                                    "Mapped decoder '{}' decoded event from {:?} into {} envelope(s)",
                                    decoder.decoder_name(),
                                    event.from_address,
                                    envelopes.len()
                                );
                            }
                            all_envelopes.extend(envelopes);
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "torii::etl::decoder_context",
                                "Mapped decoder '{}' failed: {}",
                                decoder.decoder_name(),
                                e
                            );
                        }
                    }
                } else {
                    tracing::warn!(
                        target: "torii::etl::decoder_context",
                        "Mapped decoder ID {:?} not found for contract {:?}",
                        decoder_id,
                        event.from_address
                    );
                }
            }
        } else if self.contract_filter.skip_unmapped {
            // No mapping and skip_unmapped enabled -> skip this contract
            tracing::trace!(
                target: "torii::etl::decoder_context",
                contract = %format!("{:#x}", event.from_address),
                "Skipping unmapped contract (auto-discovery disabled)"
            );
            // Return empty - no envelopes
        } else {
            // No explicit mapping, fallback on auto-discovery sending to all decoders.
            for decoder in self.decoders.values() {
                match decoder.decode_event(event).await {
                    Ok(envelopes) => {
                        if !envelopes.is_empty() {
                            tracing::trace!(
                                target: "torii::etl::decoder_context",
                                "Decoder '{}' decoded event from {:?} into {} envelope(s)",
                                decoder.decoder_name(),
                                event.from_address,
                                envelopes.len()
                            );
                        }
                        all_envelopes.extend(envelopes);
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "torii::etl::decoder_context",
                            "Decoder '{}' failed: {}",
                            decoder.decoder_name(),
                            e
                        );
                    }
                }
            }
        }

        Ok(all_envelopes)
    }

    async fn decode(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();

        for event in events {
            let envelopes = self.decode_event(event).await?;
            all_envelopes.extend(envelopes);
        }

        tracing::debug!(
            target: "torii::etl::decoder_context",
            "Decoded {} events into {} envelopes across {} decoders",
            events.len(),
            all_envelopes.len(),
            self.decoders.len(),
        );

        Ok(all_envelopes)
    }
}
