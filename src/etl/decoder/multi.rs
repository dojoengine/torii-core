//! MultiDecoder manages multiple decoders with registry-aware routing.
//!
//! The MultiDecoder maintains a registry of decoders by name (DecoderId)
//! and integrates with the ContractRegistry to route events to the appropriate
//! decoders based on contract identification.
//!
//! # Design
//!
//! - Decoders are identified by their `decoder_name()` (hashed to DecoderId)
//! - Events are routed through the ContractRegistry
//! - Deterministic ordering: decoders are always called in sorted DecoderId order
//! - Optional registry: without it, all events go to all decoders (backwards compatible)

use async_trait::async_trait;
use starknet::core::types::EmittedEvent;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::Decoder;
use crate::etl::envelope::Envelope;
use crate::etl::extractor::{ContractRegistry, DecoderId};

/// MultiDecoder runs multiple decoders with optional registry-based routing.
///
/// Decoders are organized by their DecoderId (hash of decoder name).
/// When a ContractRegistry is provided, it determines which decoders should
/// process events from each contract, providing deterministic, sorted routing.
///
/// Without a registry, all events are sent to all decoders (backwards compatible).
///
/// # Contract Identification
///
/// The registry is used for **lookup only**. Contract identification (which requires
/// a Provider) should happen in the ETL loop before calling decode(). This keeps
/// MultiDecoder simple and avoids object-safety issues with Provider.
pub struct MultiDecoder {
    /// Decoders indexed by their ID (hash of name)
    decoders: HashMap<DecoderId, Arc<dyn Decoder>>,

    /// Optional contract registry for smart routing (lookup only)
    registry: Option<Arc<Mutex<ContractRegistry>>>,
}

impl MultiDecoder {
    /// Create a new MultiDecoder from a list of decoders (without registry)
    ///
    /// Decoders are automatically indexed by their `decoder_name()`.
    /// Duplicate names will cause a panic.
    ///
    /// Without a registry, all events will be sent to all decoders.
    pub fn new(decoders: Vec<Arc<dyn Decoder>>) -> Self {
        let decoder_map = Self::build_decoder_map(decoders);
        Self { decoders: decoder_map, registry: None }
    }

    /// Create a new MultiDecoder with contract registry routing
    ///
    /// When a registry is provided, events are routed based on contract identification:
    /// 1. Registry looks up which decoders should handle each contract
    /// 2. Only relevant decoders receive events from each contract
    pub fn with_registry(
        decoders: Vec<Arc<dyn Decoder>>,
        registry: Arc<Mutex<ContractRegistry>>,
    ) -> Self {
        let decoder_map = Self::build_decoder_map(decoders);
        Self { decoders: decoder_map, registry: Some(registry) }
    }

    /// Build decoder map from list (helper for constructors)
    fn build_decoder_map(decoders: Vec<Arc<dyn Decoder>>) -> HashMap<DecoderId, Arc<dyn Decoder>> {
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
                target: "torii::etl::multi_decoder",
                "Registered decoder '{}' with ID {:?}",
                name,
                id
            );

            decoder_map.insert(id, decoder);
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
}

#[async_trait]
impl Decoder for MultiDecoder {
    fn decoder_name(&self) -> &str {
        "multi" // The multi-decoder itself has a name
    }

    async fn decode(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        // Route through registry if available, otherwise pass all events to all decoders
        match &self.registry {
            Some(registry) => self.decode_with_registry(events, registry).await,
            None => self.decode_without_registry(events).await,
        }
    }
}

impl MultiDecoder {
    /// Decode without registry: all events go to all decoders (backwards compatible)
    async fn decode_without_registry(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();

        for decoder in self.decoders.values() {
            let envelopes = decoder.decode(events).await?;
            all_envelopes.extend(envelopes);
        }

        tracing::debug!(
            target: "torii::etl::multi_decoder",
            "Decoded {} events into {} envelopes across {} decoders (no registry)",
            events.len(),
            all_envelopes.len(),
            self.decoders.len(),
        );

        Ok(all_envelopes)
    }

    /// Decode with registry: route events based on contract mappings
    ///
    /// Contracts are identified **lazily during decode** - no pre-iteration needed.
    /// Each contract is identified once on its first event, then cached.
    ///
    /// Events are processed in order without reorganization - simply route each
    /// event to its registered decoders as it comes.
    ///
    /// # Performance Consideration
    ///
    /// **Current implementation**: Lock mutex per event (simple, correct)
    /// - Lock overhead scales with number of events (10k events = 10k locks)
    ///
    /// **Alternative approach**: Pre-collect unique addresses, identify once
    /// ```rust,ignore
    /// // 1. Collect unique addresses: O(n) HashSet build
    /// let unique: HashSet<_> = events.iter().map(|e| e.from_address).collect();
    ///
    /// // 2. Identify all unknown (lock ONCE, not N times)
    /// { let mut lock = registry.lock().await;
    ///   for addr in unique { if !lock.is_known(addr) { identify... } }
    /// }
    ///
    /// // 3. Route events (read-only, could use RwLock)
    /// for event in events { let ids = registry.get_decoders_no_lock(...); }
    /// ```
    ///
    /// **Trade-off**: Extra O(n) pass to build HashSet vs N mutex locks
    ///
    /// **TODO**: Profile in production to determine if mutex overhead is significant.
    /// If it becomes a bottleneck, switch to pre-collection approach above.
    async fn decode_with_registry(
        &self,
        events: &[EmittedEvent],
        registry: &Arc<Mutex<ContractRegistry>>,
    ) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();

        // Process each event in order - no reorganization
        for event in events {
            // TODO(performance): Lock per event - see method docs for alternative approach
            // Identify contract if needed, then get decoder IDs
            let decoder_ids = {
                let mut registry_lock = registry.lock().await;
                match registry_lock
                    .ensure_identified_and_get_decoders(event.from_address)
                    .await
                {
                    Ok(Some(ids)) => ids,
                    Ok(None) => {
                        tracing::trace!(
                            target: "torii::etl::multi_decoder",
                            "No decoders registered for contract {:?}, skipping event",
                            event.from_address
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "torii::etl::multi_decoder",
                            "Failed to identify contract {:?}: {}. Skipping event.",
                            event.from_address,
                            e
                        );
                        continue;
                    }
                }
            }; // Lock dropped here

            // Route this event to its decoders
            for decoder_id in decoder_ids {
                if let Some(decoder) = self.decoders.get(&decoder_id) {
                    // Pass single event to decoder
                    let envelopes = decoder.decode(&[event.clone()]).await?;
                    all_envelopes.extend(envelopes);
                } else {
                    tracing::warn!(
                        target: "torii::etl::multi_decoder",
                        "Registry references decoder {:?} but it's not registered in MultiDecoder",
                        decoder_id
                    );
                }
            }
        }

        tracing::debug!(
            target: "torii::etl::multi_decoder",
            "Decoded {} events into {} envelopes using registry routing",
            events.len(),
            all_envelopes.len(),
        );

        Ok(all_envelopes)
    }
}
