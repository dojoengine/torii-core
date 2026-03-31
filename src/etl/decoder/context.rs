//! DecoderContext manages multiple decoders with contract filtering.
//!
//! The DecoderContext routes events to all registered decoders, optionally
//! filtering by contract address (whitelist/blacklist).
//!
//! # Design
//!
//! - Decoders are identified by their `decoder_name()` (hashed to DecoderId)
//! - Explicit contract mappings take highest priority
//! - Registry mappings (from auto-identification) take second priority
//! - Unmapped contracts with no registry fall back to all decoders
//! - Deterministic ordering: decoders are always called in sorted DecoderId order

use super::{ContractFilter, Decoder, DecoderId};
use crate::etl::decoder::TransactionDecoder;
use crate::etl::engine_db::EngineDb;
use crate::etl::envelope::TransactionMsgs;
use crate::etl::EventData;
use async_trait::async_trait;
use itertools::Itertools;
use starknet::core::types::Felt;
use std::collections::HashMap;
use std::sync::Arc;

fn event_preview(from_address: &Felt, block_number: u64, transaction_hash: &Felt) -> String {
    format!(
        " contract={:#066x} block_number={} tx={:#066x}",
        from_address, block_number, transaction_hash
    )
}

/// DecoderContext manages multiple decoders with contract filtering.
///
/// Routes events to decoders based on:
/// 1. Explicit mappings (highest priority, from ContractFilter)
/// 2. Registry mappings (from ContractRegistry auto-identification)
/// 3. All decoders (fallback when no registry is configured)
///
#[allow(dead_code)]
pub struct DecoderContext {
    /// Decoders indexed by their ID (hash of name)
    decoders: HashMap<DecoderId, Arc<dyn Decoder>>,

    /// EngineDb for ETL state persistence (cursor, not contract mappings)
    engine_db: Arc<EngineDb>,

    /// Contract filter (explicit mappings + blacklist)
    contract_filter: ContractFilter,
}

impl DecoderContext {
    /// Create a new DecoderContext
    ///
    /// # Arguments
    ///
    /// * `decoders` - List of decoders to manage
    /// * `engine_db` - Database for ETL state persistence
    /// * `contract_filter` - Explicit mappings + blacklist
    pub fn new(
        decoders: Vec<Arc<dyn Decoder>>,
        engine_db: Arc<EngineDb>,
        contract_filter: ContractFilter,
    ) -> Self {
        let decoder_map = Self::build_decoder_map(&decoders);

        let filter_desc =
            if contract_filter.mappings.is_empty() && contract_filter.blacklist.is_empty() {
                "none (will try all decoders for all contracts)".to_string()
            } else {
                let mut parts = Vec::new();
                if !contract_filter.mappings.is_empty() {
                    parts.push(format!(
                        "{} explicit mappings",
                        contract_filter.mappings.len()
                    ));
                }
                if !contract_filter.blacklist.is_empty() {
                    parts.push(format!("{} blacklisted", contract_filter.blacklist.len()));
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

            assert!(
                !decoder_map.contains_key(&id),
                "Duplicate decoder name '{name}' (id: {id:?}). Decoder names must be unique!"
            );

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
impl TransactionDecoder for DecoderContext {
    fn decoder_name(&self) -> &'static str {
        "context"
    }

    async fn decode_contract_transaction(
        &self,
        from_address: Felt,
        block_number: u64,
        transaction_hash: Felt,
        datas: &[EventData],
    ) -> anyhow::Result<TransactionMsgs> {
        // 1. Check blacklist first
        if !self.contract_filter.allows(from_address) {
            return Ok(TransactionMsgs::new_empty(
                block_number,
                transaction_hash,
                from_address,
            ));
        }
        let decoders = match self.contract_filter.get_decoders(&from_address) {
            Some(ids) => {
                tracing::trace!(
                    target: "torii::etl::decoder_context",
                    "Decoding event from {:#x} with {} specific decoder(s)",
                    from_address,
                    ids.len()
                );
                ids.into_iter()
                    .filter_map(|id| self.decoders.get(&id))
                    .collect_vec()
            }
            None => {
                tracing::trace!(
                    target: "torii::etl::decoder_context",
                    "Decoding event from {:#x} with all decoders ({} total)",
                    from_address,
                    self.decoders.len()
                );
                self.decoders.values().collect_vec()
            }
        };
        let mut all_msgs = Vec::new();
        for data in datas {
            for decoder in &decoders {
                match decoder
                    .decode_event(
                        &from_address,
                        block_number,
                        &transaction_hash,
                        &data.keys,
                        &data.data,
                    )
                    .await
                {
                    Ok(msgs) => {
                        if !msgs.is_empty() {
                            tracing::trace!(
                                target: "torii::etl::decoder_context",
                                "Decoder '{}' decoded event from {:#x} into {} envelope(s)",
                                decoder.decoder_name(),
                                from_address,
                                msgs.len()
                            );
                        }
                        all_msgs.extend(msgs);
                    }
                    Err(e) => {
                        let selector = event
                            .keys
                            .first()
                            .map_or_else(|| "<missing>".to_string(), |felt| format!("{felt:#x}"));
                        let preview = event_preview(event);
                        tracing::warn!(
                            target: "torii::etl::decoder_context",
                            contract = %format!("{:#x}", event.from_address),
                            selector = %selector,
                            tx_hash = %format!("{:#x}", event.transaction_hash),
                            block_number = event.block_number,
                            event = %preview,
                            "Decoder '{}' failed: {}",
                            decoder.decoder_name(),
                            e,
                            event_preview(&from_address, block_number, &transaction_hash)
                        );
                    }
                }
            }
        }
        Ok(TransactionMsgs::new(
            block_number,
            transaction_hash,
            from_address,
            all_msgs,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etl::engine_db::{EngineDb, EngineDbConfig};
    use crate::etl::envelope::{Envelope, TypeId, TypedBody};
    use async_trait::async_trait;
    use std::any::Any;

    #[derive(Debug)]
    struct TestBody {
        seq: u64,
    }

    impl TypedBody for TestBody {
        fn envelope_type_id(&self) -> TypeId {
            TypeId::new("test.body")
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    struct OrderedDecoder {
        contract: Felt,
    }

    #[async_trait]
    impl Decoder for OrderedDecoder {
        fn decoder_name(&self) -> &'static str {
            "ordered_decoder"
        }

        async fn decode_event(&self, event: &EmittedEvent) -> anyhow::Result<Vec<Envelope>> {
            if event.from_address != self.contract {
                return Ok(Vec::new());
            }

            Ok(vec![Envelope::new(
                format!("evt-{}", event.block_number.unwrap_or_default()),
                Box::new(TestBody {
                    seq: event.block_number.unwrap_or_default(),
                }),
                HashMap::new(),
            )])
        }
    }

    async fn make_engine_db() -> Arc<EngineDb> {
        Arc::new(
            EngineDb::new(EngineDbConfig {
                path: "sqlite::memory:".to_string(),
            })
            .await
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn decode_preserves_order_with_parallel_chunks() {
        let contract = Felt::from(0x1234_u64);
        let decoder: Arc<dyn Decoder> = Arc::new(OrderedDecoder { contract });
        let engine_db = make_engine_db().await;
        let context = DecoderContext::new(vec![decoder], engine_db, ContractFilter::new());

        let events = (0..600_u64)
            .map(|seq| EmittedEvent {
                from_address: contract,
                keys: Vec::new(),
                data: Vec::new(),
                block_hash: None,
                block_number: Some(seq),
                transaction_hash: Felt::from(seq + 1),
            })
            .collect::<Vec<_>>();

        let envelopes = Decoder::decode(&context, &events).await.unwrap();
        let actual = envelopes
            .iter()
            .map(|envelope| envelope.downcast_ref::<TestBody>().unwrap().seq)
            .collect::<Vec<_>>();
        let expected = (0..600_u64).collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
