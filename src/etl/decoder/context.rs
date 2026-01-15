//! DecoderContext manages multiple decoders with contract identification and routing.
//!
//! The DecoderContext combines decoder management with contract identification:
//! - Maintains a registry of decoders by name (DecoderId)
//! - Handles contract identification (SRC-5, ABI heuristics)
//! - Persists contract→decoder mappings to EngineDb
//! - Routes events to appropriate decoders
//!
//! # Design
//!
//! - Decoders are identified by their `decoder_name()` (hashed to DecoderId)
//! - Contract identification results are persisted to EngineDb (survives restarts)
//! - Deterministic ordering: decoders are always called in sorted DecoderId order
//! - Optional identification: without it, all events go to all decoders (backwards compatible)

use async_trait::async_trait;
use starknet::core::types::{BlockId, BlockTag, EmittedEvent, Felt, FunctionCall};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::Provider;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Batch SRC-5 checker contract address (deployed on both sepolia and mainnet)
///
/// This contract aggregates multiple `supports_interface` calls into a single call,
/// handling failures gracefully (returns false instead of reverting the whole batch).
const BATCH_SRC5_CHECKER_ADDRESS: Felt =
    Felt::from_hex_unchecked("0x0769a0daa5c600acad06cbb0ec793b77c8ac7d135f28cfa021d50acedc0c943f");

use super::Decoder;
use crate::etl::engine_db::EngineDb;
use crate::etl::envelope::Envelope;
use crate::etl::extractor::{ContractAbi, ContractIdentificationMode, DecoderId, IdentificationRule};

/// DecoderContext manages multiple decoders with integrated contract identification.
///
/// Combines decoder management with contract identification, persisting
/// contract→decoder mappings to EngineDb for performance and restartability.
pub struct DecoderContext {
    /// Decoders indexed by their ID (hash of name)
    decoders: HashMap<DecoderId, Arc<dyn Decoder>>,

    /// EngineDb for persisting contract→decoder mappings
    engine_db: Arc<EngineDb>,

    /// Identification rules (registered by sink developers)
    identification_rules: Vec<Box<dyn IdentificationRule>>,

    /// User explicit overrides (highest priority)
    explicit_mappings: HashMap<Felt, Vec<DecoderId>>,

    /// Global identification mode
    identification_mode: ContractIdentificationMode,

    /// RPC provider for contract identification (concrete type)
    provider: Option<Arc<JsonRpcClient<HttpTransport>>>,

    /// ABI cache (class_hash → ABI) - in-memory for performance
    /// Uses Mutex for interior mutability since identification happens during decode (&self)
    abi_cache: Arc<Mutex<HashMap<Felt, ContractAbi>>>,

    /// In-memory contract→decoder cache for fast lookups in hot path
    /// Populated from EngineDb on startup, updated on identification
    /// Uses Mutex for interior mutability since cache updates happen during decode (&self)
    contract_cache: Arc<Mutex<HashMap<Felt, Vec<DecoderId>>>>,
}

impl DecoderContext {
    /// Create a new DecoderContext (simple mode: no identification, all events to all decoders)
    pub fn new(decoders: Vec<Arc<dyn Decoder>>, engine_db: Arc<EngineDb>) -> Self {
        let decoder_map = Self::build_decoder_map(&decoders);
        Self {
            decoders: decoder_map,
            engine_db,
            identification_rules: Vec::new(),
            explicit_mappings: HashMap::new(),
            identification_mode: ContractIdentificationMode::empty(),
            provider: None,
            abi_cache: Arc::new(Mutex::new(HashMap::new())),
            contract_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new DecoderContext with contract identification
    ///
    /// # Arguments
    ///
    /// * `decoders` - List of decoders to manage
    /// * `engine_db` - Database for persisting contract mappings
    /// * `identification_mode` - Which identification methods to use (SRC-5, ABI heuristics)
    /// * `provider` - RPC provider for fetching contract data
    pub fn with_identification(
        decoders: Vec<Arc<dyn Decoder>>,
        engine_db: Arc<EngineDb>,
        identification_mode: ContractIdentificationMode,
        provider: Option<Arc<JsonRpcClient<HttpTransport>>>,
    ) -> Self {
        let decoder_map = Self::build_decoder_map(&decoders);
        Self {
            decoders: decoder_map,
            engine_db,
            identification_rules: Vec::new(),
            explicit_mappings: HashMap::new(),
            identification_mode,
            provider,
            abi_cache: Arc::new(Mutex::new(HashMap::new())),
            contract_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add an identification rule
    pub fn add_rule(&mut self, rule: Box<dyn IdentificationRule>) {
        self.identification_rules.push(rule);
    }

    /// Add an explicit contract mapping (highest priority, bypasses identification)
    pub fn add_explicit_mapping(&mut self, contract: Felt, decoders: Vec<DecoderId>) {
        self.explicit_mappings.insert(contract, decoders);
    }

    /// Build decoder map from list (helper for constructors)
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

    /// Check if contract identification is needed (has provider and rules/explicit mappings)
    fn identification_enabled(&self) -> bool {
        !self.identification_mode.is_empty() && (self.provider.is_some() || !self.explicit_mappings.is_empty())
    }

    /// Identify a contract using registered rules and persist to EngineDb
    async fn identify_and_persist_contract(&self, contract: Felt) -> anyhow::Result<Vec<DecoderId>> {
        // Check explicit mapping first
        if let Some(decoders) = self.explicit_mappings.get(&contract) {
            // TODO: Persist to EngineDb
            return Ok(decoders.clone());
        }

        let provider = match &self.provider {
            Some(p) => p,
            None => {
                tracing::warn!(
                    target: "torii::etl::decoder_context",
                    "No provider configured for contract identification of {:?}",
                    contract
                );
                return Ok(Vec::new());
            }
        };

        // Use BTreeSet to automatically deduplicate and maintain sorted order
        let mut all_decoders = BTreeSet::new();

        // Run SRC-5 checks first if enabled
        if self.identification_mode.contains(ContractIdentificationMode::SRC5) {
            for rule in &self.identification_rules {
                if let Some((interface_id, decoder_ids)) = rule.src5_interface() {
                    match Self::check_src5_support(contract, interface_id, provider.as_ref()).await {
                        Ok(true) => {
                            tracing::debug!(
                                target: "torii::etl::decoder_context",
                                "Contract {:?} supports SRC-5 interface {:?} (rule '{}')",
                                contract,
                                interface_id,
                                rule.name()
                            );
                            all_decoders.extend(decoder_ids);
                        }
                        Ok(false) => {}
                        Err(e) => {
                            tracing::warn!(
                                target: "torii::etl::decoder_context",
                                "SRC-5 check failed for rule '{}': {}",
                                rule.name(),
                                e
                            );
                        }
                    }
                }
            }
        }

        // Run ABI heuristics if enabled
        if self.identification_mode.contains(ContractIdentificationMode::ABI_HEURISTICS) {
            let class_hash = provider
                .get_class_hash_at(starknet::core::types::BlockId::Tag(starknet::core::types::BlockTag::Latest), contract)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get class hash for {:?}: {}", contract, e))?;

            // Check ABI cache first
            let abi = {
                let cache = self.abi_cache.lock().await;
                cache.get(&class_hash).cloned()
            };

            let abi = if let Some(cached_abi) = abi {
                cached_abi
            } else {
                let class = provider
                    .get_class(starknet::core::types::BlockId::Tag(starknet::core::types::BlockTag::Latest), class_hash)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to get class for {:?}: {}", class_hash, e))?;

                let abi = ContractAbi::from_contract_class(class)?;

                // Store in cache
                let mut cache = self.abi_cache.lock().await;
                cache.insert(class_hash, abi.clone());
                abi
            };

            // Run all identification rules
            for rule in &self.identification_rules {
                match rule.identify_by_abi(contract, class_hash, &abi) {
                    Ok(decoder_ids) => {
                        if !decoder_ids.is_empty() {
                            tracing::debug!(
                                target: "torii::etl::decoder_context",
                                "Rule '{}' identified contract {:?} with {} decoder(s)",
                                rule.name(),
                                contract,
                                decoder_ids.len()
                            );
                            all_decoders.extend(decoder_ids);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "torii::etl::decoder_context",
                            "Rule '{}' failed to identify contract {:?}: {}",
                            rule.name(),
                            contract,
                            e
                        );
                    }
                }
            }
        }

        let result: Vec<DecoderId> = all_decoders.into_iter().collect();

        // Persist to EngineDb
        let decoder_id_u64s: Vec<u64> = result.iter().map(|id| id.as_u64()).collect();

        if let Err(e) = self.engine_db.store_contract_decoders(contract, &decoder_id_u64s).await {
            tracing::warn!(
                target: "torii::etl::decoder_context",
                "Failed to persist contract decoders to DB for {:?}: {}",
                contract,
                e
            );
        } else {
            tracing::debug!(
                target: "torii::etl::decoder_context",
                "Identified and persisted contract {:?} with {} decoder(s)",
                contract,
                result.len()
            );
        }

        // Update in-memory cache
        {
            let mut cache = self.contract_cache.lock().await;
            cache.insert(contract, result.clone());
        }

        Ok(result)
    }

    /// Check SRC-5 interface support (single contract, single interface)
    async fn check_src5_support<P>(
        contract: Felt,
        interface_id: Felt,
        provider: &P,
    ) -> anyhow::Result<bool>
    where
        P: Provider,
    {
        // Call supports_interface(interface_id) -> bool
        let selector = starknet::core::utils::get_selector_from_name("supports_interface")
            .map_err(|e| anyhow::anyhow!("Failed to compute selector: {}", e))?;

        let result = provider
            .call(
                FunctionCall {
                    contract_address: contract,
                    entry_point_selector: selector,
                    calldata: vec![interface_id],
                },
                BlockId::Tag(BlockTag::Latest),
            )
            .await
            .map_err(|e| anyhow::anyhow!("SRC-5 call failed: {}", e))?;

        // Parse boolean result (0 = false, 1 = true)
        Ok(result.first().map(|v| *v != Felt::ZERO).unwrap_or(false))
    }

    /// Collect contract addresses from events that aren't in cache or explicit mappings
    ///
    /// Pre-scans events to find unknown contracts that need identification.
    /// Returns deduplicated list of addresses for batch identification.
    async fn collect_unknown_contracts(&self, events: &[EmittedEvent]) -> Vec<Felt> {
        let cache = self.contract_cache.lock().await;
        let mut unknown = Vec::new();
        let mut seen = HashSet::new();

        for event in events {
            let addr = event.from_address;
            // Skip if already in cache, explicit mappings, or already collected
            if !cache.contains_key(&addr)
                && !self.explicit_mappings.contains_key(&addr)
                && seen.insert(addr)
            {
                unknown.push(addr);
            }
        }

        unknown
    }

    /// Batch identify multiple contracts via SRC-5 checks
    ///
    /// Uses the BatchSrc5Checker contract to check all (contract, interface) pairs
    /// in a single RPC call. The aggregator handles failures gracefully (returns false
    /// instead of reverting), unlike JSON-RPC batch requests which fail entirely.
    ///
    /// Results are cached in-memory and persisted to EngineDb.
    async fn batch_identify_src5(&self, contracts: &[Felt]) -> anyhow::Result<()> {
        let provider = match &self.provider {
            Some(p) => p,
            None => return Ok(()), // No provider, skip identification
        };

        if contracts.is_empty() || self.identification_rules.is_empty() {
            return Ok(());
        }

        // Build queries: Vec<(contract, interface_id)> with tracking info
        let mut queries: Vec<Felt> = Vec::new();
        // Track which query index maps to which (contract, decoder_ids)
        let mut query_map: Vec<(Felt, Vec<DecoderId>)> = Vec::new();

        for &contract in contracts {
            for rule in &self.identification_rules {
                if let Some((interface_id, decoder_ids)) = rule.src5_interface() {
                    // Each query is (contract_address, interface_id) - 2 felts
                    queries.push(contract);
                    queries.push(interface_id);
                    query_map.push((contract, decoder_ids));
                }
            }
        }

        if queries.is_empty() {
            return Ok(());
        }

        let num_queries = query_map.len();

        tracing::info!(
            target: "torii::etl::decoder_context",
            "Batch identifying {} contracts with {} SRC-5 checks via aggregator",
            contracts.len(),
            num_queries
        );

        // Build calldata for batch_supports_interface(queries: Array<(ContractAddress, felt252)>)
        // Cairo array encoding: [length, elem0_field0, elem0_field1, elem1_field0, elem1_field1, ...]
        let mut calldata = Vec::with_capacity(1 + queries.len());
        calldata.push(Felt::from(num_queries as u64)); // Array length
        calldata.extend(queries);

        // Compute selector for batch_supports_interface
        let selector = starknet::core::utils::get_selector_from_name("batch_supports_interface")
            .map_err(|e| anyhow::anyhow!("Failed to compute selector: {}", e))?;

        // Single RPC call to the aggregator contract
        let response = provider
            .call(
                FunctionCall {
                    contract_address: BATCH_SRC5_CHECKER_ADDRESS,
                    entry_point_selector: selector,
                    calldata,
                },
                BlockId::Tag(BlockTag::Latest),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Batch SRC-5 aggregator call failed: {}", e))?;

        // Parse response: Array<bool> encoded as [length, bool0, bool1, ...]
        if response.is_empty() {
            anyhow::bail!("Empty response from batch SRC-5 aggregator");
        }

        let result_len = response[0]
            .try_into()
            .map(|v: u64| v as usize)
            .unwrap_or(0);

        if result_len != num_queries {
            anyhow::bail!(
                "Batch SRC-5 response length mismatch: expected {}, got {}",
                num_queries,
                result_len
            );
        }

        // Parse boolean results and build result map
        let mut results: HashMap<Felt, BTreeSet<DecoderId>> = HashMap::new();

        for (i, (contract, decoder_ids)) in query_map.into_iter().enumerate() {
            // Result array starts at index 1 (after length)
            let supports = response
                .get(1 + i)
                .map(|v| *v != Felt::ZERO)
                .unwrap_or(false);

            if supports {
                results.entry(contract).or_default().extend(decoder_ids);
            }
        }

        // Cache and persist results
        let mut cache = self.contract_cache.lock().await;
        for &contract in contracts {
            let decoder_ids: Vec<DecoderId> = results
                .remove(&contract)
                .map(|set| set.into_iter().collect())
                .unwrap_or_default();

            // Persist to EngineDb
            let decoder_id_u64s: Vec<u64> = decoder_ids.iter().map(|id| id.as_u64()).collect();
            if let Err(e) = self
                .engine_db
                .store_contract_decoders(contract, &decoder_id_u64s)
                .await
            {
                tracing::warn!(
                    target: "torii::etl::decoder_context",
                    "Failed to persist contract decoders for {:?}: {}",
                    contract,
                    e
                );
            }

            // Update in-memory cache
            cache.insert(contract, decoder_ids);
        }

        tracing::info!(
            target: "torii::etl::decoder_context",
            "Batch identification complete for {} contracts",
            contracts.len()
        );

        Ok(())
    }
}

#[async_trait]
impl Decoder for DecoderContext {
    fn decoder_name(&self) -> &str {
        "context" // The decoder context itself has a name
    }

    async fn decode_event(&self, event: &EmittedEvent) -> anyhow::Result<Vec<Envelope>> {
        // Route through identification if enabled, otherwise pass to all decoders
        if self.identification_enabled() {
            self.decode_event_with_identification(event).await
        } else {
            self.decode_event_without_identification(event).await
        }
    }

    async fn decode(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        // Route through identification if enabled, otherwise pass all events to all decoders
        if self.identification_enabled() {
            self.decode_with_identification(events).await
        } else {
            self.decode_without_identification(events).await
        }
    }
}

impl DecoderContext {
    /// Decode a single event without identification: pass to all decoders
    async fn decode_event_without_identification(&self, event: &EmittedEvent) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();

        for decoder in self.decoders.values() {
            let envelopes = decoder.decode_event(event).await?;
            all_envelopes.extend(envelopes);
        }

        Ok(all_envelopes)
    }

    /// Decode a single event with identification: route to appropriate decoders
    ///
    /// Uses in-memory cache for fast lookup. Falls back to DB then identification.
    async fn decode_event_with_identification(&self, event: &EmittedEvent) -> anyhow::Result<Vec<Envelope>> {
        let contract_address = event.from_address;

        // Fast path: check in-memory cache first
        let decoder_ids = {
            let cache = self.contract_cache.lock().await;
            cache.get(&contract_address).cloned()
        };

        let decoder_ids = match decoder_ids {
            Some(ids) => ids,
            None => {
                // Cache miss: check explicit mappings, then DB, then identify
                if let Some(ids) = self.explicit_mappings.get(&contract_address) {
                    // Update cache with explicit mapping
                    let mut cache = self.contract_cache.lock().await;
                    cache.insert(contract_address, ids.clone());
                    ids.clone()
                } else {
                    // Check EngineDb
                    match self.engine_db.load_contract_decoders(contract_address).await {
                        Ok(Some(decoder_id_u64s)) => {
                            let ids: Vec<DecoderId> = decoder_id_u64s
                                .into_iter()
                                .map(DecoderId::from_u64)
                                .collect();
                            // Update cache
                            let mut cache = self.contract_cache.lock().await;
                            cache.insert(contract_address, ids.clone());
                            ids
                        }
                        Ok(None) => {
                            // Need to identify this contract
                            match self.identify_and_persist_contract(contract_address).await {
                                Ok(ids) => ids, // Cache already updated by identify_and_persist
                                Err(e) => {
                                    tracing::warn!(
                                        target: "torii::etl::decoder_context",
                                        "Failed to identify contract {:?}: {}. Skipping event",
                                        contract_address,
                                        e
                                    );
                                    return Ok(Vec::new());
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "torii::etl::decoder_context",
                                "Failed to load contract decoders from DB for {:?}: {}",
                                contract_address,
                                e
                            );
                            return Ok(Vec::new());
                        }
                    }
                }
            }
        };

        if decoder_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Decode with each relevant decoder
        let mut all_envelopes = Vec::new();
        for decoder_id in decoder_ids {
            if let Some(decoder) = self.decoders.get(&decoder_id) {
                let envelopes = decoder.decode_event(event).await?;
                all_envelopes.extend(envelopes);
            }
        }

        Ok(all_envelopes)
    }

    /// Decode without identification: all events go to all decoders (backwards compatible)
    async fn decode_without_identification(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();

        for event in events {
            let envelopes = self.decode_event_without_identification(event).await?;
            all_envelopes.extend(envelopes);
        }

        tracing::debug!(
            target: "torii::etl::decoder_context",
            "Decoded {} events into {} envelopes across {} decoders (no identification)",
            events.len(),
            all_envelopes.len(),
            self.decoders.len(),
        );

        Ok(all_envelopes)
    }

    /// Decode with identification: route events based on contract mappings
    ///
    /// Pre-scans events to batch identify unknown contracts, then processes events
    /// with all lookups hitting the cache.
    async fn decode_with_identification(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        // Phase 1: Batch identify unknown contracts via SRC-5 (if enabled)
        if self.identification_mode.contains(ContractIdentificationMode::SRC5) {
            let unknown = self.collect_unknown_contracts(events).await;
            if !unknown.is_empty() {
                // Also check EngineDb before batch RPC - some may already be persisted
                let mut need_identification = Vec::new();
                {
                    let mut cache = self.contract_cache.lock().await;
                    for contract in unknown {
                        // Check EngineDb
                        match self.engine_db.load_contract_decoders(contract).await {
                            Ok(Some(decoder_id_u64s)) => {
                                // Already in DB, just load to cache
                                let ids: Vec<DecoderId> = decoder_id_u64s
                                    .into_iter()
                                    .map(DecoderId::from_u64)
                                    .collect();
                                cache.insert(contract, ids);
                            }
                            Ok(None) => {
                                // Not in DB, needs identification
                                need_identification.push(contract);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "torii::etl::decoder_context",
                                    "Failed to load contract from DB {:?}: {}",
                                    contract,
                                    e
                                );
                                need_identification.push(contract);
                            }
                        }
                    }
                }

                // Batch identify contracts not in DB
                if !need_identification.is_empty() {
                    if let Err(e) = self.batch_identify_src5(&need_identification).await {
                        tracing::warn!(
                            target: "torii::etl::decoder_context",
                            "Batch SRC-5 identification failed: {}. Falling back to individual identification.",
                            e
                        );
                    }
                }
            }
        }

        // Phase 2: Process events (all lookups should now hit cache)
        let mut all_envelopes = Vec::new();

        for event in events {
            let envelopes = self.decode_event_with_identification(event).await?;
            all_envelopes.extend(envelopes);
        }

        tracing::debug!(
            target: "torii::etl::decoder_context",
            "Decoded {} events into {} envelopes using identification routing",
            events.len(),
            all_envelopes.len(),
        );

        Ok(all_envelopes)
    }
}
