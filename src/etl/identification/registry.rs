//! Contract registry for caching contract→decoder mappings.
//!
//! The registry loads cached mappings from database and provides shared access.
//! It also supports runtime identification of unknown contracts by fetching their ABIs
//! and running identification rules.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use starknet::core::types::requests::{GetClassHashAtRequest, GetClassRequest};
use starknet::core::types::{BlockId, BlockTag, Felt};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use tokio::sync::RwLock;

use super::IdentificationRule;
use crate::etl::decoder::DecoderId;
use crate::etl::engine_db::EngineDb;
use crate::etl::extractor::ContractAbi;

/// Trait for contract identification (object-safe).
///
/// This trait allows type-erased contract identification, so that `ToriiConfig`
/// can store a registry without knowing the concrete provider type.
#[async_trait]
pub trait ContractIdentifier: Send + Sync {
    /// Identify contracts by fetching their ABIs and running rules.
    ///
    /// Returns HashMap of contract → decoder IDs for newly identified contracts.
    async fn identify_contracts(
        &self,
        contract_addresses: &[Felt],
    ) -> Result<HashMap<Felt, Vec<DecoderId>>>;

    /// Get a shared reference to the cache.
    fn shared_cache(&self) -> Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>;
}

/// Contract registry for caching contract→decoder mappings.
///
/// The registry manages:
/// - In-memory cache of contract→decoder mappings
/// - Persistence to EngineDb for restart recovery
///
/// # Thread Safety
///
/// The registry uses interior mutability (RwLock) for the cache,
/// allowing shared access while supporting concurrent reads and
/// exclusive writes during identification.
///
/// # Performance
///
/// Uses batch JSON-RPC requests to identify multiple contracts efficiently:
/// - Batch 1: Fetch all class hashes at once
/// - Batch 2: Fetch all unique contract classes (deduplicated by class hash)
///
/// This reduces N×2 sequential API calls to just 2 batch calls.
pub struct ContractRegistry {
    /// Starknet provider for fetching contract ABIs (JsonRpcClient for batch support)
    provider: Arc<JsonRpcClient<HttpTransport>>,

    /// Engine database for persistence
    engine_db: Arc<EngineDb>,

    /// Identification rules for matching contract ABIs to decoders
    rules: Vec<Box<dyn IdentificationRule>>,

    /// In-memory cache: contract → decoders
    /// Empty Vec means "identified but no decoders match"
    /// Wrapped in Arc for sharing with DecoderContext
    cache: Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>,
}

impl ContractRegistry {
    /// Create a new contract registry.
    ///
    /// # Arguments
    ///
    /// * `provider` - JsonRpcClient provider for fetching ABIs (supports batch requests)
    /// * `engine_db` - Database for persistence
    pub fn new(provider: Arc<JsonRpcClient<HttpTransport>>, engine_db: Arc<EngineDb>) -> Self {
        Self {
            provider,
            engine_db,
            rules: Vec::new(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add an identification rule.
    ///
    /// Rules are run in order during identification. All matching decoders
    /// from all rules are collected.
    pub fn with_rule(mut self, rule: Box<dyn IdentificationRule>) -> Self {
        tracing::debug!(
            target: "torii::etl::identification",
            "Registered identification rule: {}",
            rule.name()
        );
        self.rules.push(rule);
        self
    }

    /// Load cached mappings from database on startup.
    ///
    /// This should be called during initialization to restore
    /// previously identified contracts.
    pub async fn load_from_db(&self) -> Result<usize> {
        let mappings = self.engine_db.get_all_contract_decoders().await?;
        let count = mappings.len();

        let mut cache = self.cache.write().await;
        for (contract, decoder_ids, _timestamp) in mappings {
            cache.insert(contract, decoder_ids);
        }

        tracing::info!(
            target: "torii::etl::identification",
            "Loaded {} contract mappings from database",
            count
        );

        Ok(count)
    }

    /// Get a shared reference to the cache for use with DecoderContext.
    ///
    /// This allows the DecoderContext to read from the registry's cache
    /// without needing a direct reference to the registry (which has a generic Provider type).
    pub fn shared_cache(&self) -> Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>> {
        self.cache.clone()
    }

    /// Identify contracts by fetching their ABIs and running rules.
    ///
    /// This method identifies unknown contracts from a batch of events by:
    /// 1. Filtering out contracts already in the cache
    /// 2. Batch fetching all class hashes at once
    /// 3. Batch fetching all unique contract classes (deduplicated by class hash)
    /// 4. Running all identification rules
    /// 5. Caching results in both memory and database
    ///
    /// # Performance
    ///
    /// Uses batch JSON-RPC requests to minimize API calls:
    /// - Instead of N×2 sequential calls, makes just 2 batch calls
    /// - Class fetching is deduplicated (multiple contracts may share the same class)
    ///
    /// # Arguments
    ///
    /// * `contract_addresses` - Contract addresses from the current batch
    ///
    /// # Returns
    ///
    /// HashMap of contract → decoder IDs for contracts that were successfully identified.
    /// Contracts that fail identification are cached as empty (to avoid re-fetching).
    pub async fn identify_contracts(
        &self,
        contract_addresses: &[Felt],
    ) -> Result<HashMap<Felt, Vec<DecoderId>>> {
        // Deduplicate and filter out contracts already in cache
        let unique_addresses: HashSet<Felt> = contract_addresses.iter().copied().collect();
        let cache = self.cache.read().await;
        let unknown: Vec<Felt> = unique_addresses
            .into_iter()
            .filter(|addr| !cache.contains_key(addr))
            .collect();
        drop(cache);

        if unknown.is_empty() {
            return Ok(HashMap::new());
        }

        tracing::debug!(
            target: "torii::etl::identification",
            "Identifying {} unknown contracts using batch requests",
            unknown.len()
        );

        // BATCH 1: Fetch all class hashes at once
        let class_hash_requests: Vec<ProviderRequestData> = unknown
            .iter()
            .map(|&addr| {
                ProviderRequestData::GetClassHashAt(GetClassHashAtRequest {
                    block_id: BlockId::Tag(BlockTag::Latest),
                    contract_address: addr,
                })
            })
            .collect();

        let class_hash_responses = self
            .provider
            .batch_requests(&class_hash_requests)
            .await
            .context("Failed to batch fetch class hashes")?;

        // Map contract → class_hash, track failures
        let mut contract_to_class: HashMap<Felt, Felt> = HashMap::new();
        for (addr, response) in unknown.iter().zip(class_hash_responses) {
            match response {
                ProviderResponseData::GetClassHashAt(class_hash) => {
                    contract_to_class.insert(*addr, class_hash);
                }
                _ => {
                    tracing::debug!(
                        target: "torii::etl::identification",
                        contract = %format!("{:#x}", addr),
                        "Failed to get class hash, caching as empty"
                    );
                    self.cache_empty(*addr).await;
                }
            }
        }

        if contract_to_class.is_empty() {
            return Ok(HashMap::new());
        }

        // BATCH 2: Fetch unique classes (deduplicated by class hash)
        let unique_class_hashes: Vec<Felt> = contract_to_class
            .values()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        tracing::debug!(
            target: "torii::etl::identification",
            contracts = contract_to_class.len(),
            unique_classes = unique_class_hashes.len(),
            "Fetching unique contract classes"
        );

        let class_requests: Vec<ProviderRequestData> = unique_class_hashes
            .iter()
            .map(|&class_hash| {
                ProviderRequestData::GetClass(GetClassRequest {
                    block_id: BlockId::Tag(BlockTag::Latest),
                    class_hash,
                })
            })
            .collect();

        let class_responses = self
            .provider
            .batch_requests(&class_requests)
            .await
            .context("Failed to batch fetch classes")?;

        // Map class_hash → ContractAbi
        let mut class_to_abi: HashMap<Felt, ContractAbi> = HashMap::new();
        for (class_hash, response) in unique_class_hashes.iter().zip(class_responses) {
            match response {
                ProviderResponseData::GetClass(contract_class) => {
                    match ContractAbi::from_contract_class(contract_class) {
                        Ok(abi) => {
                            class_to_abi.insert(*class_hash, abi);
                        }
                        Err(e) => {
                            tracing::debug!(
                                target: "torii::etl::identification",
                                class_hash = %format!("{:#x}", class_hash),
                                error = %e,
                                "Failed to parse ABI"
                            );
                        }
                    }
                }
                _ => {
                    tracing::debug!(
                        target: "torii::etl::identification",
                        class_hash = %format!("{:#x}", class_hash),
                        "Failed to get class"
                    );
                }
            }
        }

        // Run identification rules for each contract
        let mut results = HashMap::new();
        for (contract_address, class_hash) in &contract_to_class {
            let decoder_ids = if let Some(abi) = class_to_abi.get(class_hash) {
                self.run_rules(*contract_address, *class_hash, abi)
            } else {
                Vec::new()
            };

            if !decoder_ids.is_empty() {
                tracing::info!(
                    target: "torii::etl::identification",
                    contract = %format!("{:#x}", contract_address),
                    decoders = ?decoder_ids,
                    "Contract identified"
                );
            }

            // Cache and persist
            self.cache_and_persist(*contract_address, decoder_ids.clone())
                .await;
            results.insert(*contract_address, decoder_ids);
        }

        Ok(results)
    }

    /// Run all identification rules on a contract's ABI.
    fn run_rules(
        &self,
        contract_address: Felt,
        class_hash: Felt,
        abi: &ContractAbi,
    ) -> Vec<DecoderId> {
        let mut matched_decoders = BTreeSet::new();
        for rule in &self.rules {
            match rule.identify_by_abi(contract_address, class_hash, abi) {
                Ok(decoder_ids) => {
                    if !decoder_ids.is_empty() {
                        tracing::debug!(
                            target: "torii::etl::identification",
                            contract = %format!("{:#x}", contract_address),
                            rule = rule.name(),
                            decoders = ?decoder_ids,
                            "Rule matched"
                        );
                        matched_decoders.extend(decoder_ids);
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        target: "torii::etl::identification",
                        contract = %format!("{:#x}", contract_address),
                        rule = rule.name(),
                        error = %e,
                        "Rule failed"
                    );
                }
            }
        }
        matched_decoders.into_iter().collect()
    }

    /// Cache empty result for a contract that failed identification.
    async fn cache_empty(&self, contract_address: Felt) {
        {
            let mut cache = self.cache.write().await;
            cache.insert(contract_address, Vec::new());
        }
        if let Err(e) = self
            .engine_db
            .set_contract_decoders(contract_address, &[])
            .await
        {
            tracing::warn!(
                target: "torii::etl::identification",
                contract = %format!("{:#x}", contract_address),
                error = %e,
                "Failed to persist empty contract identification"
            );
        }
    }

    /// Cache and persist identification result.
    async fn cache_and_persist(&self, contract_address: Felt, decoder_ids: Vec<DecoderId>) {
        {
            let mut cache = self.cache.write().await;
            cache.insert(contract_address, decoder_ids.clone());
        }
        if let Err(e) = self
            .engine_db
            .set_contract_decoders(contract_address, &decoder_ids)
            .await
        {
            tracing::warn!(
                target: "torii::etl::identification",
                contract = %format!("{:#x}", contract_address),
                error = %e,
                "Failed to persist contract identification"
            );
        }
    }
}

#[async_trait]
impl ContractIdentifier for ContractRegistry {
    async fn identify_contracts(
        &self,
        contract_addresses: &[Felt],
    ) -> Result<HashMap<Felt, Vec<DecoderId>>> {
        // Delegate to the inherent method
        ContractRegistry::identify_contracts(self, contract_addresses).await
    }

    fn shared_cache(&self) -> Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>> {
        ContractRegistry::shared_cache(self)
    }
}
