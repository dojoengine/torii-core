//! Contract registry for managing contract identification and decoder routing
//!
//! The registry maintains a mapping of contract addresses to decoder IDs,
//! using pluggable identification rules to determine which decoders should
//! handle events from each contract.
//!
//! # Provider Type Decision
//!
//! This registry uses a **concrete provider type** (`JsonRpcClient<HttpTransport>`)
//! instead of being generic over `P: Provider`. Here's why:
//!
//! ## Why not generic `P: Provider`?
//!
//! 1. **Trait object-safety**: `Provider` trait is not dyn-compatible due to
//!    generic methods, making `Arc<dyn Provider>` impossible.
//!
//! 2. **Generics cascade**: Making registry generic (`ContractRegistry<P>`)
//!    would force `DecoderContext<P>`, `ToriiConfig<P>`, and all downstream
//!    code to become generic, significantly complicating the API.
//!
//! 3. **Pragmatic choice**: 99% of users will use `JsonRpcClient<HttpTransport>`.
//!    For the 1% who need custom providers, they can fork or modify this code.
//!
//! 4. **Performance**: Avoiding event iteration overhead (thousands of events)
//!    to collect contract addresses is more important than provider flexibility.
//!    Identification happens lazily during decode, making the registry need
//!    direct access to a provider.
//!
//! # Identification Methods
//!
//! 1. **SRC-5**: Rules provide `(interface_id, decoder_ids)`. Registry calls
//!    `supports_interface()` on contracts and maps matching contracts.
//!
//! 2. **ABI Heuristics**: Rules inspect contract ABIs for function/event patterns
//!    and return matching decoder IDs.
//!
//! # Example
//!
//! ```rust,ignore
//! use starknet::providers::jsonrpc::{JsonRpcClient, HttpTransport};
//!
//! // Create provider
//! let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));
//!
//! // Create registry with provider
//! let mode = ContractIdentificationMode::SRC5 | ContractIdentificationMode::ABI_HEURISTICS;
//! let mut registry = ContractRegistry::with_provider(mode, provider);
//! registry.add_rule(Box::new(Erc20Rule));
//!
//! // Identification happens automatically during decode
//! let decoder_ids = registry.ensure_identified_and_get_decoders(contract).await?;
//! ```

use anyhow::{Context, Result};
use bitflags::bitflags;
use starknet::core::types::Felt;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::Provider;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use super::starknet_helpers::ContractAbi;

bitflags! {
    /// Contract identification mode configuration
    ///
    /// Controls which identification methods are enabled when discovering contract types.
    /// This is a global policy configured at the Torii level, not per-rule.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mode = ContractIdentificationMode::SRC5 | ContractIdentificationMode::ABI_HEURISTICS;
    /// ```
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct ContractIdentificationMode: u32 {
        /// Use SRC-5 supports_interface() calls (strict mode)
        const SRC5 = 1 << 0;

        /// Use ABI heuristics (may have false positives)
        const ABI_HEURISTICS = 1 << 1;
    }
}

/// Decoder identifier based on decoder name hash
///
/// Similar to `EnvelopeTypeId`, this uses a hash of the decoder's name
/// to create a unique, deterministic identifier. This ensures that decoder
/// IDs remain consistent across restarts and don't depend on registration order.
///
/// # Example
///
/// ```rust,ignore
/// let erc20_decoder_id = DecoderId::new("erc20");
/// let erc721_decoder_id = DecoderId::new("erc721");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DecoderId(u64);

impl DecoderId {
    /// Creates a DecoderId from a decoder name
    pub fn new(name: &str) -> Self {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        DecoderId(hasher.finish())
    }

    /// Creates a DecoderId from a u64 value (for deserialization)
    pub fn from_u64(value: u64) -> Self {
        DecoderId(value)
    }

    /// Returns the DecoderId as a u64
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Pluggable contract identification rule
///
/// Sink/decoder developers implement this trait to teach the registry
/// how to identify contracts that their decoder can handle.
///
/// Rules provide two identification methods:
/// 1. **SRC-5**: Return interface ID + decoder IDs. Registry checks `supports_interface()`.
/// 2. **ABI Heuristics**: Inspect ABI to pattern-match contract types.
///
/// The global identification mode determines which methods are enabled.
pub trait IdentificationRule: Send + Sync {
    /// Unique name for this rule (for debugging/logging)
    fn name(&self) -> &str;

    /// Identify a contract using ABI heuristics
    ///
    /// Returns decoder IDs that should handle this contract based on
    /// function and event signatures in the ABI.
    ///
    /// # Arguments
    ///
    /// * `contract_address` - The contract address being identified
    /// * `class_hash` - The class hash of the contract
    /// * `abi` - Parsed ABI of the contract
    ///
    /// # Returns
    ///
    /// A vector of decoder IDs that should handle this contract.
    /// Empty vector means this rule does not recognize the contract.
    ///
    /// Default: Returns empty (no ABI-based identification)
    fn identify_by_abi(
        &self,
        _contract_address: Felt,
        _class_hash: Felt,
        _abi: &ContractAbi,
    ) -> Result<Vec<DecoderId>> {
        Ok(Vec::new())
    }

    /// SRC-5 interface identification
    ///
    /// Returns `(interface_id, decoder_ids)` tuple if this rule uses SRC-5.
    /// The registry will check if the contract supports the interface ID,
    /// and if so, map it to the provided decoder IDs.
    ///
    /// # Returns
    ///
    /// - `Some((interface_id, decoder_ids))`: SRC-5 interface ID and decoders to use
    /// - `None`: This rule doesn't use SRC-5
    ///
    /// Default: None (no SRC-5 interface)
    fn src5_interface(&self) -> Option<(Felt, Vec<DecoderId>)> {
        None
    }
}

/// Contract registry for decoder routing
///
/// Maintains the mapping between contract addresses and decoder IDs.
/// Uses identification rules to automatically discover contract types,
/// with support for user-provided explicit overrides.
///
/// # Provider Storage
///
/// The registry stores an optional `JsonRpcClient<HttpTransport>` for lazy
/// contract identification. If a provider is not set, identification will fail
/// (useful for testing with explicit mappings only).
pub struct ContractRegistry {
    /// Contract → decoders mapping (single source of truth)
    mappings: HashMap<Felt, Vec<DecoderId>>,

    /// Set of contracts we've already identified
    known_contracts: HashSet<Felt>,

    /// ABI cache (class_hash → ABI)
    abi_cache: HashMap<Felt, ContractAbi>,

    /// Identification rules (registered by sink developers)
    identification_rules: Vec<Box<dyn IdentificationRule>>,

    /// User explicit overrides (highest priority)
    explicit_mappings: HashMap<Felt, Vec<DecoderId>>,

    /// Global identification mode
    identification_mode: ContractIdentificationMode,

    /// RPC provider for contract identification (concrete type)
    provider: Option<Arc<JsonRpcClient<HttpTransport>>>,
}

impl ContractRegistry {
    /// Create a new contract registry without a provider
    ///
    /// Without a provider, automatic identification will not work.
    /// Only explicit mappings will be available.
    pub fn new(identification_mode: ContractIdentificationMode) -> Self {
        Self {
            mappings: HashMap::new(),
            known_contracts: HashSet::new(),
            abi_cache: HashMap::new(),
            identification_rules: Vec::new(),
            explicit_mappings: HashMap::new(),
            identification_mode,
            provider: None,
        }
    }

    /// Create a new contract registry with a provider
    ///
    /// The provider will be used for lazy contract identification during decode.
    pub fn with_provider(
        identification_mode: ContractIdentificationMode,
        provider: Arc<JsonRpcClient<HttpTransport>>,
    ) -> Self {
        Self {
            mappings: HashMap::new(),
            known_contracts: HashSet::new(),
            abi_cache: HashMap::new(),
            identification_rules: Vec::new(),
            explicit_mappings: HashMap::new(),
            identification_mode,
            provider: Some(provider),
        }
    }

    /// Add an identification rule
    pub fn add_rule(&mut self, rule: Box<dyn IdentificationRule>) {
        self.identification_rules.push(rule);
    }

    /// Add an explicit contract mapping (highest priority)
    pub fn add_explicit_mapping(&mut self, contract: Felt, decoders: Vec<DecoderId>) {
        self.explicit_mappings.insert(contract, decoders);
    }

    /// Check if a contract is already known (to avoid re-identification)
    pub fn is_known(&self, contract: Felt) -> bool {
        self.known_contracts.contains(&contract)
    }

    /// Identify a contract using registered rules
    ///
    /// This is called on first event from a contract address.
    /// It will:
    /// 1. Check explicit mappings first
    /// 2. Fetch class hash and ABI from provider
    /// 3. Run all identification rules
    /// 4. Cache the results
    pub async fn identify_contract<P>(&mut self, contract: Felt, provider: &P) -> Result<()>
    where
        P: Provider,
    {
        // Skip if already known
        if self.is_known(contract) {
            return Ok(());
        }

        // Check explicit mapping first
        if let Some(decoders) = self.explicit_mappings.get(&contract) {
            self.mappings.insert(contract, decoders.clone());
            self.known_contracts.insert(contract);
            tracing::debug!(
                target: "torii::contract_registry",
                "Contract {:?} mapped explicitly to {} decoder(s)",
                contract,
                decoders.len()
            );
            return Ok(());
        }

        // Use BTreeSet to automatically deduplicate and maintain sorted order
        let mut all_decoders = BTreeSet::new();

        // Run SRC-5 checks first if enabled
        if self
            .identification_mode
            .contains(ContractIdentificationMode::SRC5)
        {
            for rule in &self.identification_rules {
                if let Some((interface_id, decoder_ids)) = rule.src5_interface() {
                    // Check if contract supports this interface
                    match Self::check_src5_support(contract, interface_id, provider).await {
                        Ok(true) => {
                            tracing::debug!(
                                target: "torii::contract_registry",
                                "Contract {:?} supports SRC-5 interface {:?} (rule '{}')",
                                contract,
                                interface_id,
                                rule.name()
                            );
                            all_decoders.extend(decoder_ids);
                        }
                        Ok(false) => {
                            tracing::trace!(
                                target: "torii::contract_registry",
                                "Contract {:?} does not support SRC-5 interface {:?} (rule '{}')",
                                contract,
                                interface_id,
                                rule.name()
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "torii::contract_registry",
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
        //
        // NOTE: ABI heuristics is a fallback identification mechanism where the core
        // fetches the contract's ABI and delegates pattern matching to identification rules.
        //
        // The core provides infrastructure (fetching class hash, ABI, caching), while
        // the actual identification logic is implemented by each rule's `identify_by_abi()` method.
        // This keeps the core simple and flexible.
        if self
            .identification_mode
            .contains(ContractIdentificationMode::ABI_HEURISTICS)
        {
            tracing::debug!(
                target: "torii::contract_registry",
                "Fetching class hash for contract {:?}",
                contract
            );

            let class_hash = provider
                .get_class_hash_at(
                    starknet::core::types::BlockId::Tag(starknet::core::types::BlockTag::Latest),
                    contract,
                )
                .await
                .context("Failed to fetch class hash")?;

            tracing::debug!(
                target: "torii::contract_registry",
                "Fetching ABI for contract {:?}",
                contract
            );

            let abi = if let Some(cached_abi) = self.abi_cache.get(&class_hash) {
                cached_abi.clone()
            } else {
                let class = provider
                    .get_class(
                        starknet::core::types::BlockId::Tag(
                            starknet::core::types::BlockTag::Latest,
                        ),
                        class_hash,
                    )
                    .await
                    .context("Failed to fetch contract class")?;

                let abi = ContractAbi::from_contract_class(class)?;
                self.abi_cache.insert(class_hash, abi.clone());
                abi
            };

            for rule in &self.identification_rules {
                match rule.identify_by_abi(contract, class_hash, &abi) {
                    Ok(decoders) => {
                        if !decoders.is_empty() {
                            tracing::debug!(
                                target: "torii::contract_registry",
                                "Rule '{}' identified contract {:?} for {} decoder(s) via ABI",
                                rule.name(),
                                contract,
                                decoders.len()
                            );
                            all_decoders.extend(decoders);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "torii::contract_registry",
                            "Rule '{}' ABI identification failed for contract {:?}: {}",
                            rule.name(),
                            contract,
                            e
                        );
                    }
                }
            }
        }

        // Convert to sorted Vec (BTreeSet maintains order)
        let all_decoders: Vec<DecoderId> = all_decoders.into_iter().collect();

        tracing::info!(
            target: "torii::contract_registry",
            "Contract {:?} identified with {} decoder(s)",
            contract,
            all_decoders.len()
        );

        self.mappings.insert(contract, all_decoders);
        self.known_contracts.insert(contract);

        Ok(())
    }

    /// Get decoders for a contract
    ///
    /// Returns None if the contract hasn't been identified yet.
    pub fn get_decoders(&self, contract: Felt) -> Option<&[DecoderId]> {
        self.mappings.get(&contract).map(|v| v.as_slice())
    }

    /// Ensure contract is identified, then return its decoders
    ///
    /// This is the **primary method** for use in the decode loop. It atomically:
    /// 1. Checks if contract is already known
    /// 2. If not, identifies it using the stored provider
    /// 3. Returns the decoder IDs (cloned for lock-free usage)
    ///
    /// # Returns
    ///
    /// - `Ok(Some(decoder_ids))`: Contract identified, these are the decoders
    /// - `Ok(None)`: No decoders registered for this contract
    /// - `Err(_)`: Identification failed (no provider, network error, etc.)
    ///
    /// # Performance
    ///
    /// This method is designed for the decode loop where we process thousands
    /// of events. It avoids pre-iteration of events to collect contract addresses.
    /// Instead, identification happens lazily as events are encountered, with
    /// results cached for subsequent events from the same contract.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// for event in events {
    ///     let decoder_ids = registry
    ///         .ensure_identified_and_get_decoders(event.from_address)
    ///         .await?;
    ///
    ///     if let Some(ids) = decoder_ids {
    ///         // Route to decoders...
    ///     }
    /// }
    /// ```
    pub async fn ensure_identified_and_get_decoders(
        &mut self,
        contract: Felt,
    ) -> Result<Option<Vec<DecoderId>>> {
        // Fast path: already known
        if self.is_known(contract) {
            return Ok(self.get_decoders(contract).map(|ids| ids.to_vec()));
        }

        // Slow path: need to identify
        if let Some(provider) = self.provider.clone() {
            self.identify_contract(contract, provider.as_ref()).await?;
            Ok(self.get_decoders(contract).map(|ids| ids.to_vec()))
        } else {
            // No provider - can't identify
            tracing::warn!(
                target: "torii::contract_registry",
                "Cannot identify contract {:?}: no provider configured",
                contract
            );
            Ok(None)
        }
    }

    /// Get statistics about the registry
    pub fn stats(&self) -> RegistryStats {
        RegistryStats {
            known_contracts: self.known_contracts.len(),
            cached_abis: self.abi_cache.len(),
            explicit_mappings: self.explicit_mappings.len(),
            identification_rules: self.identification_rules.len(),
        }
    }

    /// Check if a contract supports an SRC-5 interface
    ///
    /// Calls `supports_interface(interface_id)` on the contract.
    async fn check_src5_support<P: Provider>(
        contract: Felt,
        interface_id: Felt,
        provider: &P,
    ) -> Result<bool> {
        // TODO: Implement SRC-5 supports_interface call
        // This requires calling the contract's supports_interface function
        // For now, return false (not supported)
        let _ = (contract, interface_id, provider);
        Ok(false)
    }
}

/// Registry statistics
#[derive(Debug, Clone)]
pub struct RegistryStats {
    pub known_contracts: usize,
    pub cached_abis: usize,
    pub explicit_mappings: usize,
    pub identification_rules: usize,
}
