use anyhow::{Context, Result};
use starknet::core::types::contract::{AbiEntry, TypedAbiEvent};
use starknet::core::types::requests::GetClassAtRequest;
use starknet::core::types::{
    requests::GetBlockWithReceiptsRequest, BlockId, ContractClass, ExecutionResult, Felt,
    TransactionReceipt,
};
use starknet::core::types::{
    EventFilter, EventsPage, LegacyContractAbiEntry, MaybePreConfirmedBlockWithReceipts,
};
use starknet::providers::{Provider, ProviderError, ProviderRequestData, ProviderResponseData};
use std::collections::HashSet;

use crate::etl::extractor::RetryPolicy;

pub type ProviderResult<T> = Result<T, ProviderError>;

#[inline]
fn is_execution_succeeded(execution_result: &ExecutionResult) -> bool {
    matches!(execution_result, ExecutionResult::Succeeded)
}

#[inline]
fn is_receipt_succeeded(receipt: &TransactionReceipt) -> bool {
    is_execution_succeeded(receipt.execution_result())
}

/// Builds a batch of `GetBlockWithReceipts` requests for a range of block numbers.
///
/// # Arguments
///
/// * `from_block` - The starting block number
/// * `to_block` - The ending block number
///
/// # Returns
///
/// A vector of `ProviderRequestData` requests.
pub fn block_with_receipts_batch_from_block_range(
    from_block: u64,
    to_block: u64,
) -> Vec<ProviderRequestData> {
    (from_block..=to_block)
        .map(|block_num| {
            ProviderRequestData::GetBlockWithReceipts(GetBlockWithReceiptsRequest {
                block_id: BlockId::Number(block_num),
            })
        })
        .collect()
}

/// Fetches contract classes for a list of class hashes using batch requests.
///
/// This is useful for inspecting ABIs to determine contract types (ERC20, ERC721, etc.)
/// or to build selector → function name mappings.
///
/// # Arguments
///
/// * `provider` - The provider to fetch classes from
/// * `class_hashes` - The class hashes to fetch
///
/// # Returns
///
/// A vector of tuples containing (class_hash, ContractClass).
/// If a class is not found, it will return an error.
///
/// # Example
///
/// ```rust,ignore
/// let declared = vec![class_hash_1, class_hash_2];
/// let classes = fetch_classes_batch(&provider, &declared).await?;
///
/// for (class_hash, class) in classes {
///     // Inspect class.abi to determine contract type
///     // Build selector mappings, etc.
/// }
/// ```
pub async fn fetch_classes_batch<P>(
    provider: &P,
    class_hashes: &[Felt],
) -> Result<Vec<(Felt, ContractClass)>>
where
    P: Provider,
{
    if class_hashes.is_empty() {
        return Ok(Vec::new());
    }

    // Build batch request
    let requests: Vec<ProviderRequestData> = class_hashes
        .iter()
        .map(|&class_hash| {
            ProviderRequestData::GetClassAt(GetClassAtRequest {
                block_id: BlockId::Tag(starknet::core::types::BlockTag::Latest),
                contract_address: class_hash,
            })
        })
        .collect();

    // Execute batch request
    let responses = provider
        .batch_requests(&requests)
        .await
        .context("Failed to fetch classes in batch")?;

    // Extract classes from responses
    let mut classes = Vec::new();
    for (idx, response) in responses.into_iter().enumerate() {
        let class_hash = class_hashes[idx];
        match response {
            ProviderResponseData::GetClass(class) => {
                classes.push((class_hash, class));
            }
            _ => {
                anyhow::bail!("Unexpected response type for class {class_hash}: expected GetClass");
            }
        }
    }

    Ok(classes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_success_predicate_handles_succeeded_and_reverted() {
        assert!(is_execution_succeeded(&ExecutionResult::Succeeded));
        assert!(!is_execution_succeeded(&ExecutionResult::Reverted {
            reason: "reverted".to_string(),
        }));
    }
}

/// Parsed contract ABI
///
/// Simplified representation of a contract's ABI for identification purposes.
/// Contains function and event signatures extracted from the contract class.
#[derive(Debug, Clone)]
pub struct ContractAbi {
    pub abi: Option<Vec<AbiEntry>>,
    pub legacy_abi: Option<Vec<LegacyContractAbiEntry>>,
    functions: HashSet<String>,
    events: HashSet<String>,
}

impl ContractAbi {
    fn extract_function_name(name: &str) -> String {
        name.to_string()
    }

    fn extract_event_name(name: &str) -> String {
        name.to_string()
    }

    #[allow(clippy::match_wildcard_for_single_variants)]
    pub fn from_contract_class(class: ContractClass) -> Result<Self> {
        let mut abi: Option<Vec<AbiEntry>> = None;
        let mut legacy_abi: Option<Vec<LegacyContractAbiEntry>> = None;
        let mut functions = HashSet::new();
        let mut events = HashSet::new();

        match class {
            ContractClass::Sierra(sierra) => {
                let parsed_abi: Vec<AbiEntry> = serde_json::from_str(&sierra.abi)?;
                for entry in &parsed_abi {
                    match entry {
                        AbiEntry::Function(func) => {
                            functions.insert(Self::extract_function_name(&func.name));
                        }
                        AbiEntry::Interface(interface) => {
                            for item in &interface.items {
                                if let AbiEntry::Function(func) = item {
                                    functions.insert(Self::extract_function_name(&func.name));
                                }
                            }
                        }
                        AbiEntry::Event(event) => {
                            use starknet::core::types::contract::AbiEvent;
                            match event {
                                AbiEvent::Typed(TypedAbiEvent::Struct(s)) => {
                                    events.insert(Self::extract_event_name(&s.name));
                                }
                                AbiEvent::Typed(TypedAbiEvent::Enum(e)) => {
                                    events.insert(Self::extract_event_name(&e.name));
                                    for variant in &e.variants {
                                        events.insert(Self::extract_event_name(&variant.name));
                                    }
                                }
                                AbiEvent::Untyped(u) => {
                                    events.insert(Self::extract_event_name(&u.name));
                                }
                            }
                        }
                        _ => {}
                    }
                }
                abi = Some(parsed_abi);
            }
            ContractClass::Legacy(legacy) => {
                if let Some(ref legacy_abi_vec) = legacy.abi {
                    for entry in legacy_abi_vec {
                        match entry {
                            LegacyContractAbiEntry::Function(func) => {
                                functions.insert(Self::extract_function_name(&func.name));
                            }
                            LegacyContractAbiEntry::Event(event) => {
                                events.insert(Self::extract_event_name(&event.name));
                            }
                            _ => {}
                        }
                    }
                }
                legacy_abi = legacy.abi;
            }
        }

        Ok(Self {
            abi,
            legacy_abi,
            functions,
            events,
        })
    }

    pub fn has_function(&self, name: &str) -> bool {
        if self.functions.contains(name) {
            return true;
        }
        let suffix = format!("::{name}");
        self.functions.iter().any(|f| f.ends_with(&suffix))
    }

    pub fn has_event(&self, name: &str) -> bool {
        if self.events.contains(name) {
            return true;
        }
        let suffix = format!("::{name}");
        self.events.iter().any(|e| e.ends_with(&suffix))
    }
}

impl RetryPolicy {
    pub async fn get_events<P: Provider + Clone>(
        &self,
        provider: P,
        filter: EventFilter,
        continuation_token: Option<String>,
        chunk_size: u64,
    ) -> ProviderResult<EventsPage> {
        self.execute(|| {
            let provider = provider.clone();
            let filter = filter.clone();
            let continuation_token = continuation_token.clone();
            async move {
                provider
                    .get_events(filter, continuation_token, chunk_size)
                    .await
            }
        })
        .await
    }

    pub async fn get_block_with_receipts<P: Provider + Clone>(
        &self,
        provider: P,
        block_id: BlockId,
    ) -> ProviderResult<MaybePreConfirmedBlockWithReceipts> {
        self.execute(|| {
            let provider = provider.clone();
            async move { provider.get_block_with_receipts(block_id).await }
        })
        .await
    }

    pub async fn block_number<P: Provider + Clone>(&self, provider: P) -> ProviderResult<u64> {
        self.execute(|| {
            let provider = provider.clone();
            async move { provider.block_number().await }
        })
        .await
    }
}
