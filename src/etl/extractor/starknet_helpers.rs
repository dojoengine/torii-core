use anyhow::{Context, Result};
use starknet::core::types::requests::GetClassAtRequest;
use starknet::core::types::{
    requests::GetBlockWithReceiptsRequest, BlockId, ContractClass, DeclareTransactionContent, DeployAccountTransactionContent, EmittedEvent, Felt, InvokeTransactionContent,
    MaybePreConfirmedBlockWithReceipts, TransactionContent, TransactionReceipt,
};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};

use super::{BlockContext, BlockData, DeclaredClass, DeployedContract, TransactionContext};


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
pub fn block_with_receipts_batch_from_block_range(from_block: u64, to_block: u64) -> Vec<ProviderRequestData> {
    let requests = (from_block..=to_block)
        .map(|block_num| {
            ProviderRequestData::GetBlockWithReceipts(GetBlockWithReceiptsRequest {
                block_id: BlockId::Number(block_num),
            })
        })
        .collect();

    requests
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
        .map(|&class_hash| ProviderRequestData::GetClassAt(GetClassAtRequest {
            block_id: BlockId::Tag(starknet::core::types::BlockTag::Latest),
            contract_address: class_hash,
        }))
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
                anyhow::bail!(
                    "Unexpected response type for class {}: expected GetClass",
                    class_hash
                );
            }
        }
    }

    Ok(classes)
}

/// Converts a block with receipts into structured block data.
///
/// This decouples the RPC types from the internal types and extracts all relevant information:
/// - Block context (number, hash, timestamp, etc.)
/// - Transaction contexts (sender, calldata)
/// - Events emitted by transactions
/// - Declared classes (from Declare transactions)
/// - Deployed contracts (from Deploy and DeployAccount transactions)
///
/// This function doesn't support pre-confirmed blocks, only mined blocks.
/// Consumes the block and its content to avoid copying the data.
///
/// # Arguments
///
/// * `block` - The block with receipts
///
/// # Returns
///
/// A `BlockData` structure containing all extracted information.
pub fn block_into_contexts(block: MaybePreConfirmedBlockWithReceipts) -> Result<BlockData> {
    // Skip pending/pre-confirmed blocks
    let block_with_receipts = match block {
        MaybePreConfirmedBlockWithReceipts::Block(b) => b,
        MaybePreConfirmedBlockWithReceipts::PreConfirmedBlock(_) => {
            tracing::debug!(
                target: "torii::etl::block_range",
                "Skipping pre-confirmed block"
            );
            // TODO: implement custom error type for the extractor module.
            return Err(anyhow::anyhow!("pre-confirmed block not supported by block_into_context"));
        }
    };

    let block_context = BlockContext {
        number: block_with_receipts.block_number,
        hash: block_with_receipts.block_hash,
        parent_hash: block_with_receipts.parent_hash,
        timestamp: block_with_receipts.timestamp,
    };

    let transactions = block_with_receipts.transactions;

    let mut all_events = Vec::new();
    let mut transaction_contexts = Vec::new();
    let mut declared_classes = Vec::new();
    let mut deployed_contracts = Vec::new();

    for tx_with_receipt in transactions {
        let tx = tx_with_receipt.transaction;
        let receipt = tx_with_receipt.receipt;

        let tx_hash = match &receipt {
            TransactionReceipt::Invoke(r) => r.transaction_hash,
            TransactionReceipt::L1Handler(r) => r.transaction_hash,
            TransactionReceipt::Declare(r) => r.transaction_hash,
            TransactionReceipt::Deploy(r) => r.transaction_hash,
            TransactionReceipt::DeployAccount(r) => r.transaction_hash,
        };

        // Extract transaction data based on type, including metadata for declares and deploys
        let (sender_address, calldata, declare_info, deploy_account_class) = match tx {
            TransactionContent::Invoke(content) => match content {
                InvokeTransactionContent::V0(c) => {
                    // V0 doesn't have explicit sender, use contract_address
                    (Some(c.contract_address), c.calldata, None, None)
                }
                InvokeTransactionContent::V1(c) => (Some(c.sender_address), c.calldata, None, None),
                InvokeTransactionContent::V3(c) => (Some(c.sender_address), c.calldata, None, None),
            },
            TransactionContent::L1Handler(content) => {
                (Some(content.contract_address), content.calldata, None, None)
            }
            TransactionContent::Declare(content) => match content {
                DeclareTransactionContent::V0(c) => {
                    (Some(c.sender_address), Vec::new(), Some((c.class_hash, None)), None)
                }
                DeclareTransactionContent::V1(c) => {
                    (Some(c.sender_address), Vec::new(), Some((c.class_hash, None)), None)
                }
                DeclareTransactionContent::V2(c) => (
                    Some(c.sender_address),
                    Vec::new(),
                    Some((c.class_hash, Some(c.compiled_class_hash))),
                    None,
                ),
                DeclareTransactionContent::V3(c) => (
                    Some(c.sender_address),
                    Vec::new(),
                    Some((c.class_hash, Some(c.compiled_class_hash))),
                    None,
                ),
            },
            TransactionContent::Deploy(content) => {
                // Old deploy transactions don't have a sender
                (None, content.constructor_calldata, None, None)
            }
            TransactionContent::DeployAccount(content) => match content {
                DeployAccountTransactionContent::V1(c) => {
                    (None, c.constructor_calldata, None, Some(c.class_hash))
                }
                DeployAccountTransactionContent::V3(c) => {
                    (None, c.constructor_calldata, None, Some(c.class_hash))
                }
            },
        };

        // Build transaction context
        transaction_contexts.push(TransactionContext {
            hash: tx_hash,
            block_number: block_with_receipts.block_number,
            sender_address,
            calldata,
        });

        // Extract declared classes from Declare transactions
        if let Some((class_hash, compiled_class_hash)) = declare_info {
            declared_classes.push(DeclaredClass {
                class_hash,
                compiled_class_hash,
                transaction_hash: tx_hash,
            });
        }

        // Extract deployed contracts from Deploy and DeployAccount receipts
        match &receipt {
            TransactionReceipt::Deploy(deploy_receipt) => {
                deployed_contracts.push(DeployedContract {
                    contract_address: deploy_receipt.contract_address,
                    class_hash: Felt::ZERO, // Old Deploy doesn't have class_hash accessible easily
                    transaction_hash: tx_hash,
                });
            }
            TransactionReceipt::DeployAccount(deploy_account_receipt) => {
                deployed_contracts.push(DeployedContract {
                    contract_address: deploy_account_receipt.contract_address,
                    class_hash: deploy_account_class.unwrap_or(Felt::ZERO),
                    transaction_hash: tx_hash,
                });
            }
            _ => {}
        }

        // Extract events from receipt
        let events = match receipt {
            TransactionReceipt::Invoke(r) => r.events,
            TransactionReceipt::L1Handler(r) => r.events,
            TransactionReceipt::Declare(r) => r.events,
            TransactionReceipt::Deploy(r) => r.events,
            TransactionReceipt::DeployAccount(r) => r.events,
        };

        // Convert to EmittedEvent format - move ownership to avoid cloning
        for event in events {
            all_events.push(EmittedEvent {
                from_address: event.from_address,
                keys: event.keys,
                data: event.data,
                block_hash: Some(block_with_receipts.block_hash),
                block_number: Some(block_with_receipts.block_number),
                transaction_hash: tx_hash,
            });
        }
    }

    Ok(BlockData {
        block_context,
        transactions: transaction_contexts,
        events: all_events,
        declared_classes,
        deployed_contracts,
    })
}
