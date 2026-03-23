use crate::etl::{
    cursor::Cursor,
    extractor::{BlockTransactionEvents, ContractsExtractor, FeltMap, ProviderResult, RetryPolicy},
    ContractEvents, TransactionEvents,
};
use ahash::HashMapExt;
use async_trait::async_trait;
use itertools::Itertools;
use starknet::{
    core::types::{BlockId, EmittedEvent, EventFilter, EventsPage, Felt},
    providers::{Provider, ProviderError},
};
use std::{collections::BinaryHeap, ops::Deref};

#[derive(Debug, thiserror::Error)]
pub enum FetcherError {
    #[error(transparent)]
    ProviderError(#[from] ProviderError),
    #[error("Contract not found {0}")]
    ContractNotFound(Felt),
}

pub type FetcherResult<T> = std::result::Result<T, FetcherError>;

#[async_trait]
pub trait ContractEventExtractor {
    type Error;
    async fn extract(&mut self) -> Result<BlockTransactionEvents, Self::Error>;
    fn cursor(&self) -> &Cursor;
    fn contract_addresses(&self) -> &[Felt];
    fn finished(&self) -> bool;
}

pub struct ContractsTransaction(FeltMap<TransactionEvents>);

#[derive(Debug, Clone, Default)]
pub struct ContractState {
    pub address: Felt,
    pub cursor: Cursor,
    pub pending_tx: Option<TransactionEvents>,
    pub continuation_token: Option<String>,
    pub keys: Option<Vec<Vec<Felt>>>,
}

impl ContractState {
    fn make_filter(&self, to_block: Option<u64>) -> EventFilter {
        let from_block = Some(self.cursor.block_id());
        EventFilter {
            from_block,
            to_block: to_block.map(BlockId::Number),
            address: Some(self.address),
            keys: self.keys.clone(),
        }
    }

    fn to_address_position(&self) -> AddressPosition {
        AddressPosition {
            address: self.address,
            position: self.cursor.block_number(),
        }
    }
}

impl Deref for ContractsTransaction {
    type Target = FeltMap<TransactionEvents>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn extract_contract_batch(
    maybe_pending_tx: &mut Option<TransactionEvents>,
    mut events: Vec<EmittedEvent>,
    cursor: &mut Cursor,
    end: u64,
) -> Vec<TransactionEvents> {
    let mut transactions = Vec::new();

    // let mut n = events.len();
    // let block_number = loop {
    //     if n == 0 {
    //         return None;
    //     }
    //     n -= 1;
    //     if let Some(block_number) = &events[n].block_number{
    //         break *block_number;
    //     }
    // };
    // events.truncate(n + 1);

    let Some(n) = events.iter().rposition(|e| e.block_number.is_some()) else {
        return Vec::new();
    };
    let mut block_number = events[n].block_number.unwrap();
    if block_number > end {
        block_number = end + 1;
    }
    events.truncate(n + 1);
    cursor.update_block_number(block_number);

    let mut events_iter = events.into_iter();
    if maybe_pending_tx.is_none() {
        *maybe_pending_tx = events_iter.next().map(Into::into);
    }
    for event in events_iter {
        let mut pending_tx = maybe_pending_tx.take().unwrap();
        if pending_tx.transaction_hash == event.transaction_hash {
            pending_tx.events.push(event.into());
        } else {
            if pending_tx.block_number == block_number {
                cursor.append_tx(pending_tx.transaction_hash);
            }
            transactions.push(pending_tx);
            if event.block_number.unwrap() > end {
                break;
            }
            *maybe_pending_tx = Some(event.into());
        }
    }
    transactions
}

#[derive(Debug)]
pub struct BasicContractEventExtractor<P> {
    provider: P,
    retry_policy: RetryPolicy,
    chunk_size: u64,
    parallelisation: u32,
    end_block: Option<u64>,
    contracts: FeltMap<ContractState>,
    queue: BinaryHeap<AddressPosition>,
    at_head: bool,
}

pub struct ContractConfig {
    address: Felt,
    start_block: u64,
    keys: Option<Vec<Vec<Felt>>>,
}

impl From<ContractConfig> for ContractState {
    fn from(value: ContractConfig) -> Self {
        ContractState {
            address: value.address,
            cursor: Cursor::new(value.start_block),
            pending_tx: None,
            continuation_token: None,
            keys: value.keys,
        }
    }
}

impl<P: Provider + Clone> BasicContractEventExtractor<P> {
    pub fn new(
        provider: P,
        retry_policy: RetryPolicy,
        parallelisation: u32,
        chunk_size: u64,
        end_block: Option<u64>,
        contracts: Vec<ContractConfig>,
    ) -> Self {
        let mut extractor = Self {
            provider,
            retry_policy,
            end_block,
            chunk_size,
            parallelisation,
            at_head: false,
            contracts: FeltMap::with_capacity(contracts.len()),
            queue: BinaryHeap::with_capacity(contracts.len()),
        };
        for contract in contracts {
            let state: ContractState = contract.into();
            extractor.queue.push(state.to_address_position());
            extractor.contracts.insert(state.address, state);
        }
        extractor
    }

    pub fn take_contract_state(&mut self, address: &Felt) -> FetcherResult<ContractState> {
        self.contracts
            .remove(address)
            .ok_or_else(|| FetcherError::ContractNotFound(*address))
    }

    pub async fn get_contract_events(
        &self,
        state: &mut ContractState,
    ) -> FetcherResult<ContractEvents> {
        let page = self
            .get_events(
                state.make_filter(self.end_block),
                state.continuation_token.clone(),
                self.chunk_size,
            )
            .await?;
        state.continuation_token = page.continuation_token;
        let events = extract_contract_batch(
            &mut state.pending_tx,
            page.events,
            &mut state.cursor,
            self.end_block.unwrap_or(u64::MAX),
        );
        Ok(ContractEvents::new(state.address, events))
    }

    pub fn take_next_states(&mut self) -> Vec<ContractState> {
        let mut states = Vec::new();
        while states.len() < self.parallelisation as usize {
            if let Some(address) = self.queue.pop() {
                if let Some(state) = self.contracts.remove(&address.address) {
                    states.push(state);
                }
            }
        }
        states
    }

    pub fn update_state(&mut self, state: ContractState) {
        if let Some(end) = self.end_block {
            if state.cursor.block_number() <= end {
                self.queue.push(state.to_address_position());
            }
        }
        self.contracts.insert(state.address, state);
    }

    pub async fn get_events(
        &self,
        filter: EventFilter,
        continuation_token: Option<String>,
        chunk_size: u64,
    ) -> ProviderResult<EventsPage> {
        self.retry_policy
            .get_events(
                self.provider.clone(),
                filter,
                continuation_token,
                chunk_size,
            )
            .await
    }
}

impl<P: Provider + Clone> ContractsExtractor for BasicContractEventExtractor<P> {
    type Error = FetcherError;

    async fn next_events(&mut self) -> Vec<Result<ContractEvents, Self::Error>> {
        let mut states = self.take_next_states();

        let fetches = states
            .iter_mut()
            .map(|state| self.get_contract_events(state))
            .collect_vec();
        let results = futures::future::join_all(fetches).await;
        states
            .into_iter()
            .for_each(|state| self.update_state(state));
        self.at_head = self.queue.is_empty();
        results
    }
    fn at_end_block(&self) -> bool {
        self.at_head
    }
}

// This is reversing the comparison to make the BinaryHeap a min-heap based on the block number
#[derive(Debug)]
struct AddressPosition {
    address: Felt,
    position: u64,
}

impl PartialEq for AddressPosition {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.position == other.position
    }
}

impl Eq for AddressPosition {}
impl PartialOrd for AddressPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AddressPosition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.position.cmp(&other.position).reverse()
    }
}
