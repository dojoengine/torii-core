use async_trait::async_trait;
use starknet::{
    core::types::{
        requests::GetEventsRequest, BlockId, EmittedEvent, EventFilter, EventFilterWithPage,
        EventsPage, Felt, ResultPageRequest,
    },
    providers::{
        jsonrpc::HttpTransport, JsonRpcClient, Provider, ProviderError, ProviderRequestData,
    },
};
use std::{mem, ops::Deref, sync::Arc};

use crate::etl::{
    cursor::Cursor,
    extractor::{
        BlockTransactionEvents, ContractsExtractor, EventData, FeltMap, ProviderResult, RetryPolicy,
    },
    ContractEvents, TransactionEvents,
};

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
}

impl Deref for ContractsTransaction {
    type Target = FeltMap<TransactionEvents>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn extract_contract_batch(
    pending_tx: Option<TransactionEvents>,
    mut events: Vec<EmittedEvent>,
    cursor: &mut Cursor,
) -> (Vec<TransactionEvents>, Option<TransactionEvents>) {
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
        return (Vec::new(), pending_tx);
    };
    let block_number = events[n].block_number.unwrap();
    events.truncate(n + 1);

    let mut events_iter = events.into_iter();
    let mut pending_tx = match pending_tx {
        Some(tx) => tx,
        None => events_iter.next().unwrap().into(),
    };
    for event in events_iter {
        if pending_tx.transaction_hash == event.transaction_hash {
            pending_tx.events.push(event.into());
        } else {
            if pending_tx.block_number == block_number {
                cursor.append_tx(pending_tx.transaction_hash);
            }
            transactions.push(pending_tx);
            pending_tx = event.into();
        }
    }
    (transactions, Some(pending_tx))
}
#[derive(Debug, Clone)]
pub struct BasicContractEventExtractor<P> {
    provider: P,
    retry_policy: RetryPolicy,
    contracts: FeltMap<ContractState>,
    contract_addresses: Vec<Felt>,
    contracts_remaining: Vec<Felt>,
    continuation_token: Option<String>,
    pending_tx: Option<TransactionEvents>,
    start_block: u64,
    end_block: Option<u64>,
    chunk_size: u64,
    parallelisation: u32,
}

impl<P: Provider + Clone> BasicContractEventExtractor<P> {
    pub fn take_contract_state(&mut self, address: &Felt) -> ContractState {
        match self.contracts.remove(address) {
            Some(state) => state,
            None => ContractState {
                address: *address,
                cursor: Cursor::new(self.start_block, Vec::new()),
                pending_tx: None,
                continuation_token: None,
                keys: None,
            },
        }
    }

    pub async fn get_contract_events(
        &mut self,
        state: &mut ContractState,
    ) -> ProviderResult<Vec<TransactionEvents>> {
        let page = self
            .get_events(
                state.make_filter(self.end_block),
                state.continuation_token.clone(),
                self.chunk_size,
            )
            .await?;
        let (transactions, pending) = extract_contract_batch(
            mem::take(&mut state.pending_tx),
            page.events,
            &mut state.cursor,
        );
        state.pending_tx = pending;
        state.continuation_token = page.continuation_token;
        Ok(transactions)
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
    type Error = ProviderError;

    async fn next_events(&mut self) -> Result<Vec<ContractEvents>, Self::Error> {}
}
