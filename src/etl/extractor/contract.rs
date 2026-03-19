use async_trait::async_trait;
use starknet::{
    core::types::{
        requests::GetEventsRequest, BlockId, EventFilter, EventFilterWithPage, EventsPage, Felt,
        ResultPageRequest,
    },
    providers::{
        jsonrpc::HttpTransport, JsonRpcClient, Provider, ProviderError, ProviderRequestData,
    },
};
use std::{mem, sync::Arc};

use crate::etl::{cursor::Cursor, extractor::TransactionEvents};

pub type ProviderResult<T> = Result<T, ProviderError>;

#[async_trait]
pub trait ContractEventExtractor {
    type Error;
    async fn extract(&mut self) -> Result<TransactionEvents, Self::Error>;
    fn cursor(&self) -> &Cursor;
    fn contract_address(&self) -> Felt;
    fn finished(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct BasicContractEventExtractor {
    contract_address: Felt,
    continuation_token: Option<String>,
    cursor: Cursor,
    last_tx: Option<TransactionEvents>,
    provider: Arc<JsonRpcClient<HttpTransport>>,
    start_block: u64,
    end_block: u64,
    chunk_size: u64,
    filter: EventFilter,
}

impl BasicContractEventExtractor {
    pub fn build_request(&self) -> ProviderRequestData {
        ProviderRequestData::GetEvents(GetEventsRequest {
            filter: EventFilterWithPage {
                event_filter: EventFilter {
                    from_block: Some(BlockId::Number(self.start_block)),
                    to_block: Some(BlockId::Number(self.end_block)),
                    address: None,
                    keys: None,
                },
                result_page_request: ResultPageRequest {
                    continuation_token: self.continuation_token.clone(),
                    chunk_size: self.chunk_size,
                },
            },
        })
    }

    pub async fn get_events(&self) -> ProviderResult<EventsPage> {
        self.provider
            .get_events(
                self.filter,
                self.continuation_token.clone(),
                self.chunk_size,
            )
            .await
    }
}

#[async_trait]
impl ContractEventExtractor for BasicContractEventExtractor {
    type Error = ProviderError;
    fn contract_address(&self) -> Felt {
        self.contract_address
    }
    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
    async fn extract(&mut self) -> ProviderResult<TransactionEvents> {
        let events_page = self.get_events().await?;
        let last_block = events_page
            .events
            .last()
            .and_then(|e| e.block_number)
            .unwrap_or(self.cursor.block_number());
        let mut transactions = Vec::new();
        let mut last_tx = mem::take(&mut self.last_tx).unwrap_or_default();
        for event in events_page.events {
            if last_tx.transaction_hash == event.transaction_hash {
                last_tx.events.push(EventData {
                    keys: event.keys,
                    data: event.data,
                });
            } else {
                if !last_tx.events.is_empty() {
                    transactions.push(last_tx);
                }
                last_tx = event.into();
            }
        }
        self.continuation_token = events_page.continuation_token;
        Ok(transactions)
    }
}
