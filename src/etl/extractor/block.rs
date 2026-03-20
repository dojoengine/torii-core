use crate::etl::extractor::{BlockExtractor, RetryPolicy};
use async_trait::async_trait;
use starknet::{
    core::types::{BlockId, BlockWithReceipts, MaybePreConfirmedBlockWithReceipts, StarknetError},
    providers::{Provider, ProviderError},
};
use std::time::Duration;

pub type ProviderResult<T> = Result<T, ProviderError>;

const BLOCK_NOT_FOUND_ERROR: ProviderError =
    ProviderError::StarknetError(StarknetError::BlockNotFound);

pub struct ConfirmedBlockExtractor<P> {
    provider: P,
    retry_policy: RetryPolicy,
    next_block: u64,
    batch_size: u64,
    poll_interval: Duration,
    current_head: u64,
}

impl<P: Provider + Clone> ConfirmedBlockExtractor<P> {
    pub async fn fetch_maybe_block_with_receipts(
        &self,
        block_number: u64,
    ) -> ProviderResult<MaybePreConfirmedBlockWithReceipts> {
        self.retry_policy
            .get_block_with_receipts(self.provider.clone(), BlockId::Number(block_number))
            .await
    }
    pub async fn fetch_head_block_number(&self) -> ProviderResult<u64> {
        self.retry_policy.block_number(self.provider.clone()).await
    }
}

#[async_trait]
impl<P: Provider + Clone + Send + Sync> BlockExtractor for ConfirmedBlockExtractor<P> {
    type Error = ProviderError;
    async fn fetch_block_with_receipts(
        &self,
        block_number: u64,
    ) -> ProviderResult<BlockWithReceipts> {
        match self.fetch_maybe_block_with_receipts(block_number).await? {
            MaybePreConfirmedBlockWithReceipts::Block(block) => Ok(block),
            MaybePreConfirmedBlockWithReceipts::PreConfirmedBlock(_) => Err(BLOCK_NOT_FOUND_ERROR),
        }
    }
    async fn wait_for_block_with_receipts(
        &self,
        block_number: u64,
    ) -> ProviderResult<BlockWithReceipts> {
        loop {
            match self.fetch_maybe_block_with_receipts(block_number).await? {
                MaybePreConfirmedBlockWithReceipts::Block(block) => return Ok(block),
                MaybePreConfirmedBlockWithReceipts::PreConfirmedBlock(_) => {
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }
    async fn fetch_head(&self) -> ProviderResult<u64> {
        self.fetch_head_block_number().await
    }
    fn set_head(&mut self, block_number: u64) {
        self.current_head = block_number;
    }
    fn current_head(&self) -> u64 {
        self.current_head
    }
    fn next_block_to_fetch(&self) -> u64 {
        self.next_block
    }
    fn set_next_block(&mut self, next_block: u64) {
        self.next_block = next_block;
    }
}
