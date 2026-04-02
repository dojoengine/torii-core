use std::collections::HashMap;

use crate::sqlite::{BlockContextRow, SqliteExt};
use crate::PFError;
use crate::{decoding::BlockEvents, PFResult};
use itertools::Itertools;
use rusqlite::Connection;
use starknet::core::types::{EmittedEvent, Felt};

pub struct BlockTxHashes {
    pub block_number: u64,
    pub hashes: Vec<Felt>,
}

#[cfg(not(feature = "etl"))]
/// Block context information Copied from core to avoid importing the entire crate
#[derive(Debug, Clone, Default)]
pub struct BlockContext {
    pub number: u64,
    pub hash: Felt,
    pub parent_hash: Felt,
    pub timestamp: u64,
}

#[cfg(feature = "etl")]
pub use torii::etl::extractor::BlockContext;

impl From<BlockContextRow> for BlockContext {
    fn from(value: BlockContextRow) -> Self {
        Self {
            number: value.number,
            hash: Felt::from_bytes_be_slice(&value.hash),
            parent_hash: Felt::from_bytes_be_slice(&value.parent_hash),
            timestamp: value.timestamp,
        }
    }
}

impl BlockTxHashes {
    pub fn new(block_number: u64) -> Self {
        Self {
            block_number,
            hashes: Vec::new(),
        }
    }
}

pub trait EmittedEventExt {
    fn with_block_hash(self, block_hashes: &HashMap<u64, Felt>) -> PFResult<EmittedEvent>;
}

impl EmittedEventExt for EmittedEvent {
    fn with_block_hash(mut self, block_hashes: &HashMap<u64, Felt>) -> PFResult<EmittedEvent> {
        let block_number = self.block_number.unwrap();
        match block_hashes.get(&block_number) {
            Some(block_hash) => self.block_hash = Some(*block_hash),
            None => return Err(PFError::block_hash_missing(block_number)),
        }
        Ok(self)
    }
}

pub trait EmittedEventsExt {
    fn with_block_hashes(self, block_hashes: &HashMap<u64, Felt>) -> PFResult<Vec<EmittedEvent>>;
}

impl EmittedEventsExt for Vec<EmittedEvent> {
    fn with_block_hashes(self, block_hashes: &HashMap<u64, Felt>) -> PFResult<Vec<EmittedEvent>> {
        self.into_iter()
            .map(|event| event.with_block_hash(block_hashes))
            .collect()
    }
}

pub trait EventFetcher {
    fn get_events_for_blocks(&self, from_block: u64, to_block: u64) -> PFResult<Vec<BlockEvents>>;
    fn get_tx_hashes_for_blocks(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> PFResult<Vec<BlockTxHashes>>;
    fn get_emitted_events_wo_block_hashes(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> PFResult<Vec<EmittedEvent>>;
    fn get_emitted_events(&self, from_block: u64, to_block: u64) -> PFResult<Vec<EmittedEvent>>;
    fn get_emitted_events_with_context(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> PFResult<(Vec<BlockContext>, Vec<EmittedEvent>)>;
}

impl EventFetcher for Connection {
    fn get_events_for_blocks(&self, from_block: u64, to_block: u64) -> PFResult<Vec<BlockEvents>> {
        self.get_block_events_rows(from_block, to_block)?
            .into_iter()
            .map(BlockEvents::try_from)
            .collect()
    }
    fn get_tx_hashes_for_blocks(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> PFResult<Vec<BlockTxHashes>> {
        let mut values = Vec::with_capacity((to_block - from_block + 1) as usize);
        let mut current = BlockTxHashes::new(from_block);
        for row in self.get_block_tx_hash_rows(from_block, to_block)? {
            let (block_number, hash) = row.into();
            if block_number != current.block_number {
                values.push(current);
                current = BlockTxHashes::new(block_number);
            }
            current.hashes.push(hash);
        }
        values.push(current);
        Ok(values)
    }
    fn get_emitted_events_wo_block_hashes(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> PFResult<Vec<EmittedEvent>> {
        let block_events = self.get_events_for_blocks(from_block, to_block)?;
        let mut tx_hashes = self
            .get_block_tx_hash_rows(from_block, to_block)?
            .into_iter()
            .map_into();
        let mut emitted_events = Vec::new();
        for block in block_events {
            for transaction in block.transactions {
                let transaction_hash = tx_hashes
                    .next()
                    .filter(|(tx_bn, _)| tx_bn == &block.block_number)
                    .ok_or_else(|| PFError::tx_hash_mismatch(block.block_number))?
                    .1;
                for event in transaction {
                    emitted_events.push(EmittedEvent {
                        block_hash: None,
                        block_number: Some(block.block_number),
                        data: event.data,
                        from_address: event.from_address,
                        keys: event.keys,
                        transaction_hash,
                    });
                }
            }
        }
        Ok(emitted_events)
    }
    fn get_emitted_events(&self, from_block: u64, to_block: u64) -> PFResult<Vec<EmittedEvent>> {
        let block_hashes: HashMap<u64, Felt> = self
            .get_block_hash_rows(from_block, to_block)?
            .into_iter()
            .map_into()
            .collect();
        self.get_emitted_events_wo_block_hashes(from_block, to_block)?
            .with_block_hashes(&block_hashes)
    }
    fn get_emitted_events_with_context(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> PFResult<(Vec<BlockContext>, Vec<EmittedEvent>)> {
        let events = self.get_emitted_events_wo_block_hashes(from_block, to_block)?;
        let block_contexts: Vec<BlockContext> = self
            .get_block_context_rows(from_block, to_block)?
            .into_iter()
            .map_into()
            .collect_vec();
        let block_hashes: HashMap<u64, Felt> = block_contexts
            .iter()
            .map(|ctx| (ctx.number, ctx.hash))
            .collect();
        Ok((block_contexts, events.with_block_hashes(&block_hashes)?))
    }
}
