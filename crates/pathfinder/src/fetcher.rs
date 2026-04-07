use std::collections::HashMap;

use crate::decoding::BlockEvents;
use crate::sqlite::{BlockContextRow, SqliteExt};
use crate::{PFError, PFResult};
use rusqlite::Connection;

#[cfg(not(feature = "etl"))]
/// Block context information Copied from core to avoid importing the entire crate
#[derive(Debug, Clone, Default)]
pub struct BlockContext {
    pub number: u64,
    pub hash: Felt,
    pub parent_hash: Felt,
    pub timestamp: u64,
}

use starknet_types_raw::event::EmittedEvent;
use starknet_types_raw::Felt;
#[cfg(feature = "etl")]
pub use torii::etl::extractor::BlockContext;

impl From<BlockContextRow> for BlockContext {
    fn from(value: BlockContextRow) -> Self {
        BlockContext {
            number: value.number,
            hash: value.hash.into(),
            parent_hash: value.parent_hash.into(),
            timestamp: value.timestamp,
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

pub trait EventFetcher {
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
    fn get_emitted_events_wo_block_hashes(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> PFResult<Vec<EmittedEvent>> {
        let total_events = self.get_number_of_events_for_blocks(from_block, to_block)?;
        let mut tx_hashes = self
            .get_block_tx_hash_rows(from_block, to_block)?
            .into_iter();
        let mut emitted_events = Vec::with_capacity(total_events as usize);

        for row in self.get_block_events_rows(from_block, to_block)? {
            let block: BlockEvents = row.try_into()?;
            for transaction in block.transactions {
                let transaction_hash = tx_hashes
                    .next()
                    .map(|r| r.1)
                    .ok_or_else(|| PFError::tx_hash_mismatch(block.block_number))?;
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
        let contexts = self.get_block_hash_rows_with_count(from_block, to_block)?;
        let total_events: u64 = contexts.iter().map(|r| r.event_count).sum();
        let mut tx_hashes = self
            .get_block_tx_hash_rows(from_block, to_block)?
            .into_iter();
        let mut hash_rows = contexts.into_iter();
        let mut emitted_events = Vec::with_capacity(total_events as usize);

        for row in self.get_block_events_rows(from_block, to_block)? {
            let block: BlockEvents = row.try_into()?;
            let block_hash = loop {
                match hash_rows.next() {
                    Some(hash_row) => {
                        if hash_row.number == block.block_number {
                            break hash_row.hash;
                        } else if hash_row.number > block.block_number {
                            return Err(PFError::block_hash_missing(block.block_number));
                        }
                    }
                    None => return Err(PFError::block_hash_missing(block.block_number)),
                }
            };
            for transaction in block.transactions {
                let transaction_hash = tx_hashes
                    .next()
                    .map(|r| r.1)
                    .ok_or_else(|| PFError::tx_hash_mismatch(block.block_number))?;
                for event in transaction {
                    emitted_events.push(EmittedEvent {
                        block_hash: Some(block_hash),
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

    fn get_emitted_events_with_context(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> PFResult<(Vec<BlockContext>, Vec<EmittedEvent>)> {
        let context_rows = self.get_block_context_rows(from_block, to_block)?;
        let total_events: u64 = context_rows.iter().map(|r| r.event_count).sum();
        let mut tx_hashes = self
            .get_block_tx_hash_rows(from_block, to_block)?
            .into_iter();
        let mut emitted_events = Vec::with_capacity(total_events as usize);
        let mut contexts = Vec::with_capacity(context_rows.len());
        let mut ctx_iter = context_rows.into_iter();

        for row in self.get_block_events_rows(from_block, to_block)? {
            let block = BlockEvents::try_from(row)?;
            let ctx = loop {
                match ctx_iter.next() {
                    Some(ctx) => match ctx.number.cmp(&block.block_number) {
                        std::cmp::Ordering::Equal => break ctx,
                        std::cmp::Ordering::Less => contexts.push(ctx.into()),
                        std::cmp::Ordering::Greater => {
                            return Err(PFError::block_context_missing(block.block_number))
                        }
                    },
                    None => return Err(PFError::block_context_missing(block.block_number)),
                }
            };
            for transaction in block.transactions {
                let transaction_hash = tx_hashes
                    .next()
                    .map(|r| r.1)
                    .ok_or_else(|| PFError::tx_hash_mismatch(block.block_number))?;
                for event in transaction {
                    emitted_events.push(EmittedEvent {
                        block_hash: Some(ctx.hash),
                        block_number: Some(block.block_number),
                        data: event.data,
                        from_address: event.from_address,
                        keys: event.keys,
                        transaction_hash,
                    });
                }
            }
            contexts.push(ctx.into());
        }

        Ok((contexts, emitted_events))
    }
}
