use starknet::core::types::{BlockId, Felt};

use crate::etl::extractor::FeltMap;

#[derive(Debug, Clone, Default)]
pub struct Cursor {
    block_number: u64,
    tx_hashes: Vec<Felt>,
}

pub enum Cursors {
    Global(Cursor),
    Contracts(FeltMap<Cursor>),
}

impl Cursor {
    pub fn new(block_number: u64, tx_hashes: Vec<Felt>) -> Self {
        Self {
            block_number,
            tx_hashes,
        }
    }

    pub fn processed(&self, block_number: u64, tx_hash: &Felt) -> bool {
        if block_number == self.block_number {
            return self.tx_hashes.contains(tx_hash);
        }
        if block_number > self.block_number {
            return false;
        }
        true
    }

    pub fn unprocessed(&self, block_number: u64, tx_hash: &Felt) -> bool {
        !self.processed(block_number, tx_hash)
    }

    pub fn block_number(&self) -> u64 {
        self.block_number
    }

    pub fn block_id(&self) -> BlockId {
        BlockId::Number(self.block_number)
    }

    pub fn tx_hashes(&self) -> &[Felt] {
        &self.tx_hashes
    }

    pub fn append_txs(&mut self, transaction_hashes: Vec<Felt>) {
        for tx_hash in transaction_hashes {
            self.append_tx(tx_hash);
        }
    }

    pub fn append_tx(&mut self, tx_hash: Felt) {
        if !self.tx_hashes.contains(&tx_hash) {
            self.tx_hashes.push(tx_hash);
        }
    }

    pub fn set(&mut self, block_number: u64, tx_hashes: Vec<Felt>) {
        self.block_number = block_number;
        self.tx_hashes = tx_hashes;
    }

    pub fn claim(&mut self, block_number: u64, tx_hash: Felt) -> bool {
        if block_number < self.block_number {
            false
        } else if block_number == self.block_number {
            if self.tx_hashes.contains(&tx_hash) {
                false
            } else {
                self.tx_hashes.push(tx_hash);
                true
            }
        } else {
            self.tx_hashes.clear();
            self.tx_hashes.push(tx_hash);
            self.block_number = block_number;
            true
        }
    }

    pub fn update<T: BlockTxStates>(&mut self, state: T) {
        let block_number = state.block_number();
        if block_number > self.block_number {
            self.tx_hashes = state.tx_hashes();
            self.block_number = block_number;
        } else if block_number == self.block_number {
            self.append_txs(state.tx_hashes());
        }
    }
}

pub trait TransactionHash {
    fn transaction_hash(&self) -> Felt;
}

pub trait TransactionIndex {
    fn block_number(&self) -> u64;
    fn transaction_hash(&self) -> Felt;
}

pub trait BlockTxStates {
    fn block_number(&self) -> u64;
    fn tx_hashes(self) -> Vec<Felt>;
}
