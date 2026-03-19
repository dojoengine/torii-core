use std::collections::HashSet;

use starknet::core::types::Felt;

#[derive(Debug, Clone, Default)]
pub struct Cursor {
    block_number: u64,
    tx_hashes: Vec<Felt>,
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

    pub fn tx_hashes(&self) -> &[Felt] {
        &self.tx_hashes
    }

    pub fn set(&mut self, block_number: u64, tx_hashes: Vec<Felt>) {
        self.block_number = block_number;
        self.tx_hashes = tx_hashes;
    }

    pub fn claim(&mut self, block_number: u64, tx_hash: &Felt) -> bool {
        if block_number < self.block_number {
            false
        } else if block_number == self.block_number {
            if self.tx_hashes.contains(tx_hash) {
                false
            } else {
                self.tx_hashes.push(*tx_hash);
                true
            }
        } else {
            self.tx_hashes = vec![*tx_hash];
            self.block_number = block_number;
            true
        }
    }
}
