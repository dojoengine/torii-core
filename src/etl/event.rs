use std::collections::HashMap;

use starknet::core::types::{EmittedEvent, Felt};

pub trait EmittedEventExt {
    fn metadata(&self) -> HashMap<String, String>;
    fn split_keys(&self) -> Option<(&Felt, &[Felt])>;
}

impl EmittedEventExt for EmittedEvent {
    fn metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "from_address".to_string(),
            self.from_address.to_fixed_hex_string(),
        );
        metadata.insert(
            "tx_hash".to_string(),
            self.transaction_hash.to_fixed_hex_string(),
        );
        if let Some(block_hash) = self.block_hash {
            metadata.insert("block_hash".to_string(), block_hash.to_fixed_hex_string());
        }
        if let Some(block_number) = self.block_number {
            metadata.insert("block_number".to_string(), block_number.to_string());
        }
        metadata
    }
    fn split_keys(&self) -> Option<(&Felt, &[Felt])> {
        self.keys.split_first()
    }
}
