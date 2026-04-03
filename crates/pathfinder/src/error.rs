use std::sync::PoisonError;

use bincode::error::DecodeError;

#[derive(Debug, thiserror::Error)]
pub enum PFError {
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("data mismatch: {0}")]
    DataMismatch(String),
    #[error("Block Hash missing for block {0}")]
    BlockHashMissing(u64),
    #[error("Block context missing for block {0}")]
    BlockContextMissing(u64),
    #[error("Lock poisoned: {0}")]
    LockPoison(String),
}

impl<T> From<PoisonError<T>> for PFError {
    fn from(err: PoisonError<T>) -> Self {
        Self::LockPoison(err.to_string())
    }
}

pub type PFResult<T> = std::result::Result<T, PFError>;

impl PFError {
    pub fn tx_hash_mismatch(block_number: u64) -> Self {
        Self::DataMismatch(format!(
            "Transactions hash missing for block {block_number}"
        ))
    }
    pub fn block_hash_missing(block_number: u64) -> Self {
        Self::BlockHashMissing(block_number)
    }
    pub fn block_context_missing(block_number: u64) -> Self {
        Self::BlockContextMissing(block_number)
    }
}
