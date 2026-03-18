use std::sync::PoisonError;

use dojo_introspect::DojoIntrospectError;
use introspect_types::transcode::TranscodeError;
use introspect_types::DecodeError;
use starknet::core::utils::NonAsciiNameError;
use starknet_types_core::felt::Felt;

#[derive(Debug, thiserror::Error)]
pub enum DojoToriiError {
    #[error("Unknown Dojo Event selector {0:#066x}")]
    UnknownDojoEventSelector(Felt),
    #[error("Missing event selector")]
    MissingEventSelector,
    #[error("Column {0:#066x} not found in table {1}")]
    ColumnNotFound(Felt, String),
    #[error("Failed to parse field {0:#066x} in table {1}")]
    FieldParseError(Felt, String),
    #[error("Too many values provided for field {0:#066x}")]
    TooManyFieldValues(Felt),
    #[error("Failed to parse values for table {0}")]
    ParseValuesError(String),
    #[error("Cannot add {2} table already exists with id {0:#066x} and name {1}")]
    TableAlreadyExists(Felt, String, String),
    #[error("Table not found with id {0:#066x}")]
    TableNotFoundById(Felt),
    #[error("Failed to acquire lock: {0}")]
    LockError(String),
    #[error("Store error: {0}")]
    StoreError(String),
    #[error("Starknet selector error: {0}")]
    StarknetSelectorError(#[from] NonAsciiNameError),
    #[error("Lock poisoned: {0}")]
    LockPoisoned(String),
    #[error(transparent)]
    DecodeError(#[from] DecodeError),
    #[error("Failed to deserialize event data for {0}: {1:?}")]
    EventDeserializationError(&'static str, DecodeError),
    #[error(transparent)]
    DojoIntrospectError(#[from] DojoIntrospectError),
    #[error("Transcode error: {0:?}")]
    TranscodeError(TranscodeError<DecodeError, ()>),
}

pub type DojoToriiResult<T> = std::result::Result<T, DojoToriiError>;

impl From<TranscodeError<DecodeError, ()>> for DojoToriiError {
    fn from(err: TranscodeError<DecodeError, ()>) -> Self {
        Self::TranscodeError(err)
    }
}

impl DojoToriiError {
    pub fn store_error<T: ToString>(err: T) -> Self {
        Self::StoreError(err.to_string())
    }
}

impl<T> From<PoisonError<T>> for DojoToriiError {
    fn from(err: PoisonError<T>) -> Self {
        Self::LockPoisoned(err.to_string())
    }
}
