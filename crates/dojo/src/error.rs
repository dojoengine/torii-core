use dojo_introspect_types::DojoIntrospectError;
use introspect_types::DecodeError;
use starknet::core::utils::NonAsciiNameError;
use starknet_types_core::felt::Felt;

#[derive(Debug, thiserror::Error)]
pub enum DojoToriiError {
    #[error("Unknown Dojo Event selector {0}")]
    UnknownDojoEventSelector(Felt),
    #[error("Column {0} not found in table {1}")]
    ColumnNotFound(Felt, String),
    #[error("Failed to parse field {0} in table {1}")]
    FieldParseError(Felt, String),
    #[error("Too many values provided for field {0}")]
    TooManyFieldValues(Felt),
    #[error("Failed to parse values for table {0}")]
    ParseValuesError(String),
    #[error("Table already exists with id {0}")]
    TableAlreadyExists(Felt),
    #[error("Table not found with id {0}")]
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
    #[error(transparent)]
    DojoIntrospectError(#[from] DojoIntrospectError),
}

pub type DojoToriiResult<T> = std::result::Result<T, DojoToriiError>;
