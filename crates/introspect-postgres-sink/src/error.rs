use std::sync::PoisonError;

use introspect_types::{PrimaryTypeDef, TypeDef};
use sqlx::Error as SqlxError;
use starknet_types_core::felt::Felt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgTypeError {
    #[error("Unsupported type for {0}")]
    UnsupportedType(String),
    #[error("Nested arrays are not supported")]
    NestedArrays,
}

pub type PgTypeResult<T> = std::result::Result<T, PgTypeError>;

#[derive(Debug, Error)]
pub enum PgTableError {
    #[error("Column with id: {0} not found in table {1}")]
    ColumnNotFound(Felt, String),
    #[error(transparent)]
    TypeError(#[from] PgTypeError),
    #[error("Current type mismatch error")]
    TypeMismatch,
    #[error("Unsupported upgrade for table {table} column {column}: {reason}")]
    UnsupportedUpgrade {
        table: String,
        column: String,
        reason: UpgradeError,
    },
}

pub type TableResult<T> = std::result::Result<T, PgTableError>;

#[derive(Debug, thiserror::Error)]
pub enum UpgradeError {
    #[error("Failed to upgrade type from {old} to {new}")]
    TypeUpgradeError {
        old: &'static str,
        new: &'static str,
    },
    #[error("Failed to upgrade primary from {old} to {new}")]
    PrimaryUpgradeError {
        old: &'static str,
        new: &'static str,
    },
    #[error(transparent)]
    TypeCreationError(#[from] PgTypeError),
    #[error("Array length cannot be decreased from {old} to {new}")]
    ArrayLengthDecreaseError { old: u32, new: u32 },
    #[error("Cannot reduce element in tuple")]
    TupleReductionError,
}

pub type UpgradeResult<T> = Result<T, UpgradeError>;

impl UpgradeError {
    pub fn type_upgrade_err<T>(old: &TypeDef, new: &TypeDef) -> UpgradeResult<T> {
        Err(Self::TypeUpgradeError {
            old: old.item_name(),
            new: new.item_name(),
        })
    }
    pub fn type_upgrade_to_err<T>(old: &TypeDef, new: &'static str) -> UpgradeResult<T> {
        Err(Self::TypeUpgradeError {
            old: old.item_name(),
            new,
        })
    }
    pub fn type_cast_err<T>(old: &TypeDef, new: &'static str) -> UpgradeResult<T> {
        Err(Self::TypeUpgradeError {
            old: old.item_name(),
            new,
        })
    }
    pub fn array_shorten_err<T>(old: u32, new: u32) -> UpgradeResult<T> {
        Err(Self::ArrayLengthDecreaseError { old, new })
    }
    pub fn primary_upgrade_err<T>(old: &PrimaryTypeDef, new: &PrimaryTypeDef) -> UpgradeResult<T> {
        Err(Self::PrimaryUpgradeError {
            old: old.item_name(),
            new: new.item_name(),
        })
    }
}

pub trait UpgradeResultExt<T> {
    fn to_table_result(self, table: &str, column: &str) -> TableResult<T>;
}

impl<T> UpgradeResultExt<T> for UpgradeResult<T> {
    fn to_table_result(self, table: &str, column: &str) -> TableResult<T> {
        self.map_err(|err| PgTableError::UnsupportedUpgrade {
            table: table.to_string(),
            column: column.to_string(),
            reason: err,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PgDbError {
    #[error(transparent)]
    DatabaseError(#[from] SqlxError),
    #[error("Invalid event format: {0}")]
    InvalidEventFormat(String),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    TableError(#[from] PgTableError),
    #[error(transparent)]
    TypeError(#[from] PgTypeError),
    #[error("Table with id: {0} already exists, incoming name: {1}, existing name: {2}")]
    TableAlreadyExists(Felt, String, String),
    #[error("Table not found with id: {0}")]
    TableNotFound(Felt),
    #[error("Table not alive - id: {0}, name: {1}")]
    TableNotAlive(Felt, String),
    #[error("Manager does not support updating")]
    UpdateNotSupported,
    #[error("Table poison error: {0}")]
    PoisonError(String),
}

pub type PgDbResult<T> = std::result::Result<T, PgDbError>;

impl<T> From<PoisonError<T>> for PgDbError {
    fn from(err: PoisonError<T>) -> Self {
        Self::PoisonError(err.to_string())
    }
}
