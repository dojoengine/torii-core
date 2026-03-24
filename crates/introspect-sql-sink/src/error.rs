use crate::TableKey;
use introspect_types::{PrimaryTypeDef, TypeDef};
use sqlx::Error as SqlxError;
use starknet_types_core::felt::{Felt, FromStrError};
use std::sync::PoisonError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TypeError {
    #[error("Unsupported type for {0}")]
    UnsupportedType(String),
    #[error("Nested arrays are not supported")]
    NestedArrays,
}

pub type TypeResult<T> = std::result::Result<T, TypeError>;

#[derive(Debug, Error)]
pub enum TableError {
    #[error("Column with id: {0} not found in table {1}")]
    ColumnNotFound(Felt, String),
    #[error(transparent)]
    TypeError(#[from] TypeError),
    #[error("Current type mismatch error")]
    TypeMismatch,
    #[error("Unsupported upgrade for table {table} column {column}: {reason}")]
    UnsupportedUpgrade {
        table: String,
        column: String,
        reason: UpgradeError,
    },
}

pub type TableResult<T> = std::result::Result<T, TableError>;

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
    TypeCreationError(#[from] TypeError),
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
        self.map_err(|err| TableError::UnsupportedUpgrade {
            table: table.to_string(),
            column: column.to_string(),
            reason: err,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error(transparent)]
    DatabaseError(#[from] SqlxError),
    #[error("Invalid event format: {0}")]
    InvalidEventFormat(String),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    TableError(#[from] TableError),
    #[error(transparent)]
    TypeError(#[from] TypeError),
    #[error("Table with id: {0} already exists, incoming name: {1}, existing name: {2}")]
    TableAlreadyExists(TableKey, String, String),
    #[error("Table not found with id: {0}")]
    TableNotFound(TableKey),
    #[error("Table not alive - id: {0}, name: {1}")]
    TableNotAlive(Felt, String),
    #[error("Manager does not support updating")]
    UpdateNotSupported,
    #[error("Table poison error: {0}")]
    PoisonError(String),
    #[error("Schema not found for address: {0:#063x}")]
    SchemaNotFound(Felt),
}

pub type DbResult<T> = std::result::Result<T, DbError>;

impl<T> From<PoisonError<T>> for DbError {
    fn from(err: PoisonError<T>) -> Self {
        Self::PoisonError(err.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("Invalid address length for schema: {0} should be 63 characters long")]
    InvalidAddressLength(String),
    #[error(transparent)]
    AddressFromStrError(#[from] FromStrError),
    #[error("Schema {0} does not match expected schema {1}")]
    SchemaMismatch(String, String),
    #[error("Schema {1} not found for address: {0:#063x}")]
    AddressNotFound(Felt, String),
}

pub type SchemaResult<T> = std::result::Result<T, SchemaError>;

#[derive(Debug, thiserror::Error)]
pub enum TableLoadError {
    #[error(transparent)]
    SchemaError(#[from] SchemaError),
    #[error("Table {0} {1:#063x} not found for column {2} with id: {3:#063x}")]
    ColumnTableNotFound(String, Felt, String, Felt),
    #[error("Table {0} {1:#063x} not found for dead field {2} with id: {3}")]
    TableDeadNotFound(String, Felt, String, u128),
}
