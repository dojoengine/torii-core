use crate::types::PgTypeError;
use introspect_types::{ColumnInfo, FeltIds, MemberDef, PrimaryDef};
use itertools::Itertools;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use thiserror::Error;
use torii_introspect::schema::TableInfo;

#[derive(Debug, Error)]
pub enum PgTableError {
    #[error("Column with id: {0} not found in table {1}")]
    ColumnNotFound(Felt, String),
    #[error(transparent)]
    TypeError(#[from] PgTypeError),
    #[error("Current type mismatch error")]
    TypeMismatch,
}

pub type TableResult<T> = std::result::Result<T, PgTableError>;

#[derive(Debug)]
pub struct PgTable {
    pub name: String,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnInfo>,
    pub order: Vec<Felt>,
    pub alive: bool,
    pub dead_members: HashMap<String, MemberDef>,
}

impl From<TableInfo> for PgTable {
    fn from(value: TableInfo) -> Self {
        Self {
            name: value.name,
            primary: value.primary,
            order: value.columns.ids(),
            columns: value.columns.into_iter().map_into().collect(),
            alive: true,
            dead_members: HashMap::new(),
        }
    }
}
