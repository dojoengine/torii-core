use crate::processor::PgSchema;
use crate::types::PgTypeError;
use crate::{PgStructDef, PgTableStructure, PostgresField};
use introspect_types::{ColumnDef, ColumnDefs, FeltIds, PrimaryDef};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use thiserror::Error;
use torii_introspect::tables::RecordSchema;
use torii_introspect::CreateTable;

#[derive(Debug, Error)]
pub enum PgTableError {
    #[error("Column with id: {0} not found in table {1}")]
    ColumnNotFound(Felt, String),
    #[error(transparent)]
    TypeError(#[from] PgTypeError),
}

pub type TableResult<T> = std::result::Result<T, PgTableError>;

#[derive(Debug)]
pub struct PgTable {
    pub name: String,
    pub postgres: PgTableStructure,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnDef>,
    pub order: Vec<Felt>,
    pub alive: bool,
}

impl PgStructDef {
    pub fn new(fields: Vec<PostgresField>) -> Self {
        Self {
            order: fields.iter().map(|f| f.name.clone()).collect(),
            fields: fields.into_iter().map(|f| (f.name, f.pg_type)).collect(),
        }
    }
}

impl PgTable {
    pub fn new(
        schema: &PgSchema,
        name: String,
        primary: PrimaryDef,
        columns: Vec<ColumnDef>,
        queries: &mut Vec<String>,
    ) -> TableResult<Self> {
        let postgres = PgTableStructure::new(schema, &name, &primary, &columns, queries)?;
        Ok(Self {
            name,
            postgres,
            primary,
            order: columns.ids(),
            columns: columns.as_hash_map(),
            alive: true,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &PgSchema {
        self.postgres.schema()
    }

    pub fn new_from_event(
        schema: &PgSchema,
        event: CreateTable,
        queries: &mut Vec<String>,
    ) -> TableResult<(Felt, Self)> {
        Self::new(schema, event.name, event.primary, event.columns, queries)
            .map(|table| (event.id, table))
    }

    pub fn get_columns(&self, selectors: &[Felt]) -> TableResult<Vec<&ColumnDef>> {
        selectors
            .iter()
            .map(|selector| self.get_column(selector))
            .collect()
    }

    pub fn get_column(&self, selector: &Felt) -> TableResult<&ColumnDef> {
        self.columns
            .get(selector)
            .ok_or_else(|| PgTableError::ColumnNotFound(*selector, self.name.clone()))
    }

    pub fn get_schema(&self, column_ids: &[Felt]) -> TableResult<RecordSchema<'_>> {
        let columns = self.get_columns(column_ids)?;
        Ok(RecordSchema::new(&self.primary, columns))
    }
}
