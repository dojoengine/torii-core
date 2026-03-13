use crate::processor::PgSchema;
use crate::types::PgTypeError;
use crate::{PgStructDef, PgTableStructure, PostgresField, PostgresType};
use introspect_types::{ColumnDef, ColumnDefs, FeltIds, MemberDef, PrimaryDef, VariantDef};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use thiserror::Error;
use torii_introspect::schema::TableSchema;
use torii_introspect::tables::RecordSchema;

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
    pub schema: PgSchema,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnDef>,
    pub order: Vec<Felt>,
    pub alive: bool,
    pub dead_members: HashMap<String, MemberDef>,
    pub dead_variants: HashMap<String, Vec<VariantDef>>,
}

impl PgStructDef {
    pub fn new(fields: Vec<PostgresField>) -> Self {
        Self {
            order: fields.iter().map(|f| f.name.clone()).collect(),
            fields: fields.into_iter().map(|f| (f.name, f.pg_type)).collect(),
        }
    }
    pub fn add_member(&mut self, name: &str, pg_type: PostgresType) {
        self.order.push(field.name.clone());
        self.fields.insert(field.name.clone(), field.pg_type);
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
            schema: schema.clone(),
            primary,
            order: columns.ids(),
            columns: columns.as_hash_map(),
            alive: true,
        })
    }

    pub fn upgrade(
        &mut self,
        new_primary: PrimaryDef,
        new_columns: Vec<ColumnDef>,
        queries: &mut Vec<String>,
    ) -> TableResult<()> {
        self.postgres.upgrade(
            &self.primary,
            &new_primary,
            &self.columns,
            &new_columns,
            queries,
        )?;
        self.primary = new_primary;
        self.order = new_columns.ids();
        self.columns = new_columns.as_hash_map();
        Ok(())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &PgSchema {
        self.postgres.schema()
    }

    pub fn new_from_table(
        schema: &PgSchema,
        to_table: impl Into<TableSchema>,
        queries: &mut Vec<String>,
    ) -> TableResult<(Felt, Self)> {
        let table = to_table.into();
        Self::new(schema, table.name, table.primary, table.columns, queries)
            .map(|pg_table| (table.id, pg_table))
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
