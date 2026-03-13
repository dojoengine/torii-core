use introspect_types::{ColumnDef, FeltIds, PrimaryDef};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use thiserror::Error;
use torii_introspect::schema::TableSchema;
use torii_introspect::tables::RecordSchema;

#[derive(Debug, Error)]
pub enum SqliteTableError {
    #[error("Column with id: {0} not found in table {1}")]
    ColumnNotFound(Felt, String),
}

pub type TableResult<T> = std::result::Result<T, SqliteTableError>;

#[derive(Debug, Clone)]
pub struct SqliteTable {
    pub name: String,
    pub storage_name: String,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnDef>,
    pub order: Vec<Felt>,
    pub alive: bool,
}

impl SqliteTable {
    pub fn new(
        storage_name: String,
        name: String,
        primary: PrimaryDef,
        columns: Vec<ColumnDef>,
    ) -> Self {
        Self {
            name,
            storage_name,
            primary,
            order: columns.ids(),
            columns: columns
                .into_iter()
                .map(|column| (column.id, column))
                .collect(),
            alive: true,
        }
    }

    pub fn new_from_table(namespace: &str, table: impl Into<TableSchema>) -> (Felt, Self) {
        let table = table.into();
        let storage_name = if namespace.is_empty() {
            table.name.clone()
        } else {
            format!("{namespace}__{}", table.name)
        };
        (
            table.id,
            Self::new(storage_name, table.name, table.primary, table.columns),
        )
    }

    pub fn get_column(&self, selector: &Felt) -> TableResult<&ColumnDef> {
        self.columns
            .get(selector)
            .ok_or_else(|| SqliteTableError::ColumnNotFound(*selector, self.name.clone()))
    }

    pub fn get_schema(&self, column_ids: &[Felt]) -> TableResult<RecordSchema<'_>> {
        let columns = column_ids
            .iter()
            .map(|selector| self.get_column(selector))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordSchema::new(&self.primary, columns))
    }
}
