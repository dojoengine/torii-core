use crate::TableSchema;
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{PoisonError, RwLock},
};
use thiserror::Error;
use torii_types_introspect::DeclareTableV1;

#[derive(Debug, Error)]
pub enum TableManagerError {
    #[error("Lock error")]
    LockError,
    #[error("Table schema error: {0}")]
    TableSchemaError(#[from] crate::table::TableError),
}

type Result<T> = std::result::Result<T, TableManagerError>;

impl<T> From<PoisonError<T>> for TableManagerError {
    fn from(_: PoisonError<T>) -> Self {
        TableManagerError::LockError
    }
}

#[derive(Default)]
pub struct TableManager(pub RwLock<HashMap<String, RwLock<TableSchema>>>);

impl Deref for TableManager {
    type Target = RwLock<HashMap<String, RwLock<TableSchema>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TableManager {
    pub fn declare_table(&self, table_name: &str, event: &DeclareTableV1) -> Result<Vec<String>> {
        let table_read = self.0.read()?;

        match table_read.get(table_name) {
            Some(schema) => {
                println!("Upgrading existing table schema: {}", table_name);
                let mut schema = schema.write()?;
                Ok(schema.upgrade_schema(table_name, &event.columns)?)
            }
            None => {
                println!("Declaring new table schema: {}", table_name);
                let (table, queries) =
                    TableSchema::new(table_name, &event.primary, &event.columns)?;
                drop(table_read);

                self.0
                    .write()?
                    .insert(table_name.to_string(), RwLock::new(table));
                Ok(queries)
            }
        }
    }
}
