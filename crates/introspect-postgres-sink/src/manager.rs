use crate::PgTableSchema;
use introspect_types::{Attribute, ColumnDef, PrimaryDef, TypeDef};
use sqlx::{Postgres, Transaction, Type};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{PoisonError, RwLock};
use thiserror::Error;
use torii_introspect::events::IdName;

#[derive(Debug, Error)]
pub enum TableManagerError {
    #[error("Lock error")]
    LockError,
    #[error("Table schema error: {0}")]
    TableSchemaError(#[from] crate::table::TableError),
}

type ManagerResult<T> = std::result::Result<T, TableManagerError>;

impl<T> From<PoisonError<T>> for TableManagerError {
    fn from(_: PoisonError<T>) -> Self {
        TableManagerError::LockError
    }
}

#[derive(Default)]
pub struct TableManager(pub RwLock<HashMap<String, RwLock<PgTableSchema>>>);

impl Deref for TableManager {
    type Target = RwLock<HashMap<String, RwLock<PgTableSchema>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub trait PgTableManager {
    fn declare_table(
        &self,
        id: Felt,
        name: &str,
        attributes: &[Attribute],
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        tx: &mut Transaction<'_, Postgres>,
    ) -> ManagerResult<()>;
    fn rename_table(
        &self,
        id: Felt,
        new_name: &str,
        tx: &mut Transaction<'_, Postgres>,
    ) -> ManagerResult<()>;
    fn add_columns(
        &self,
        table: Felt,
        columns: &[ColumnDef],
        tx: &mut Transaction<'_, Postgres>,
    ) -> ManagerResult<()>;
    fn rename_primary(
        &self,
        id: Felt,
        name: &str,
        tx: &mut Transaction<'_, Postgres>,
    ) -> ManagerResult<()>;
    fn rename_columns(
        &self,
        table: Felt,
        columns: &[IdName],
        tx: &mut Transaction<'_, Postgres>,
    ) -> ManagerResult<()>;
}

pub trait PgMutTableManager {
    fn update_table(
        &self,
        id: Felt,
        name: &str,
        attributes: &[Attribute],
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        tx: &mut Transaction<'_, Postgres>,
    ) -> ManagerResult<()>;
}

impl TableManager {
    pub fn declare_table(
        &self,
        id: Felt,
        table_name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
    ) -> ManagerResult<Vec<String>> {
        let table_read = self.0.read()?;

        match table_read.get(table_name) {
            Some(schema) => {
                println!("Upgrading existing table schema: {}", table_name);
                let mut schema = schema.write()?;
                Ok(schema.upgrade_schema(table_name, columns)?)
            }
            None => {
                println!("Declaring new table schema: {}", table_name);
                let (table, queries) = PgTableSchema::new(table_name, primary, columns)?;
                drop(table_read);

                self.0
                    .write()?
                    .insert(table_name.to_string(), RwLock::new(table));
                Ok(queries)
            }
        }
    }

    pub fn update_table(
        &self,
        table_name: &str,
        columns: &[ColumnDef],
    ) -> ManagerResult<Vec<String>> {
        let table_read = self.0.read()?;

        match table_read.get(table_name) {
            Some(schema) => {
                println!("Updating existing table schema: {}", table_name);
                let mut schema = schema.write()?;
                Ok(schema.upgrade_schema(table_name, columns)?)
            }
            None => Err(TableManagerError::TableSchemaError(
                crate::table::TableError::TableNotFound(table_name.to_string()),
            )),
        }
    }
}
