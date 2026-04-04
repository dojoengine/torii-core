use crate::backend::{IntrospectInitialize, IntrospectPool, IntrospectProcessor};
use crate::error::TableLoadError;
use crate::table::{DeadField, Table};
use crate::tables::Tables;
use crate::{DbResult, IntrospectQueryMaker, NamespaceMode};
use async_trait::async_trait;
use introspect_types::{ColumnInfo, PrimaryDef, TypeDef};
use itertools::Itertools;
use sqlx::{Database, Pool};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use torii::etl::envelope::TypedTransactionMsgs;
use torii_introspect::events::{IntrospectBody, IntrospectMsg};
use torii_sql::{Executable, FlexQuery, PoolExt};

pub const COMMIT_CMD: &str = "--COMMIT";

pub struct IntrospectDb<Backend> {
    tables: Tables,
    namespaces: NamespaceMode,
    db: Backend,
}

pub struct DbTable {
    pub namespace: String,
    pub id: Felt,
    pub owner: Felt,
    pub name: String,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnInfo>,
    pub dead: HashMap<u128, DeadField>,
    pub alive: bool,
}

pub struct DbColumn {
    pub namespace: String,
    pub table: Felt,
    pub id: Felt,
    pub name: String,
    pub type_def: TypeDef,
}

pub struct DbDeadField {
    pub namespace: String,
    pub table: Felt,
    pub id: u128,
    pub name: String,
    pub type_def: TypeDef,
}

pub type IntrospectTxEvents = TypedTransactionMsgs<&IntrospectMsg>;

impl<DB: Database> PoolExt<DB> for IntrospectDb<Pool<DB>> {
    fn pool(&self) -> &Pool<DB> {
        &self.db
    }
}

pub trait IntoHashMap<K, V> {
    fn into_hash_map(self) -> HashMap<K, V>;
}

impl<K, V, T> IntoHashMap<K, V> for Vec<T>
where
    T: Into<(K, V)>,
    K: std::hash::Hash + Eq,
{
    fn into_hash_map(self) -> HashMap<K, V> {
        self.into_iter().map_into().collect()
    }
}

#[async_trait]
impl<DB: Database + Send + Sync + IntrospectQueryMaker> IntrospectProcessor for Pool<DB>
where
    Vec<FlexQuery<DB>>: Executable<DB>,
{
    async fn process_batch(
        &self,
        tables: &Tables,
        namespaces: &NamespaceMode,
        msgs: &[IntrospectTxEvents],
    ) -> DbResult<Vec<DbResult<()>>> {
        self.execute_msgs(tables, namespaces, msgs).await
    }
}

impl<Backend> IntrospectDb<Backend> {
    pub fn new(pool: Backend, namespaces: impl Into<NamespaceMode>) -> Self {
        Self {
            tables: Tables::default(),
            namespaces: namespaces.into(),
            db: pool,
        }
    }
}
impl<Backend: IntrospectProcessor + IntrospectInitialize> IntrospectDb<Backend> {
    pub async fn initialize_introspect_sql_sink(&self) -> DbResult<Vec<TableLoadError>> {
        self.db.initialize().await?;
        self.load_store_data().await
    }

    pub async fn process_batch(&self, batch: &[IntrospectTxEvents]) -> DbResult<Vec<DbResult<()>>> {
        self.db
            .process_batch(&self.tables, &self.namespaces, batch)
            .await
    }

    pub async fn load_store_data(&self) -> DbResult<Vec<TableLoadError>> {
        let mut errors = Vec::new();
        let namespaces = self.namespaces.namespaces();
        let mut tables: HashMap<(String, Felt), Table> =
            self.db.load_tables(&namespaces).await?.into_hash_map();
        for column in self.db.load_columns(&namespaces).await? {
            let (namespace, table_id, id, column_info) = column.into();
            if let Some(table) = tables.get_mut(&(namespace.clone(), table_id)) {
                table.columns.insert(id, column_info);
            } else {
                errors.push(TableLoadError::ColumnTableNotFound(
                    namespace,
                    table_id,
                    column_info.name,
                    id,
                ));
            }
        }
        for dead_field in self.db.load_dead_fields(&namespaces).await? {
            let (namespace, table_id, id, field) = dead_field.into();
            if let Some(table) = tables.get_mut(&(namespace.clone(), table_id)) {
                table.dead.insert(id, field);
            } else {
                errors.push(TableLoadError::TableDeadNotFound(
                    namespace, table_id, field.name, id,
                ));
            }
        }
        let mut map = self.tables.write()?;
        for ((namespace, id), table) in tables {
            match self.namespaces.get_key(namespace, id, &table.owner) {
                Ok(key) => {
                    map.insert(key, table);
                }
                Err(err) => errors.push(TableLoadError::NamespaceError(err)),
            }
        }
        Ok(errors)
    }
}

impl From<DbTable> for ((String, Felt), Table) {
    fn from(value: DbTable) -> Self {
        (
            (value.namespace.clone(), value.id),
            Table {
                id: value.id,
                namespace: value.namespace,
                name: value.name,
                owner: value.owner,
                primary: value.primary.into(),
                columns: value.columns,
                dead: value.dead,
                alive: value.alive,
            },
        )
    }
}

impl From<DbColumn> for (String, Felt, Felt, ColumnInfo) {
    fn from(value: DbColumn) -> Self {
        (
            value.namespace,
            value.table,
            value.id,
            ColumnInfo {
                name: value.name,
                attributes: Vec::new(),
                type_def: value.type_def,
            },
        )
    }
}

impl From<DbDeadField> for (String, Felt, u128, DeadField) {
    fn from(value: DbDeadField) -> Self {
        (
            value.namespace,
            value.table,
            value.id,
            DeadField {
                name: value.name,
                type_def: value.type_def,
            },
        )
    }
}
