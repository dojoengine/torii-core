use crate::{
    DbColumn, DbDeadField, DbResult, DbTable, DeadField, IntrospectDb, IntrospectInitialize,
    IntrospectQueryMaker, IntrospectSqlSink, RecordResult, Table, TableResult,
};
use async_trait::async_trait;
use introspect_types::{ColumnDef, ColumnInfo, PrimaryDef, ResultInto};
use itertools::Itertools;
use sqlx::prelude::FromRow;
use sqlx::types::Json;
use starknet_types_core::felt::{Felt, FromStrError};
use std::collections::HashMap;
use torii_introspect::tables::RecordSchema;
use torii_introspect::Record;
use torii_sql::{DbConnection, Queries, Sqlite, SqlitePool, SqliteQuery};

use super::query::create_table_query;

pub const INTROSPECT_SQLITE_SINK_MIGRATIONS: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/sqlite");

pub type IntrospectSqliteDb<T> = IntrospectDb<SqliteBackend<T>>;

pub struct SqliteBackend<T: DbConnection<Sqlite>>(T);

impl<T: DbConnection<Sqlite>> From<T> for SqliteBackend<T> {
    fn from(value: T) -> Self {
        SqliteBackend(value)
    }
}

impl<T: DbConnection<Sqlite>> DbConnection<Sqlite> for SqliteBackend<T> {
    fn pool(&self) -> &SqlitePool {
        self.0.pool()
    }
}

#[derive(FromRow)]
pub struct SqliteTableRow {
    namespace: String,
    id: String,
    owner: String,
    name: String,
    primary: Json<PrimaryDef>,
    columns: Json<HashMap<Felt, ColumnInfo>>,
    dead: Json<HashMap<u128, DeadField>>,
    alive: bool,
}

impl TryFrom<SqliteTableRow> for DbTable {
    type Error = FromStrError;
    fn try_from(value: SqliteTableRow) -> Result<Self, FromStrError> {
        Ok(DbTable {
            namespace: value.namespace,
            id: Felt::from_hex(&value.id)?,
            owner: Felt::from_hex(&value.owner)?,
            name: value.name,
            primary: value.primary.0,
            columns: value.columns.0.into_iter().map_into().collect(),
            dead: value.dead.0.into_iter().map_into().collect(),
            alive: value.alive,
        })
    }
}

#[async_trait]
impl<T: DbConnection<Sqlite> + Send + Sync> IntrospectQueryMaker for SqliteBackend<T> {
    type DB = Sqlite;
    fn create_table_queries(
        namespace: &str,
        _id: &Felt,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        _from_address: &Felt,
        _block_number: u64,
        _transaction_hash: &Felt,
        queries: &mut Vec<SqliteQuery>,
    ) -> TableResult<()> {
        queries.add(create_table_query(namespace, name, primary, columns));
        Ok(())
    }
    fn update_table_queries(
        _table: &mut Table,
        _name: &str,
        _primary: &PrimaryDef,
        _columns: &[ColumnDef],
        _from_address: &Felt,
        _block_number: u64,
        _transaction_hash: &Felt,
        _queries: &mut Vec<SqliteQuery>,
    ) -> TableResult<()> {
        Ok(())
    }
    fn insert_record_queries(
        _namespace: &str,
        _table_name: &str,
        _schema: &RecordSchema<'_>,
        _records: &[Record],
        _from_address: &Felt,
        _block_number: u64,
        _transaction_hash: &Felt,
        _queries: &mut Vec<SqliteQuery>,
    ) -> RecordResult<()> {
        Ok(())
    }
}

impl<T: DbConnection<Sqlite>> IntrospectSqlSink for SqliteBackend<T> {
    const NAME: &'static str = "Introspect Sqlite";
}

#[async_trait]
impl<T: DbConnection<Sqlite> + Send + Sync> IntrospectInitialize for SqliteBackend<T> {
    async fn load_tables(&self, _schemas: &Option<Vec<String>>) -> DbResult<Vec<DbTable>> {
        let rows: Vec<SqliteTableRow> = sqlx::query_as(
            r"
            SELECT namespace, id, owner, name, primary, columns, dead, alive
            FROM introspect_sink_schema_state
            ORDER BY updated_at ASC
            ",
        )
        .fetch_all(self.pool())
        .await?;

        let tables: Vec<DbTable> = rows
            .into_iter()
            .map(|row| row.try_into())
            .collect::<Result<_, _>>()?;
        Ok(tables)
    }
    async fn load_columns(&self, _schemas: &Option<Vec<String>>) -> DbResult<Vec<DbColumn>> {
        Ok(Vec::new())
    }
    async fn load_dead_fields(&self, _schemas: &Option<Vec<String>>) -> DbResult<Vec<DbDeadField>> {
        Ok(Vec::new())
    }
    async fn initialize(&self) -> DbResult<()> {
        self.migrate(Some("introspect"), INTROSPECT_SQLITE_SINK_MIGRATIONS)
            .await
            .err_into()
    }
}
