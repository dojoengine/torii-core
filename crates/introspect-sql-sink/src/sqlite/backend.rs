use super::query::create_table_query;
use crate::sqlite::record::{coalesce_sql, SqliteDeserializer};
use crate::sqlite::types::SqliteColumn;
use crate::{
    DbColumn, DbDeadField, DbResult, DbTable, DeadField, IntrospectDb, IntrospectInitialize,
    IntrospectQueryMaker, IntrospectSqlSink, RecordResult, Table, TableResult, TypeResult,
};
use async_trait::async_trait;
use introspect_types::bytes::IntoByteSource;
use introspect_types::schema::{Names, TypeDefs};
use introspect_types::{ColumnDef, ColumnInfo, PrimaryDef, ResultInto};
use itertools::Itertools;
use sqlx::prelude::FromRow;
use sqlx::types::Json;
use sqlx::Arguments;
use sqlx::Error::Encode as EncodeError;
use starknet_types_core::felt::{Felt, FromStrError};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use torii_introspect::tables::RecordSchema;
use torii_introspect::Record;
use torii_sql::{DbPool, Queries, Sqlite, SqliteArguments, SqlitePool, SqliteQuery};

pub const INTROSPECT_SQLITE_SINK_MIGRATIONS: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/sqlite");

pub type IntrospectSqliteDb = IntrospectDb<SqliteBackend>;

pub struct SqliteBackend(SqlitePool);

impl Deref for SqliteBackend {
    type Target = SqlitePool;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<SqlitePool> for SqliteBackend {
    fn from(value: SqlitePool) -> Self {
        SqliteBackend(value)
    }
}

impl DbPool<Sqlite> for SqliteBackend {
    fn pool(&self) -> &SqlitePool {
        &self.0
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
impl IntrospectQueryMaker for SqliteBackend {
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
        namespace: &str,
        table_name: &str,
        schema: &RecordSchema<'_>,
        records: &[Record],
        _from_address: &Felt,
        _block_number: u64,
        _transaction_hash: &Felt,
        queries: &mut Vec<SqliteQuery>,
    ) -> RecordResult<()> {
        let qualified_table_name = qualified_table_name(namespace, table_name);
        let all_columns = schema.all_columns();
        let sql_columns = all_columns
            .iter()
            .map(|c| (*c).try_into())
            .collect::<TypeResult<Vec<SqliteColumn>>>()?;
        let column_names = all_columns.names();
        let placeholders = sql_columns.iter().map(SqliteColumn::placeholder).join(", ");
        let coalesce = sql_columns[1..]
            .iter()
            .map(|col| coalesce_sql(&qualified_table_name, col))
            .join(", ");
        let sql: Arc<str> = format!(
            r#"INSERT INTO "{qualified_table_name}" ({}) VALUES ({}) ON CONFLICT("{}") DO UPDATE SET {}"#,
            column_names
                .iter()
                .map(|name| format!(r#""{name}""#))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders,
            schema.primary_name(),
            coalesce
        ).into();

        for record in records {
            let mut arguments: SqliteArguments<'static> = SqliteArguments::default();
            let mut primary_data = record.id.as_slice().into_source();
            let mut data = record.values.as_slice().into_source();
            arguments
                .add(
                    schema
                        .primary_type_def()
                        .deserialize_column(&mut primary_data)?,
                )
                .map_err(EncodeError)?;
            for type_def in schema.columns().type_defs() {
                arguments
                    .add(type_def.deserialize_column(&mut data)?)
                    .map_err(EncodeError)?;
            }
            queries.add((sql.clone(), arguments));
        }
        Ok(())
    }
}

impl IntrospectSqlSink for SqliteBackend {
    const NAME: &'static str = "Introspect Sqlite";
}

#[async_trait]
impl IntrospectInitialize for SqliteBackend {
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

pub fn qualified_table_name(namespace: &str, table_name: &str) -> String {
    if namespace.is_empty() {
        table_name.to_string()
    } else {
        format!("{}_{}", namespace, table_name)
    }
}
