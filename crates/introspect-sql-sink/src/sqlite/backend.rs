use crate::sqlite::record::{coalesce_sql, SqliteDeserializer};
use crate::sqlite::table::{
    create_table_query, persist_table_state_query, qualified_table_name, update_column,
    update_column_query, ColumnInfoRef, FETCH_TABLES_QUERY,
};
use crate::sqlite::types::SqliteColumn;
use crate::{
    DbColumn, DbDeadField, DbResult, DbTable, IntrospectDb, IntrospectInitialize,
    IntrospectQueryMaker, IntrospectSqlSink, RecordResult, Table, TableResult, TypeResult,
    UpgradeResultExt,
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
            dead: HashMap::new(),
            alive: value.alive,
        })
    }
}

#[async_trait]
impl IntrospectQueryMaker for SqliteBackend {
    type DB = Sqlite;
    fn create_table_queries(
        namespace: &str,
        id: &Felt,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<SqliteQuery>,
    ) -> TableResult<()> {
        queries.add(create_table_query(namespace, name, primary, columns)?);
        persist_table_state_query(
            namespace,
            id,
            name,
            primary,
            columns.iter().map(ColumnInfoRef::from_def).collect(),
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
    }
    fn update_table_queries(
        table: &mut Table,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<SqliteQuery>,
    ) -> TableResult<()> {
        let table_name = qualified_table_name(&table.namespace, name);
        if table.name != name {
            queries.add(format!(
                r#"ALTER TABLE "{}" RENAME TO "{table_name}""#,
                qualified_table_name(&table.namespace, &table.name),
            ));
            table.name = name.to_string();
        }
        update_column(
            &table_name,
            &mut table.primary,
            &primary.name,
            &((&primary.type_def).into()),
            queries,
        )
        .to_table_result(&table_name, "primary");
        // let mut all_columns = table.columns.iter().map(ColumnInfoRef::from_info).collect();
        // for column in columns{
        //     match all_columns.get_mut(&column.id){
        //         Some(mut existing)
        //     }
        // }
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
        let table_name = qualified_table_name(namespace, table_name);
        let all_columns = schema.all_columns();
        let sql_columns = all_columns
            .iter()
            .map(|c| (*c).try_into())
            .collect::<TypeResult<Vec<SqliteColumn>>>()?;
        let column_names = all_columns.names();
        let placeholders = sql_columns.iter().map(SqliteColumn::placeholder).join(", ");
        let coalesce = sql_columns[1..]
            .iter()
            .map(|col| coalesce_sql(&table_name, col))
            .join(", ");
        let sql: Arc<str> = format!(
            r#"INSERT INTO "{table_name}" ({}) VALUES ({}) ON CONFLICT("{}") DO UPDATE SET {}"#,
            column_names
                .iter()
                .map(|name| format!(r#""{name}""#))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders,
            schema.primary_name(),
            coalesce
        )
        .into();

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
        let rows: Vec<SqliteTableRow> = sqlx::query_as(FETCH_TABLES_QUERY)
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
