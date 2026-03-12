use super::DojoStoreTrait;
use crate::decoder::primary_field_def;
use crate::table::DojoTableInfo;
use crate::DojoTable;
use async_trait::async_trait;
use introspect_types::{ColumnInfo, ResultInto};
use itertools::Itertools;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{FromRow, PgPool, Postgres};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::ops::Deref;
use torii_introspect::postgres::owned::{column_info_insert_query, PgTypeDef, TABLE_INSERT_QUERY};
use torii_introspect::postgres::{PgFelt, SqlxResult};
use torii_introspect::schema::ColumnKeyTrait;
use torii_postgres::db::PostgresConnection;

const DOJO_COLUMN_TABLE: &str = "dojo.columns";
const DOJO_TABLE_TABLE: &str = "dojo.table";

pub const FETCH_TABLES_QUERY: &str = r#"
    SELECT id, name, attributes, keys, "values", legacy
    FROM dojo.tables
    WHERE $1::felt252[] = '{}' OR owner = ANY($1)"#;

pub const FETCH_COLUMNS_QUERY: &str = r#"
    SELECT "table", id, name, attributes, type_def
    FROM dojo.columns
    WHERE $1::felt252[] = '{}' OR owner = ANY($1)"#;

pub const INSERT_TABLE_QUERY: &str = r#"
    INSERT INTO dojo.tables (owner, id, name, attributes, keys, "values", legacy, updated_at, created_block, updated_block, created_tx, updated_tx)
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8, $8, $9, $9)
        ON CONFLICT (owner, id) DO UPDATE SET
        name = EXCLUDED.name,
        attributes = EXCLUDED.attributes,
        keys = EXCLUDED.keys,
        "values" = EXCLUDED."values",
        legacy = EXCLUDED.legacy,
        updated_at = NOW(),
        updated_block = EXCLUDED.updated_block,
        updated_tx = EXCLUDED.updated_tx"#;

pub const INSERT_COLUMN_QUERY: &str = r#"
    INSERT INTO dojo.columns (owner, "table", id, name, attributes, type_def)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (owner, "table", id) DO UPDATE SET
        name = EXCLUDED.name,
        attributes = EXCLUDED.attributes,
        type_def = EXCLUDED.type_def"#;

pub const INSERT_META_DATA_QUERY: &str = r#"
            INSERT INTO dojo.tables (owner, id, name, __created_block, __updated_block, __created_tx, __updated_tx)
            VALUES ($1, $2, $3, $4, $4, $5, $5) 
            ON CONFLICT (owner, id, name) DO UPDATE SET
            __updated_at = NOW(), 
            __updated_block = EXCLUDED.__updated_block, 
            __updated_tx = EXCLUDED.__updated_tx"#;

pub const DOJO_STORE_MIGRATIONS: Migrator = sqlx::migrate!();

#[derive(Debug, thiserror::Error)]
pub enum DojoPgStoreError {
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
    #[error("historical schema bootstrap is not supported from dojo.table")]
    UnsupportedHistoricalLoad,
    #[error("Column not found for table {name} with id {table_id} and column {column_id}")]
    ColumnNotFound {
        name: String,
        table_id: Felt,
        column_id: Felt,
    },
    #[error("Duplicate tables found for owner {owner:?} and table id {table_id}")]
    DuplicateTables { owner: Felt, table_id: Felt },
}

impl DojoPgStoreError {
    pub fn column_not_found<K: ColumnKeyTrait>(name: String, key: &K) -> Self {
        let (table_id, column_id) = key.as_parts();
        Self::ColumnNotFound {
            name,
            table_id: *table_id,
            column_id: *column_id,
        }
    }
}

#[derive(FromRow)]
pub struct TableRow {
    id: PgFelt,
    name: String,
    attributes: Vec<String>,
    keys: Vec<PgFelt>,
    #[sqlx(rename = "values")]
    values: Vec<PgFelt>,
    legacy: bool,
}

impl From<TableRow> for ((), DojoTable) {
    fn from(value: TableRow) -> Self {
        (
            (),
            DojoTable {
                id: value.id.into(),
                name: value.name,
                attributes: value.attributes,
                primary: primary_field_def(),
                columns: HashMap::new(),
                key_fields: value.keys.into_iter().map_into().collect(),
                value_fields: value.values.into_iter().map_into().collect(),
                legacy: value.legacy,
            },
        )
    }
}

impl From<TableRow> for (Felt, DojoTableInfo) {
    fn from(value: TableRow) -> Self {
        (
            value.id.into(),
            DojoTableInfo {
                name: value.name,
                attributes: value.attributes,
                primary: primary_field_def(),
                columns: HashMap::new(),
                key_fields: value.keys.into_iter().map_into().collect(),
                value_fields: value.values.into_iter().map_into().collect(),
                legacy: value.legacy,
            },
        )
    }
}

#[async_trait]
impl PgTypeDef<Felt> for DojoTableInfo {
    type Row = TableRow;
    async fn get_rows(
        pool: &PgPool,
        query: &'static str,
        owners: &[Felt],
    ) -> SqlxResult<Vec<(Felt, DojoTableInfo)>> {
        Self::get_pg_rows(pool, query, owners)
            .await
            .map(|rows| rows.into_iter().map_into().collect_vec())
    }
}

#[async_trait]
impl PgTypeDef<()> for DojoTable {
    type Row = TableRow;
    async fn get_rows(
        pool: &PgPool,
        query: &'static str,
        owners: &[Felt],
    ) -> SqlxResult<Vec<((), DojoTable)>> {
        Self::get_pg_rows(pool, query, owners)
            .await
            .map(|rows| rows.into_iter().map_into().collect_vec())
    }
}

pub fn table_insert_query(
    owner: &Felt,
    id: &Felt,
    name: &str,
    attributes: &[String],
    keys: &[Felt],
    values: &[Felt],
    legacy: bool,
    created_block: u64,
    created_tx: &Felt,
) -> Query<'static, Postgres, PgArguments> {
    sqlx::query::<Postgres>(TABLE_INSERT_QUERY)
        .bind(PgFelt::from(*owner))
        .bind(PgFelt::from(*id))
        .bind(name.to_owned())
        .bind(attributes.to_owned())
        .bind(keys.iter().copied().map(PgFelt::from).collect_vec())
        .bind(values.iter().copied().map(PgFelt::from).collect_vec())
        .bind(legacy)
        .bind(created_block.to_string())
        .bind(PgFelt::from(*created_tx))
}

impl DojoTable {
    pub fn insert_query(
        &self,
        owner: &Felt,
        tx_hash: &Felt,
        block_number: u64,
    ) -> Query<'static, Postgres, PgArguments> {
        table_insert_query(
            owner,
            &self.id,
            &self.name,
            &self.attributes,
            &self.key_fields.iter().copied().collect_vec(),
            &self.value_fields.iter().copied().collect_vec(),
            self.legacy,
            block_number,
            tx_hash,
        )
    }
}

pub struct PgStore<T>(pub T);

impl<T> Deref for PgStore<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: PostgresConnection + Send + Sync> PgStore<T> {
    pub async fn initialize(&self) -> SqlxResult<()> {
        println!("Running Dojo migrations...");
        println!("{:?}", DOJO_STORE_MIGRATIONS);
        self.migrate(Some("dojo"), DOJO_STORE_MIGRATIONS).await
    }
}

impl<T: PostgresConnection> From<T> for PgStore<T> {
    fn from(pool: T) -> Self {
        PgStore(pool)
    }
}

#[async_trait]
impl<T: PostgresConnection + Send + Sync + 'static> DojoStoreTrait for PgStore<T> {
    type Error = DojoPgStoreError;

    async fn save_table(
        &self,
        owner: &Felt,
        table: &DojoTable,
        tx_hash: &Felt,
        block_number: u64,
    ) -> Result<(), Self::Error> {
        let mut transaction = self.begin().await?;
        table
            .insert_query(owner, tx_hash, block_number)
            .execute(&mut *transaction)
            .await?;
        for (id, info) in &table.columns {
            column_info_insert_query(INSERT_COLUMN_QUERY, owner, &table.id, id, info)
                .execute(&mut *transaction)
                .await?;
        }

        transaction.commit().await.err_into()
    }

    async fn load_tables(&self, owners: &[Felt]) -> Result<Vec<DojoTable>, Self::Error> {
        let mut tables = DojoTable::get_rows(self.pool(), DOJO_TABLE_TABLE, owners)
            .await?
            .into_iter()
            .map(|row| row.1)
            .collect_vec();
        let mut columns: HashMap<(Felt, Felt), _> =
            ColumnInfo::get_hash_map(self.pool(), DOJO_COLUMN_TABLE, owners).await?;
        for table in &mut tables {
            for key in table.key_fields.iter().chain(table.value_fields.iter()) {
                let column = columns.remove(&(table.id, *key)).ok_or_else(|| {
                    DojoPgStoreError::column_not_found(table.name.clone(), &(table.id, *key))
                })?;
                table.columns.insert(*key, column);
            }
        }
        Ok(tables)
    }
}
