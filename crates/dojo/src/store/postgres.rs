use super::DojoStoreTrait;
use crate::decoder::primary_field_def;
use crate::table::DojoTableInfo;
use crate::DojoTable;
use async_trait::async_trait;
use introspect_types::{ColumnInfo, ResultInto};
use itertools::Itertools;
use sqlx::{FromRow, PgPool};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use torii_introspect::postgres::owned::PgTypeDef;
use torii_introspect::postgres::{PgFelt, SqlxResult};
use torii_introspect::schema::{ColumnKeyTrait, TableKeyTrait};
use torii_introspect::{ColumnKey, TableKey};

const DOJO_COLUMN_TABLE: &str = "dojo.column";
const DOJO_TABLE_TABLE: &str = "dojo.table";

#[derive(Debug, thiserror::Error)]
pub enum DojoPgStoreError {
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
    #[error("Column not found for owner {owner:?} table {table_id} and column {column_id}")]
    ColumnNotFound {
        owner: Option<Felt>,
        table_id: Felt,
        column_id: Felt,
    },
}

impl DojoPgStoreError {
    pub fn column_not_found<K: ColumnKeyTrait>(key: K) -> Self {
        let (owner, table_id, column_id) = key.as_parts();
        Self::ColumnNotFound {
            owner: owner.cloned(),
            table_id: *table_id,
            column_id: *column_id,
        }
    }
}

#[derive(FromRow)]
pub struct TableRow {
    owner: Option<PgFelt>,
    id: PgFelt,
    name: String,
    attributes: Vec<String>,
    keys: Vec<PgFelt>,
    #[sqlx(rename = "values")]
    values: Vec<PgFelt>,
    legacy: bool,
}

impl From<TableRow> for DojoTable {
    fn from(value: TableRow) -> Self {
        DojoTable {
            owner: value.owner.map(Into::into),
            id: value.id.into(),
            name: value.name,
            attributes: value.attributes,
            primary: primary_field_def(),
            columns: HashMap::new(),
            key_fields: value.keys.into_iter().map_into().collect(),
            value_fields: value.values.into_iter().map_into().collect(),
            legacy: value.legacy,
        }
    }
}

impl From<TableRow> for ((), DojoTable) {
    fn from(value: TableRow) -> Self {
        ((), value.into())
    }
}

impl<K: TableKeyTrait> From<TableRow> for (K, DojoTableInfo) {
    fn from(value: TableRow) -> Self {
        (
            K::from_parts(value.owner.map(Into::into), value.id.into()),
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
impl<K: TableKeyTrait> PgTypeDef<K> for DojoTableInfo {
    type Row = TableRow;
    fn insert_query(&self, key: &K) -> String {
        let (owner, id) = key.as_parts();
        make_set_table_query(
            owner,
            id,
            &self.name,
            &self.attributes,
            &self.key_fields,
            &self.value_fields,
            self.legacy,
        )
    }
    async fn get_rows(
        pool: &PgPool,
        pg_table: &str,
        contract: Option<&Felt>,
    ) -> SqlxResult<Vec<(K, DojoTableInfo)>> {
        get_dojo_table_rows(pool, pg_table, contract)
            .await
            .map(|rows| rows.into_iter().map_into().collect())
    }
}

#[async_trait]
impl PgTypeDef<()> for DojoTable {
    type Row = TableRow;
    fn insert_query(&self, _key: &()) -> String {
        make_set_table_query(
            self.owner.as_ref(),
            &self.id,
            &self.name,
            &self.attributes,
            &self.key_fields,
            &self.value_fields,
            self.legacy,
        )
    }
    async fn get_rows(
        pool: &PgPool,
        pg_table: &str,
        contract: Option<&Felt>,
    ) -> SqlxResult<Vec<((), DojoTable)>> {
        get_dojo_table_rows(pool, pg_table, contract)
            .await
            .map(|rows| rows.into_iter().map_into().collect_vec())
    }
}

async fn get_dojo_table_rows(
    pool: &PgPool,
    pg_table: &str,
    contract: Option<&Felt>,
) -> SqlxResult<Vec<TableRow>> {
    let mut query =
        format!("SELECT contract, id, name, attributes, keys, \"values\", legacy FROM {pg_table}",);
    if let Some(contract) = contract {
        query.push_str(&format!(" WHERE contract = {}", felt252_type(contract)));
    }
    sqlx::query_as(&query).fetch_all(pool).await
}

#[async_trait]
pub trait DojoPgStore {
    async fn get_table_rows(&self) -> SqlxResult<Vec<TableRow>>;
    async fn get_columns(&self) -> SqlxResult<HashMap<ColumnKey, ColumnInfo>>;
}

#[async_trait]
impl DojoPgStore for PgPool {
    async fn get_table_rows(&self) -> SqlxResult<Vec<TableRow>> {
        sqlx::query_as(&format!(
            "SELECT contract, id, name, attributes, keys, \"values\", legacy FROM {DOJO_TABLE_TABLE}",
        ))
        .fetch_all(self)
        .await
    }
    async fn get_columns(&self) -> SqlxResult<HashMap<ColumnKey, ColumnInfo>> {
        ColumnInfo::get_rows(self, DOJO_COLUMN_TABLE, None)
            .await
            .map(|rows| rows.into_iter().collect())
    }
}

pub fn felt252_type(value: &Felt) -> String {
    format!("'\\x{}'::felt252", hex::encode(value.to_bytes_be()))
}

pub fn string_type(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn parse_column_ids(ids: Vec<Vec<u8>>) -> Vec<Felt> {
    ids.iter().map(|b| Felt::from_bytes_be_slice(b)).collect()
}

pub fn make_set_table_query(
    contract: Option<&Felt>,
    id: &Felt,
    name: &str,
    attributes: &[String],
    keys: &[Felt],
    values: &[Felt],
    legacy: bool,
) -> String {
    format!(
        r#"
        INSERT INTO dojo.table (contract, id, name, attributes, keys, "values", legacy)
            VALUES ({contract}, {id}, {name}, ARRAY[{attributes}]::TEXT[], ARRAY[{keys}], ARRAY[{values}], {legacy})
            ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            attributes = EXCLUDED.attributes,
            keys = EXCLUDED.keys,
            "values" = EXCLUDED."values",
            legacy = EXCLUDED.legacy
        "#,
        contract = contract
            .map(felt252_type)
            .unwrap_or_else(|| "NULL".to_string()),
        id = felt252_type(id),
        name = string_type(name),
        attributes = attributes.into_iter().map(|s| string_type(s)).join(","),
        keys = keys.into_iter().map(felt252_type).join(","),
        values = values.into_iter().map(felt252_type).join(","),
    )
}

#[async_trait]
impl DojoStoreTrait for PgPool {
    type Error = DojoPgStoreError;

    async fn save_table(&self, data: &DojoTable) -> Result<(), Self::Error> {
        let mut transaction = self.begin().await?;
        let query = make_set_table_query(
            data.owner.as_ref(),
            &data.id,
            &data.name,
            &data.attributes,
            &data.key_fields,
            &data.value_fields,
            data.legacy,
        );
        sqlx::query(&query).execute(&mut *transaction).await?;
        for (id, column) in &data.columns {
            sqlx::query(&column.insert_query(&(data.owner, data.id, *id)))
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await.err_into()
    }

    async fn load_tables(&self) -> Result<Vec<DojoTable>, Self::Error> {
        let mut tables = DojoTable::get_rows(self, DOJO_TABLE_TABLE, None)
            .await?
            .into_iter()
            .map(|r| r.1)
            .collect_vec();
        let mut columns: HashMap<(TableKey, Felt), _> =
            ColumnInfo::get_hash_map(self, DOJO_COLUMN_TABLE, None).await?;
        for table in tables.iter_mut() {
            let table_key = table.key();
            for key in table.key_fields.iter().chain(table.value_fields.iter()) {
                let column = columns
                    .remove(&(table_key, *key))
                    .ok_or(DojoPgStoreError::column_not_found((table_key, *key)))?;
                table.columns.insert(*key, column);
            }
        }
        Ok(tables)
    }
}
