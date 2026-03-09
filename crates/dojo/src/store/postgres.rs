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
use torii_introspect::schema::ColumnKeyTrait;

const DOJO_COLUMN_TABLE: &str = "dojo.column";
const DOJO_TABLE_TABLE: &str = "dojo.table";

#[derive(Debug, thiserror::Error)]
pub enum DojoPgStoreError {
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
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
            name: name,
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
    fn insert_query(&self, owner: &Felt, key: &Felt) -> String {
        make_set_table_query(
            owner,
            key,
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
        owners: &[Felt],
    ) -> SqlxResult<Vec<(Felt, DojoTableInfo)>> {
        get_dojo_table_rows(pool, pg_table, owners)
            .await
            .map(|rows| rows.into_iter().map_into().collect())
    }
}

#[async_trait]
impl PgTypeDef<()> for DojoTable {
    type Row = TableRow;
    fn insert_query(&self, owner: &Felt, _key: &()) -> String {
        make_set_table_query(
            owner,
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
        owners: &[Felt],
    ) -> SqlxResult<Vec<((), DojoTable)>> {
        get_dojo_table_rows(pool, pg_table, owners)
            .await
            .map(|rows| rows.into_iter().map_into().collect_vec())
    }
}

async fn get_dojo_table_rows(
    pool: &PgPool,
    pg_table: &str,
    owners: &[Felt],
) -> SqlxResult<Vec<TableRow>> {
    let mut query =
        format!("SELECT owner, id, name, attributes, keys, \"values\", legacy FROM {pg_table}",);
    if !owners.is_empty() {
        let owner_list = owners.iter().map(felt252_type).join(",");
        query.push_str(&format!(" WHERE owner IN ({})", owner_list));
    }
    sqlx::query_as(&query).fetch_all(pool).await
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
    owner: &Felt,
    id: &Felt,
    name: &str,
    attributes: &[String],
    keys: &[Felt],
    values: &[Felt],
    legacy: bool,
) -> String {
    format!(
        r#"
        INSERT INTO dojo.table (owner, id, name, attributes, keys, "values", legacy)
            VALUES ({owner}, {id}, {name}, ARRAY[{attributes}]::TEXT[], ARRAY[{keys}], ARRAY[{values}], {legacy})
            ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            attributes = EXCLUDED.attributes,
            keys = EXCLUDED.keys,
            "values" = EXCLUDED."values",
            legacy = EXCLUDED.legacy
        "#,
        owner = felt252_type(owner),
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

    async fn save_table(&self, owner: &Felt, data: &DojoTable) -> Result<(), Self::Error> {
        let mut transaction = self.begin().await?;
        let query = make_set_table_query(
            owner,
            &data.id,
            &data.name,
            &data.attributes,
            &data.key_fields,
            &data.value_fields,
            data.legacy,
        );
        sqlx::query(&query).execute(&mut *transaction).await?;
        for (id, column) in &data.columns {
            sqlx::query(&column.insert_query(&owner, &(data.id, *id)))
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await.err_into()
    }

    async fn load_tables(&self, owners: &[Felt]) -> Result<Vec<DojoTable>, Self::Error> {
        let mut tables = DojoTable::get_rows(self, DOJO_TABLE_TABLE, owners)
            .await?
            .into_iter()
            .map(|r| r.1)
            .collect_vec();
        let mut columns: HashMap<(Felt, Felt), _> =
            ColumnInfo::get_hash_map(self, DOJO_COLUMN_TABLE, owners).await?;
        for table in tables.iter_mut() {
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
