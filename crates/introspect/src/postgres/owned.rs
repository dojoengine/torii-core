use std::collections::HashMap;

use async_trait::async_trait;
use introspect_types::{Attribute, ColumnDef, ColumnInfo, TypeDef};
use itertools::Itertools;
use sqlx::types::Json;
use sqlx::{FromRow, PgPool};
use starknet_types_core::felt::Felt;

use crate::postgres::{attribute_type, felt252_type, string_type, PgAttribute, PgFelt, SqlxResult};
use crate::schema::ColumnKeyTrait;

#[derive(FromRow)]
pub struct ColumnRow {
    table: PgFelt,
    id: PgFelt,
    name: String,
    attributes: Vec<PgAttribute>,
    type_def: Json<TypeDef>,
}

impl<K> From<ColumnRow> for (K, ColumnInfo)
where
    K: ColumnKeyTrait,
{
    fn from(value: ColumnRow) -> Self {
        (
            K::from_parts(value.table.into(), value.id.into()),
            ColumnInfo {
                name: value.name,
                attributes: value.attributes.into_iter().map(Into::into).collect(),
                type_def: value.type_def.0,
            },
        )
    }
}

impl From<ColumnRow> for (Felt, ColumnDef) {
    fn from(value: ColumnRow) -> Self {
        (
            value.table.into(),
            ColumnDef {
                id: value.id.into(),
                name: value.name,
                attributes: value.attributes.into_iter().map(Into::into).collect(),
                type_def: value.type_def.0,
            },
        )
    }
}

#[async_trait]
pub trait PgTypeDef<Key> {
    type Row;
    fn insert_query(&self, owner: &Felt, key: &Key) -> String;
    async fn get_rows(
        pool: &PgPool,
        pg_table: &str,
        owners: &[Felt],
    ) -> SqlxResult<Vec<(Key, Self)>>
    where
        Self: Sized;
    async fn get_hash_map(
        pool: &PgPool,
        pg_table: &str,
        owners: &[Felt],
    ) -> SqlxResult<HashMap<Key, Self>>
    where
        Self: Sized,
        Key: std::hash::Hash + Eq,
    {
        Self::get_rows(pool, pg_table, owners)
            .await
            .map(|rows| rows.into_iter().collect())
    }
}

#[async_trait]
impl<K> PgTypeDef<K> for ColumnInfo
where
    K: ColumnKeyTrait + Send + Sync,
{
    type Row = ColumnRow;
    fn insert_query(&self, owner: &Felt, key: &K) -> String {
        let (table, id) = key.as_parts();
        column_insert_query(
            owner,
            table,
            id,
            &self.name,
            &self.attributes,
            &self.type_def,
        )
    }

    async fn get_rows(pool: &PgPool, pg_table: &str, owners: &[Felt]) -> SqlxResult<Vec<(K, Self)>>
    where
        Self: Sized,
    {
        get_column_rows(pool, pg_table, owners)
            .await
            .map(|rows| rows.into_iter().map_into().collect_vec())
    }
}

#[async_trait]
impl PgTypeDef<Felt> for ColumnDef {
    type Row = ColumnRow;
    fn insert_query(&self, owner: &Felt, key: &Felt) -> String {
        column_insert_query(
            owner,
            key,
            &self.id,
            &self.name,
            &self.attributes,
            &self.type_def,
        )
    }
    async fn get_rows(
        pool: &PgPool,
        pg_table: &str,
        owners: &[Felt],
    ) -> SqlxResult<Vec<(Felt, Self)>> {
        get_column_rows(pool, pg_table, owners)
            .await
            .map(|rows| rows.into_iter().map_into().collect_vec())
    }
}

fn column_insert_query(
    owner: &Felt,
    table: &Felt,
    id: &Felt,
    name: &str,
    attributes: &[Attribute],
    type_def: &TypeDef,
) -> String {
    format!(
        r#"
            INSERT INTO dojo.columns (owner, "table", id, name, attributes, type_def)
                VALUES ({owner}, {table}, {id}, {name}, ARRAY[{attributes}]::introspect.attribute[], {type_def}::jsonb)
                ON CONFLICT (owner, "table", id) DO UPDATE SET
                name = EXCLUDED.name,
                attributes = EXCLUDED.attributes,
                type_def = EXCLUDED.type_def
            "#,
        owner = felt252_type(owner),
        table = felt252_type(table),
        id = felt252_type(id),
        name = string_type(name),
        attributes = attributes.iter().map(attribute_type).join(","),
        type_def = string_type(&serde_json::to_string(&type_def).unwrap()),
    )
}

async fn get_column_rows(
    pool: &PgPool,
    pg_table: &str,
    owners: &[Felt],
) -> SqlxResult<Vec<ColumnRow>> {
    sqlx::query_as(&get_rows_query(pg_table, owners))
        .fetch_all(pool)
        .await
}

fn get_rows_query(table: &str, owners: &[Felt]) -> String {
    let mut string = format!(
        r#"
            SELECT "table", id, name, attributes, type_def
            FROM {table}
        "#,
    );
    if !owners.is_empty() {
        let owners_str = owners.iter().map(felt252_type).join(", ");
        string.push_str(&format!("WHERE owner IN ({})", owners_str));
    }
    string
}
