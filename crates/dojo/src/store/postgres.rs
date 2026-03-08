use super::DojoStoreTrait;
use crate::DojoTable;
use async_trait::async_trait;
use introspect_types::{Attribute, TypeDef};
use itertools::Itertools;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Error as SqlxError, PgPool};
use starknet_types_core::felt::Felt;

pub struct DojoPgStore {
    pub pool: PgPool,
}

impl DojoPgStore {
    pub async fn new(database_url: &str, max_connections: Option<u32>) -> Result<Self, SqlxError> {
        Ok(Self {
            pool: PgPoolOptions::new()
                .max_connections(max_connections.unwrap_or(5))
                .connect(database_url)
                .await?,
        })
    }
}

pub fn felt252_type(value: &Felt) -> String {
    format!("'\\x{}'::felt252", hex::encode(value.to_bytes_be()))
}

pub fn string_type(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn attribute_type(attr: &Attribute) -> String {
    let data = match &attr.data {
        Some(bytes) => format!("'\\x{}'::bytea", hex::encode(bytes)),
        None => "NULL".to_string(),
    };
    format!(
        "ROW({}, {})::introspect.attribute",
        string_type(&attr.name),
        data
    )
}

pub fn make_set_table_query(
    id: &Felt,
    name: &str,
    attributes: &[String],
    keys: &[Felt],
    values: &[Felt],
    legacy: bool,
) -> String {
    format!(
        r#"
        INSERT INTO dojo.table (id, name, attributes, keys, "values", legacy)
            VALUES ({id}, '{name}', ARRAY[{attributes}]::TEXT[], ARRAY[{keys}], ARRAY[{values}], {legacy})
            ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            attributes = EXCLUDED.attributes,
            keys = EXCLUDED.keys,
            "values" = EXCLUDED."values",
            legacy = EXCLUDED.legacy
        "#,
        id = felt252_type(id),
        name = string_type(name),
        attributes = attributes.into_iter().map(|s| string_type(s)).join(","),
        keys = keys.into_iter().map(felt252_type).join(","),
        values = values.into_iter().map(felt252_type).join(","),
    )
}

pub fn make_column_query(
    table: &Felt,
    id: &Felt,
    name: &str,
    attributes: &[Attribute],
    type_def: &TypeDef,
) -> String {
    format!(
        r#"
        INSERT INTO dojo.columns (table_id, column_id, name, attributes, type_def)
            VALUES ({table}, {id}, {name}, ARRAY[{attributes}]::introspect.attribute[], {type_def}::jsonb)
            ON CONFLICT (table_id, column_id) DO UPDATE SET
            name = EXCLUDED.name,
            attributes = EXCLUDED.attributes,
            type_def = EXCLUDED.type_def
        "#,
        table = felt252_type(table),
        id = felt252_type(id),
        name = string_type(name),
        attributes = attributes.iter().map(attribute_type).join(","),
        type_def = string_type(&serde_json::to_string(type_def).unwrap()),
    )
}

#[async_trait]
impl DojoStoreTrait for DojoPgStore {
    type Error = SqlxError;

    async fn save_table(&self, data: &DojoTable) -> Result<(), Self::Error> {
        let transaction = self.pool.begin().await?;
        let query = make_set_table_query(
            &data.id,
            &data.name,
            &data.attributes,
            &data.key_fields,
            &data.value_fields,
            data.legacy,
        );

        for column in data.columns.values() {
            let query = make_column_query(
                &data.id,
                &column.id,
                &column.name,
                &column.attributes,
                &column.type_def,
            );
            sqlx::query(&query).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn load_tables(&self) -> Result<Vec<DojoTable>, Self::Error> {
        // Implement the logic to load all tables from PostgreSQL
        unimplemented!()
    }
}
