use introspect_types::{ColumnDef, ColumnInfo, FeltIds, PrimaryDef};
use itertools::Itertools;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use thiserror::Error;
use torii_introspect::schema::TableSchema;
use torii_introspect::tables::RecordSchema;

#[derive(Debug, Error)]
pub enum SqliteTableError {
    #[error("Column with id: {0} not found in table {1}")]
    ColumnNotFound(Felt, String),
}

pub type TableResult<T> = std::result::Result<T, SqliteTableError>;

#[derive(Debug, Clone)]
pub struct SqliteTable {
    pub name: String,
    pub storage_name: String,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnInfo>,
    pub order: Vec<Felt>,
    pub upsert_sql: String,
    pub alive: bool,
}

impl SqliteTable {
    pub fn new(
        storage_name: String,
        name: String,
        primary: PrimaryDef,
        columns: Vec<ColumnDef>,
    ) -> Self {
        Self {
            name,
            storage_name,
            primary,
            order: columns.ids(),
            columns: columns.into_iter().map_into().collect(),
            upsert_sql: String::new(),
            alive: true,
        }
        .with_upsert_sql()
    }

    pub fn new_from_table(namespace: &str, table: impl Into<TableSchema>) -> (Felt, Self) {
        let table = table.into();
        let storage_name = if namespace.is_empty() {
            table.name.clone()
        } else {
            format!("{namespace}__{}", table.name)
        };
        (
            table.id,
            Self::new(storage_name, table.name, table.primary, table.columns),
        )
    }

    pub fn get_column(&self, selector: &Felt) -> TableResult<&ColumnInfo> {
        self.columns
            .get(selector)
            .ok_or_else(|| SqliteTableError::ColumnNotFound(*selector, self.name.clone()))
    }

    pub fn get_schema(&self, column_ids: &[Felt]) -> TableResult<RecordSchema<'_>> {
        let columns = column_ids
            .iter()
            .map(|selector| self.get_column(selector))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordSchema::new(&self.primary, columns))
    }

    fn with_upsert_sql(mut self) -> Self {
        self.upsert_sql = build_upsert_sql(&self);
        self
    }
}

fn sqlite_column_type(type_def: &introspect_types::TypeDef) -> &'static str {
    if matches!(
        type_def,
        introspect_types::TypeDef::Struct(_)
            | introspect_types::TypeDef::Enum(_)
            | introspect_types::TypeDef::Tuple(_)
            | introspect_types::TypeDef::Array(_)
            | introspect_types::TypeDef::FixedArray(_)
            | introspect_types::TypeDef::Option(_)
            | introspect_types::TypeDef::Nullable(_)
            | introspect_types::TypeDef::Result(_)
    ) {
        "JSONB"
    } else {
        ""
    }
}

fn build_upsert_sql(table: &SqliteTable) -> String {
    let column_names = std::iter::once(table.primary.name.as_str())
        .chain(table.order.iter().map(|id| table.columns[id].name.as_str()))
        .collect::<Vec<_>>();
    let column_type_defs = table
        .order
        .iter()
        .map(|id| &table.columns[id].type_def)
        .collect::<Vec<_>>();

    let placeholders = std::iter::once("?".to_string())
        .chain(column_type_defs.iter().map(|td| {
            if sqlite_column_type(td) == "JSONB" {
                "jsonb(?)".to_string()
            } else {
                "?".to_string()
            }
        }))
        .collect::<Vec<_>>()
        .join(", ");

    let update_columns = column_names
        .iter()
        .skip(1)
        .zip(column_type_defs.iter())
        .map(|(name, td)| {
            if sqlite_column_type(td) == "JSONB" {
                format!(
                    r#""{name}" = COALESCE(jsonb(excluded."{name}"), "{table_name}"."{name}")"#,
                    table_name = table.storage_name
                )
            } else {
                format!(
                    r#""{name}" = COALESCE(excluded."{name}", "{table_name}"."{name}")"#,
                    table_name = table.storage_name
                )
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r#"INSERT INTO "{}" ({}) VALUES ({}) ON CONFLICT("{}") DO UPDATE SET {}"#,
        table.storage_name,
        column_names
            .iter()
            .map(|name| format!(r#""{name}""#))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders,
        table.primary.name,
        update_columns
    )
}
