use crate::sqlite::types::SqliteType;
use crate::{TableResult, UpgradeError, UpgradeResult};
use introspect_types::{Attribute, ColumnDef, ColumnInfo, PrimaryDef, TypeDef};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::{Result as JsonResult, Serializer as JsonSerializer};
use sqlx::Arguments;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fmt::{Display, Write};
use torii_sql::types::SqlFelt;
use torii_sql::{Queries, SqliteArguments, SqliteQuery};

pub const FETCH_TABLES_QUERY: &str = r#"
    SELECT namespace, id, owner, name, "primary", columns, alive
    FROM introspect_db_tables
    ORDER BY updated_at ASC
"#;

const INSERT_TABLE_QUERY: &str = r#" INSERT INTO introspect_db_tables
    (namespace, id, owner, name, "primary", columns, updated_at)
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, unixepoch())
    ON CONFLICT (namespace, id) DO UPDATE SET 
    owner = excluded.owner, name = excluded.name, "primary" = excluded."primary", columns = excluded.columns, dead = excluded.dead, alive = excluded.alive, updated_at = unixepoch()
"#;

struct TableName<'a>(&'a str, &'a str);

impl Display for TableName<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.0.is_empty() {
            write!(f, "{}__", self.0)?;
        }
        self.1.fmt(f)
    }
}

pub fn qualified_table_name(namespace: &str, table_name: &str) -> String {
    if namespace.is_empty() {
        table_name.to_string()
    } else {
        format!("{}__{}", namespace, table_name)
    }
}

pub fn serialize_columns<'a>(columns: Vec<impl Into<ColumnDefRef<'a>>>) -> JsonResult<String> {
    let mut data = Vec::new();
    let mut serializer = JsonSerializer::new(&mut data);
    let mut array = serializer.serialize_map(Some(columns.len()))?;
    for column in columns {
        let column_ref: ColumnDefRef = column.into();
        array.serialize_entry(column_ref.id, &column_ref)?;
    }
    array.end()?;
    Ok(unsafe { String::from_utf8_unchecked(data) })
}

pub fn persist_table_state_query<'a>(
    namespace: &str,
    id: &Felt,
    name: &str,
    primary: &PrimaryDef,
    columns: &[impl Into<ColumnDefRef<'a>>],
    from_address: &Felt,
    _block_number: u64,
    _transaction_hash: &Felt,
    queries: &mut Vec<SqliteQuery>,
) -> TableResult<()> {
    let mut args = SqliteArguments::default();
    args.add(namespace.to_string())?;
    args.add(Into::<SqlFelt>::into(*id))?;
    args.add(Into::<SqlFelt>::into(*from_address))?;
    args.add(name.to_string())?;
    args.add(serde_json::to_string(primary)?)?;
    args.add(serialize_columns(columns)?)?;
    queries.add((INSERT_TABLE_QUERY, args));
    Ok(())
}

pub fn create_table_query(
    namespace: &str,
    name: &str,
    primary: &PrimaryDef,
    columns: &[ColumnDef],
) -> TableResult<String> {
    let table_name = TableName(namespace, name);
    let mut query = format!(
        r#"CREATE TABLE IF NOT EXISTS "{table_name}" ("{}" {} PRIMARY KEY "#,
        primary.name,
        TryInto::<SqliteType>::try_into(primary)?
    );
    for column in columns {
        let sql_type: SqliteType = column.try_into()?;
        write!(query, r#", "{}" {sql_type}"#, column.name).unwrap();
    }
    query.push_str(");");
    Ok(query)
}

pub fn update_columns(
    columns: &mut HashMap<Felt, ColumnInfo>,
    table_name: &str,
    new: &[ColumnDef],
    queries: &mut Vec<SqliteQuery>,
) -> TableResult<()> {
    for column in new {
        match columns.get_mut(&column.id) {
            Some(existing) => {
                update_column(
                    table_name,
                    existing,
                    &column.name,
                    &column.type_def,
                    queries,
                )?;
            }
            None => {
                queries.add(create_column_query(table_name, column)?);
                columns.insert(
                    column.id,
                    ColumnInfo {
                        name: column.name.clone(),
                        type_def: column.type_def.clone(),
                        attributes: column.attributes.clone(),
                    },
                );
            }
        }
    }
    Ok(())
}

pub fn update_column(
    table_name: &str,
    column: &mut ColumnInfo,
    new_name: &str,
    new_type: &TypeDef,
    queries: &mut Vec<SqliteQuery>,
) -> UpgradeResult {
    use introspect_types::TypeDef::{
        Array, Bool, ByteArray, ByteArrayEncoded, Bytes31, Bytes31Encoded, ClassHash,
        ContractAddress, Custom, Enum, EthAddress, Felt252, FixedArray, Nullable,
        Option as TDOption, Result as TDResult, ShortUtf8, StorageAddress, StorageBaseAddress,
        Struct, Tuple, Utf8String, I128, I16, I32, I64, I8, U128, U16, U256, U32, U512, U64, U8,
    };
    if column.name != new_name {
        queries.add(format!(
            r#"ALTER TABLE "{table_name}" RENAME COLUMN "{}" TO "{new_name}";"#,
            column.name
        ));
        column.name = new_name.to_string();
    }
    let cast = match (&column.type_def, &new_type) {
        (Bool | U8 | U16 | U32, Bool | U8 | U16 | U32 | I8 | I16 | I32 | I64)
        | (I8 | I16 | I32 | I64, I8 | I16 | I32 | I64)
        | (U64 | U128 | U256 | U512, U64 | U128 | U256 | U512 | I128)
        | (I128, I128)
        | (
            Felt252 | ClassHash | ContractAddress | EthAddress | StorageAddress
            | StorageBaseAddress,
            Felt252 | ClassHash | ContractAddress | EthAddress | StorageAddress
            | StorageBaseAddress,
        )
        | (ShortUtf8 | Utf8String, ShortUtf8 | Utf8String)
        | (
            Bytes31 | Bytes31Encoded(_) | ByteArray | ByteArrayEncoded(_) | Custom(_),
            Bytes31 | Bytes31Encoded(_) | ByteArray | ByteArrayEncoded(_) | Custom(_),
        )
        | (
            Tuple(_) | Array(_) | FixedArray(_) | Struct(_) | Enum(_) | TDOption(_) | TDResult(_)
            | Nullable(_),
            Tuple(_) | Array(_) | FixedArray(_) | Struct(_) | Enum(_) | TDOption(_) | TDResult(_)
            | Nullable(_),
        ) => None,
        (Bool | U8 | U16 | U32, U64 | U128 | U256 | U512 | I128) | (I8 | I16 | I32 | I64, I128) => {
            Some(format!(r#"CAST("{new_name}" AS TEXT)"#))
        }
        (
            Bool | U8 | U16 | U32,
            Felt252 | ClassHash | ContractAddress | EthAddress | StorageAddress
            | StorageBaseAddress,
        ) => Some(format!(r#"printf('0x%064x', "{new_name}")"#)),
        _ => return UpgradeError::type_upgrade_err(&column.type_def, &new_type),
    };
    column.type_def = new_type.clone();
    if let Some(cast) = cast {
        queries.add(format!(
            r#"UPDATE "{table_name}" SET "{new_name}" = {cast};"#
        ));
    }
    Ok(())
}

pub fn create_column_query(table_name: &str, column: &ColumnDef) -> TableResult<String> {
    let sql_type: SqliteType = column.try_into()?;
    Ok(format!(
        r#"ALTER TABLE "{table_name}" ADD COLUMN "{}" {sql_type};"#,
        column.name
    ))
}
