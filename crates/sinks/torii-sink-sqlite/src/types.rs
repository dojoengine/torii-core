use anyhow::{anyhow, bail, Result};
use introspect_types::{ColumnDef, EnumDef, StructDef, TypeDef, VariantDef};
use introspect_value::{Enum as ValueEnum, Struct as ValueStruct, Value};
use primitive_types::U256;
use serde_json::to_string;
use std::collections::HashMap;
use torii_core::format::felt_to_padded_hex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqliteType {
    Integer,
    Text,
}

impl SqliteType {
    pub fn as_sql(&self) -> &'static str {
        match self {
            SqliteType::Integer => "INTEGER",
            SqliteType::Text => "TEXT",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub sql_type: SqliteType,
}

#[derive(Clone, Debug)]
pub enum ColumnValue {
    Null,
    Integer(i64),
    Text(String),
}

pub fn sanitize_identifier(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut chars = name.chars();
    while let Some(ch) = chars.next() {
        // Ensures we don't get the enum with inner types like Some(T).
        match ch {
            '(' | '<' => break,
            ch if ch.is_ascii_alphanumeric() || ch == '_' => result.push(ch.to_ascii_lowercase()),
            _ => result.push('_'),
        }
    }
    if result.is_empty() {
        "_".to_string()
    } else {
        result
    }
}

fn display_variant_name(raw: &str) -> String {
    let trimmed = raw
        .split(|c| matches!(c, '(' | '<'))
        .next()
        .unwrap_or(raw)
        .trim();
    if trimmed.is_empty() {
        raw.trim().to_string()
    } else {
        trimmed.to_string()
    }
}

fn join_identifier(base: &str, segment: &str) -> String {
    let segment = sanitize_identifier(segment);
    if base.is_empty() {
        segment
    } else {
        format!("{base}.{segment}")
    }
}

pub fn base_column_name(column: &ColumnDef) -> String {
    sanitize_identifier(&column.name)
}

pub fn collect_columns(column: &ColumnDef) -> Vec<ColumnInfo> {
    let base = base_column_name(column);
    collect_columns_inner(&base, &column.type_def)
}

fn collect_columns_inner(base: &str, type_def: &TypeDef) -> Vec<ColumnInfo> {
    match type_def {
        TypeDef::None => Vec::new(),
        TypeDef::Bool
        | TypeDef::U8
        | TypeDef::U16
        | TypeDef::U32
        | TypeDef::I8
        | TypeDef::I16
        | TypeDef::I32
        | TypeDef::I64 => vec![ColumnInfo {
            name: base.to_string(),
            sql_type: SqliteType::Integer,
        }],
        TypeDef::Felt252
        | TypeDef::ShortString
        | TypeDef::ClassHash
        | TypeDef::ContractAddress
        | TypeDef::EthAddress
        | TypeDef::ByteArray
        | TypeDef::U64
        | TypeDef::U128
        | TypeDef::U256
        | TypeDef::I128
        | TypeDef::USize => vec![ColumnInfo {
            name: base.to_string(),
            sql_type: SqliteType::Text,
        }],
        TypeDef::Tuple(elements) => {
            let mut columns = Vec::new();
            for (idx, element) in elements.iter().enumerate() {
                let next_base = join_identifier(base, &idx.to_string());
                columns.extend(collect_columns_inner(&next_base, element));
            }
            columns
        }
        TypeDef::Struct(struct_def) => collect_struct_columns(base, struct_def),
        TypeDef::Enum(enum_def) => collect_enum_columns(base, enum_def),
        TypeDef::Array(_)
        | TypeDef::FixedArray(_)
        | TypeDef::Felt252Dict(_)
        | TypeDef::Ref(_)
        | TypeDef::Schema(_)
        | TypeDef::Custom(_)
        | TypeDef::Option(_)
        | TypeDef::Result(_)
        | TypeDef::Nullable(_)
        | TypeDef::Encoding(_)
        | TypeDef::DynamicEncoding => vec![ColumnInfo {
            name: base.to_string(),
            sql_type: SqliteType::Text,
        }],
    }
}

fn collect_struct_columns(base: &str, struct_def: &StructDef) -> Vec<ColumnInfo> {
    let mut columns = Vec::new();
    for field in &struct_def.fields {
        let next_base = join_identifier(base, &field.name);
        columns.extend(collect_columns_inner(&next_base, &field.type_def));
    }
    columns
}

fn collect_enum_columns(base: &str, enum_def: &EnumDef) -> Vec<ColumnInfo> {
    let mut columns = vec![ColumnInfo {
        name: base.to_string(),
        sql_type: SqliteType::Text,
    }];
    for variant in enum_def.variants.values() {
        let next_base = join_identifier(base, &variant.name);
        columns.extend(collect_columns_inner(&next_base, &variant.type_def));
    }
    columns
}

pub fn field_values(
    column: &ColumnDef,
    columns: &[ColumnInfo],
    value: &Value,
) -> Result<Vec<(String, ColumnValue)>> {
    let base = base_column_name(column);
    let mut map = HashMap::new();
    for info in columns {
        map.insert(info.name.clone(), ColumnValue::Null);
    }
    assign_values(&mut map, &base, &column.type_def, value)?;
    let mut ordered = Vec::with_capacity(columns.len());
    for info in columns {
        if let Some(val) = map.remove(&info.name) {
            ordered.push((info.name.clone(), val));
        }
    }
    Ok(ordered)
}

fn assign_values(
    map: &mut HashMap<String, ColumnValue>,
    base: &str,
    type_def: &TypeDef,
    value: &Value,
) -> Result<()> {
    if matches!(value, Value::None) {
        return Ok(());
    }

    match (type_def, value) {
        (TypeDef::None, Value::None) => Ok(()),
        (TypeDef::Bool, Value::Bool(v)) => {
            map.insert(
                base.to_string(),
                ColumnValue::Integer(if *v { 1 } else { 0 }),
            );
            Ok(())
        }
        (TypeDef::U8, Value::U8(v)) => {
            map.insert(base.to_string(), ColumnValue::Integer(i64::from(*v)));
            Ok(())
        }
        (TypeDef::U16, Value::U16(v)) => {
            map.insert(base.to_string(), ColumnValue::Integer(i64::from(*v)));
            Ok(())
        }
        (TypeDef::U32, Value::U32(v)) => {
            map.insert(base.to_string(), ColumnValue::Integer(i64::from(*v)));
            Ok(())
        }
        (TypeDef::I8, Value::I8(v)) => {
            map.insert(base.to_string(), ColumnValue::Integer(i64::from(*v)));
            Ok(())
        }
        (TypeDef::I16, Value::I16(v)) => {
            map.insert(base.to_string(), ColumnValue::Integer(i64::from(*v)));
            Ok(())
        }
        (TypeDef::I32, Value::I32(v)) => {
            map.insert(base.to_string(), ColumnValue::Integer(i64::from(*v)));
            Ok(())
        }
        (TypeDef::I64, Value::I64(v)) => {
            map.insert(base.to_string(), ColumnValue::Integer(*v));
            Ok(())
        }
        (TypeDef::Felt252, Value::Felt252(v))
        | (TypeDef::ClassHash, Value::ClassHash(v))
        | (TypeDef::ContractAddress, Value::ContractAddress(v))
        | (TypeDef::EthAddress, Value::EthAddress(v)) => {
            map.insert(base.to_string(), ColumnValue::Text(felt_to_padded_hex(v)));
            Ok(())
        }
        (TypeDef::ShortString, Value::ShortString(v))
        | (TypeDef::ByteArray, Value::ByteArray(v)) => {
            map.insert(base.to_string(), ColumnValue::Text(v.clone()));
            Ok(())
        }
        (TypeDef::U64, Value::U64(v)) => {
            map.insert(base.to_string(), ColumnValue::Text(v.to_string()));
            Ok(())
        }
        (TypeDef::U128, Value::U128(v)) => {
            map.insert(base.to_string(), ColumnValue::Text(v.to_string()));
            Ok(())
        }
        (TypeDef::I128, Value::I128(v)) => {
            map.insert(base.to_string(), ColumnValue::Text(v.to_string()));
            Ok(())
        }
        (TypeDef::USize, Value::USize(v)) => {
            map.insert(base.to_string(), ColumnValue::Text(v.to_string()));
            Ok(())
        }
        (TypeDef::U256, Value::U256(v)) => {
            map.insert(base.to_string(), ColumnValue::Text(u256_to_padded_hex(v)));
            Ok(())
        }
        (TypeDef::Tuple(types), Value::Tuple(values)) => {
            if types.len() != values.len() {
                bail!("tuple length mismatch for column {base}");
            }
            for (idx, (ty, val)) in types.iter().zip(values.iter()).enumerate() {
                let next_base = join_identifier(base, &idx.to_string());
                assign_values(map, &next_base, ty, val)?;
            }
            Ok(())
        }
        (TypeDef::Struct(struct_def), Value::Struct(struct_value)) => {
            assign_struct(map, base, &struct_def, struct_value)
        }
        (TypeDef::Enum(enum_def), Value::Enum(enum_value)) => {
            assign_enum(map, base, &enum_def, enum_value)
        }
        (
            TypeDef::Array(_)
            | TypeDef::FixedArray(_)
            | TypeDef::Felt252Dict(_)
            | TypeDef::Ref(_)
            | TypeDef::Schema(_)
            | TypeDef::Custom(_)
            | TypeDef::Option(_)
            | TypeDef::Result(_)
            | TypeDef::Nullable(_)
            | TypeDef::Encoding(_)
            | TypeDef::DynamicEncoding,
            _,
        ) => {
            map.insert(base.to_string(), ColumnValue::Text(to_string(value)?));
            Ok(())
        }
        _ => Err(anyhow!(
            "unsupported value {value:?} for column type {type_def:?}"
        )),
    }
}

fn assign_struct(
    map: &mut HashMap<String, ColumnValue>,
    base: &str,
    struct_def: &StructDef,
    struct_value: &ValueStruct,
) -> Result<()> {
    let field_map: HashMap<&str, &Value> = struct_value
        .fields
        .iter()
        .map(|field| (field.name.as_str(), &field.value))
        .collect();
    for field_def in &struct_def.fields {
        let next_base = join_identifier(base, &field_def.name);
        if let Some(value) = field_map.get(field_def.name.as_str()) {
            assign_values(map, &next_base, &field_def.type_def, value)?;
        } else {
            // leave as null if data not present
        }
    }
    Ok(())
}

fn assign_enum(
    map: &mut HashMap<String, ColumnValue>,
    base: &str,
    enum_def: &EnumDef,
    enum_value: &ValueEnum,
) -> Result<()> {
    let variant_column = base.to_string();
    map.insert(
        variant_column,
        ColumnValue::Text(display_variant_name(&enum_value.variant)),
    );

    let variant = find_variant(enum_def, &enum_value.variant)
        .ok_or_else(|| anyhow!("unknown enum variant {}", enum_value.variant))?;

    let next_base = join_identifier(base, &variant.name);

    assign_values(map, &next_base, &variant.type_def, &enum_value.value)
}

fn find_variant<'a>(enum_def: &'a EnumDef, name: &str) -> Option<&'a VariantDef> {
    enum_def
        .variants
        .values()
        .find(|variant| variant.name == name)
}

pub fn u256_to_padded_hex(value: &U256) -> String {
    format!("{:#066x}", value)
}
