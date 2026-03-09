use introspect_types::{Attribute, PrimaryDef, PrimaryTypeDef};
use itertools::Itertools;
use sqlx::types::Json;
use starknet_types_core::felt::Felt;

#[derive(sqlx::Type)]
#[sqlx(type_name = "introspect.attribute")]
pub struct PgAttribute {
    pub name: String,
    pub data: Option<Vec<u8>>,
}

#[derive(sqlx::Type)]
#[sqlx(transparent)]
pub struct PgFelt(pub Vec<u8>);

impl From<PgAttribute> for Attribute {
    fn from(value: PgAttribute) -> Self {
        Attribute {
            name: value.name,
            data: value.data,
        }
    }
}

impl From<Attribute> for PgAttribute {
    fn from(value: Attribute) -> Self {
        PgAttribute {
            name: value.name,
            data: value.data,
        }
    }
}

impl From<PgFelt> for Felt {
    fn from(value: PgFelt) -> Self {
        Felt::from_bytes_be_slice(&value.0)
    }
}

impl From<Felt> for PgFelt {
    fn from(value: Felt) -> Self {
        PgFelt(value.to_bytes_be().to_vec())
    }
}

#[derive(sqlx::Type)]
#[sqlx(type_name = "introspect.primary_def")]
pub struct PgPrimary {
    name: String,
    attributes: Vec<PgAttribute>,
    type_def: Json<PrimaryTypeDef>,
}

impl From<PgPrimary> for PrimaryDef {
    fn from(value: PgPrimary) -> Self {
        PrimaryDef {
            name: value.name,
            attributes: value.attributes.into_iter().map_into().collect(),
            type_def: value.type_def.0,
        }
    }
}

pub fn felt252_type(value: &Felt) -> String {
    format!("'\\x{}'::felt252", hex::encode(value.to_bytes_be()))
}

pub fn felt252_array_type(values: &[Felt]) -> String {
    let value_strs = values.iter().map(felt252_type).join(",");
    format!("ARRAY[{}]::felt252[]", value_strs)
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

pub fn attributes_array_type(attrs: &[Attribute]) -> String {
    let attr_strs = attrs.iter().map(attribute_type).join(",");
    format!("ARRAY[{}]::introspect.attribute[]", attr_strs)
}

pub fn primary_def_type(primary: &PrimaryDef) -> String {
    format!(
        "ROW({name}, {attributes}, {type_def})::introspect.primary_def",
        name = string_type(&primary.name),
        attributes = attributes_array_type(&primary.attributes),
        type_def = string_type(&serde_json::to_string(&primary.type_def).unwrap())
    )
}
