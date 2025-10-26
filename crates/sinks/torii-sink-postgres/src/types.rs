use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use xxhash_rust::xxh3::Xxh3;

use crate::utils::HasherExt;

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
pub enum PostgresType {
    None,
    Boolean,
    SmallInt, // i16
    Int,      // i32
    BigInt,   // i64
    Int128,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Uint128,
    Uint256,
    Felt252,
    StarknetHash,
    EthAddress,
    Numeric(u32, u32),
    Text,
    Char(u32),
    Varchar(u32),
    Json,
    JsonB,
    Array(Box<PostgresType>, Option<u32>),
    Struct(String),
    Tuple(String),
    Enum(String),
    RustEnum(String),
}

impl ToString for PostgresType {
    fn to_string(&self) -> String {
        match self {
            PostgresType::None => "VOID".to_string(),
            PostgresType::Boolean => "BOOLEAN".to_string(),
            PostgresType::SmallInt => "SMALLINT".to_string(),
            PostgresType::Int => "INTEGER".to_string(),
            PostgresType::BigInt => "BIGINT".to_string(),
            PostgresType::Int128 => "int128".to_string(),
            PostgresType::Uint8 => "uint8".to_string(),
            PostgresType::Uint16 => "uint16".to_string(),
            PostgresType::Uint32 => "uint32".to_string(),
            PostgresType::Uint64 => "uint64".to_string(),
            PostgresType::Uint128 => "uint128".to_string(),
            PostgresType::Uint256 => "uint256".to_string(),
            PostgresType::Felt252 => "felt252".to_string(),
            PostgresType::StarknetHash => "starknet_hash".to_string(),
            PostgresType::EthAddress => "eth_address".to_string(),
            PostgresType::Numeric(precision, scale) => {
                format!("NUMERIC({}, {})", precision, scale)
            }
            PostgresType::Text => "TEXT".to_string(),
            PostgresType::Char(size) => format!("CHAR({})", size),
            PostgresType::Varchar(size) => format!("VARCHAR({})", size),
            PostgresType::Json => "JSON".to_string(),
            PostgresType::JsonB => "JSONB".to_string(),
            PostgresType::Array(element_type, size) => match size {
                Some(s) => format!("{}[{}]", element_type.to_string(), s),
                None => format!("{}[]", element_type.to_string()),
            },
            PostgresType::Struct(type_name) => type_name.clone(),
            PostgresType::Enum(type_name) => type_name.clone(),
            PostgresType::RustEnum(type_name) => type_name.clone(),
            PostgresType::Tuple(type_name) => type_name.clone(),
        }
    }
}

pub fn variant_member_name(variant: &str) -> String {
    format!("_{}", variant)
}

#[derive(Deserialize, Serialize)]
pub struct PgStructDef {
    pub fields: HashMap<String, PostgresType>,
    pub order: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PgRustEnum {
    pub variants: HashMap<String, PostgresType>,
    pub order: Vec<String>,
    pub variants_type_name: String,
}

#[derive(Deserialize, Serialize)]
pub struct PostgresField {
    pub name: String,
    pub pg_type: PostgresType,
}

impl Into<(String, PostgresType)> for PostgresField {
    fn into(self) -> (String, PostgresType) {
        (self.name, self.pg_type)
    }
}

impl PgRustEnum {
    pub fn new(branch: &Xxh3, name: &str, variants: Vec<PostgresField>) -> (String, Self) {
        let struct_name = branch.type_name(name);
        let variants_type_name = branch.branch_to_type_name("variants", name);
        let order = variants.iter().map(|f| f.name.clone()).collect();
        let variants_map = variants.into_iter().map(PostgresField::into).collect();
        (
            struct_name.to_string(),
            Self {
                order,
                variants: variants_map,
                variants_type_name,
            },
        )
    }

    pub fn variant_fields(&self) -> Vec<PostgresField> {
        self.order
            .iter()
            .filter_map(|v| match self.variants.get(v)? {
                PostgresType::None => None,
                pg_type => Some(PostgresField::new(variant_member_name(v), pg_type.clone())),
            })
            .collect()
    }

    pub fn all_fields(&self) -> Vec<PostgresField> {
        let mut fields = vec![PostgresField::new_enum(
            "variant".to_string(),
            &self.variants_type_name,
        )];
        fields.extend(self.variant_fields());
        fields
    }
}

impl PostgresField {
    pub fn new(name: String, pg_type: PostgresType) -> Self {
        Self { name, pg_type }
    }
    pub fn new_struct(name: String, type_name: &str) -> Self {
        Self {
            name,
            pg_type: PostgresType::Struct(type_name.to_string()),
        }
    }
    pub fn new_enum(name: String, type_name: &str) -> Self {
        Self {
            name,
            pg_type: PostgresType::Enum(type_name.to_string()),
        }
    }
    pub fn new_rust_enum(name: String, type_name: &str) -> Self {
        Self {
            name,
            pg_type: PostgresType::RustEnum(type_name.to_string()),
        }
    }

    pub fn new_tuple(name: String, type_name: &str) -> Self {
        Self {
            name,
            pg_type: PostgresType::Tuple(type_name.to_string()),
        }
    }
}
