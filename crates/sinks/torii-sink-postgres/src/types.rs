use introspect_types::{ColumnDef, EnumDef, FixedArrayDef, StructDef, TypeDef};
use rand::distr::{Alphanumeric, SampleString};
use rand::rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Hash, Clone, Deserialize, Serialize)]
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
        }
    }
}

#[derive(Deserialize, Serialize)]
pub enum PostgresComplexType {
    Struct(Vec<PostgresField>),
    Enum(Vec<String>),
    RustEnum(PgRustEnum),
    Tuple(Vec<PostgresType>),
}

#[derive(Deserialize, Serialize)]
pub struct PgRustEnum {
    pub variants: HashMap<String, PostgresType>,
    pub order: Vec<String>,
    pub variants_type_name: String,
}

#[derive(Hash, Deserialize, Serialize)]
pub struct PostgresField {
    pub name: String,
    pub pg_type: PostgresType,
}

impl PostgresField {
    pub fn new(name: String, pg_type: PostgresType) -> Self {
        Self { name, pg_type }
    }
    pub fn new_complex(name: String, type_name: &str) -> Self {
        Self {
            name,
            pg_type: PostgresType::Struct(type_name.to_string()),
        }
    }
}

fn create_enum_type_query(type_name: String, variants: &[String]) -> String {
    let quoted_variants = variants
        .iter()
        .map(|v| format!("'{}'", v))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{type_name}') THEN
                CREATE TYPE "{type_name}" AS ENUM ({quoted_variants});
            END IF;
        END $$;"#
    )
}

fn create_struct_type_query(type_name: String, fields: Vec<PostgresField>) -> String {
    let field_defs = fields
        .iter()
        .map(|f| format!(r#""{}" {}"#, f.name, f.pg_type.to_string()))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r#"DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{type_name}') THEN
                CREATE TYPE "{type_name}" AS ({field_defs});
            END IF;
        END $$;"#
    )
}

fn truncate(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        None => s,
        Some((idx, _)) => &s[..idx],
    }
}

pub trait PostgresComplexTypes {
    fn add_struct(&mut self, name: &str, fields: Vec<PostgresField>) -> PostgresType;
    fn add_rust_enum(&mut self, name: &str, variants: Vec<(String, PostgresType)>) -> PostgresType;
    fn add_tuple(&mut self, fields: Vec<PostgresType>) -> PostgresType;
    fn create_type_queries(&self) -> Vec<String>;
}

fn name_and_rand(base: &str, length: usize) -> String {
    format!("{}_{}", truncate(base, 31), random_alphanumeric(length))
}

fn random_alphanumeric(length: usize) -> String {
    Alphanumeric.sample_string(&mut rng(), length)
}

impl PostgresComplexTypes for Vec<(String, PostgresComplexType)> {
    fn add_struct(&mut self, name: &str, fields: Vec<PostgresField>) -> PostgresType {
        let name = name_and_rand(name, 31).to_lowercase();
        self.push((name.clone(), PostgresComplexType::Struct(fields)));
        PostgresType::Struct(name)
    }
    fn add_tuple(&mut self, fields: Vec<PostgresType>) -> PostgresType {
        let name = name_and_rand("tuple_", 56).to_lowercase();
        self.push((name.clone(), PostgresComplexType::Tuple(fields)));
        PostgresType::Tuple(name)
    }
    fn add_rust_enum(&mut self, name: &str, variants: Vec<(String, PostgresType)>) -> PostgresType {
        let rand = random_alphanumeric(29);
        let enum_def = PgRustEnum {
            order: variants.iter().map(|(name, _)| name.clone()).collect(),
            variants: variants.into_iter().collect(),
            variants_type_name: format!("{}_v_{}", truncate(name, 31), rand).to_lowercase(),
        };
        let struct_name = format!("{}_{}", truncate(name, 31), rand).to_lowercase();
        self.push((struct_name.clone(), PostgresComplexType::RustEnum(enum_def)));
        PostgresType::Struct(struct_name)
    }

    fn create_type_queries(&self) -> Vec<String> {
        self.iter()
            .map(|(name, complex_type)| match complex_type {
                PostgresComplexType::Struct(fields) => {
                    create_struct_type_query(name.clone(), fields.fields().collect())
                }
                PostgresComplexType::Enum(variant_strings) => {
                    create_enum_type_query(name.clone(), variant_strings)
                }
            })
            .collect()
    }
}

fn extract_types_from_tuple(
    type_defs: &[TypeDef],
    types: &mut Vec<(String, PostgresComplexType)>,
) -> PostgresType {
    let fields = type_defs.iter().map(|t| t.extract_type(types)).collect();
    types.add_tuple(fields)
}

pub trait PostgresTypeExtractor {
    fn extract_type(&self, types: &mut Vec<(String, PostgresComplexType)>) -> PostgresType;
    fn extract_type_string(&self, types: &mut Vec<(String, PostgresComplexType)>) -> String {
        self.extract_type(types).to_string()
    }
}

pub trait PostgresFieldExtractor {
    fn extract_field(&self, types: &mut Vec<(String, PostgresComplexType)>) -> PostgresField;
}

impl PostgresFieldExtractor for ColumnDef {
    fn extract_field(&self, types: &mut Vec<(String, PostgresComplexType)>) -> PostgresField {
        PostgresField::new(self.name.clone(), self.type_def.extract_type(types))
    }
}

impl PostgresTypeExtractor for TypeDef {
    fn extract_type(&self, types: &mut Vec<(String, PostgresComplexType)>) -> PostgresType {
        match self {
            TypeDef::None => PostgresType::None,
            TypeDef::Bool => PostgresType::Boolean,
            TypeDef::I8 | TypeDef::I16 => PostgresType::SmallInt,
            TypeDef::I32 => PostgresType::Int,
            TypeDef::I64 => PostgresType::BigInt,
            TypeDef::U8 => PostgresType::Uint8,
            TypeDef::U16 => PostgresType::Uint16,
            TypeDef::U32 => PostgresType::Uint32,
            TypeDef::U64 => PostgresType::Uint64,
            TypeDef::U128 => PostgresType::Uint128,
            TypeDef::I128 => PostgresType::Int128,
            TypeDef::U256 => PostgresType::Uint256,
            TypeDef::Felt252 => PostgresType::Felt252,
            TypeDef::ContractAddress | TypeDef::ClassHash => PostgresType::StarknetHash,
            TypeDef::EthAddress => PostgresType::EthAddress,
            TypeDef::ByteArray => PostgresType::Text,
            TypeDef::ShortString => PostgresType::Varchar(31),
            TypeDef::Tuple(type_defs) => extract_types_from_tuple(type_defs, types),
            TypeDef::Enum(enum_def) => enum_def.extract_type(types),
            TypeDef::Array(type_def) => {
                PostgresType::Array(Box::new(type_def.extract_type(types)), None)
            }
            TypeDef::FixedArray(fixed_array_def) => fixed_array_def.extract_type(types),
            TypeDef::Struct(struct_def) => struct_def.extract_type(types),
            _ => unimplemented!(),
        }
    }
}

impl PostgresTypeExtractor for FixedArrayDef {
    fn extract_type(&self, types: &mut Vec<(String, PostgresComplexType)>) -> PostgresType {
        PostgresType::Array(
            Box::new(self.type_def.extract_type(types)),
            Some(self.size as u32),
        )
    }
}

impl PostgresTypeExtractor for StructDef {
    fn extract_type(&self, types: &mut Vec<(String, PostgresComplexType)>) -> PostgresType {
        let fields = self
            .fields
            .iter()
            .map(|f| PostgresField::new(f.name.clone(), f.type_def.extract_type(types)))
            .collect();
        types.add_struct(&self.name, fields)
    }
}

impl PostgresTypeExtractor for EnumDef {
    fn extract_type(&self, types: &mut Vec<(String, PostgresComplexType)>) -> PostgresType {
        let variants = self
            .order
            .iter()
            .map(|selector| {
                let variant = &self.variants[selector];
                (variant.name.clone(), variant.type_def.extract_type(types))
            })
            .collect();
        types.add_rust_enum(&self.name, variants)
    }
}
