use introspect_types::{EnumDef, FixedArrayDef, StructDef, TypeDef};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Hash, Deserialize, Serialize)]
pub enum PostgresType {
    Boolean,
    SmallInt, // i16
    Int,      // i32
    BigInt,   // i64
    Int128,
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
    Composite(String),
}

impl ToString for PostgresType {
    fn to_string(&self) -> String {
        match self {
            PostgresType::Boolean => "BOOLEAN".to_string(),
            PostgresType::SmallInt => "SMALLINT".to_string(),
            PostgresType::Int => "INTEGER".to_string(),
            PostgresType::BigInt => "BIGINT".to_string(),
            PostgresType::Int128 => "int128".to_string(),
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
            PostgresType::Composite(type_name) => type_name.clone(),
        }
    }
}

#[derive(Hash, Deserialize, Serialize)]
pub enum PostgresComplexType {
    Struct(Vec<PostgresField>),
    Enum(Vec<String>),
}

#[derive(Hash, Deserialize, Serialize)]
pub struct PostgresField {
    name: String,
    ptype: PostgresType,
}

impl PostgresField {
    pub fn new(name: String, ptype: PostgresType) -> Self {
        Self { name, ptype }
    }
}

#[derive(Default, Deserialize, Serialize)]
pub struct PostgresTypes {
    types: HashMap<u64, PostgresComplexType>,
    to_create: Vec<u64>,
}

fn hash_value<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn get_type_name(hash: u64) -> String {
    format!("type_{hash:x}")
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

fn create_struct_type_query(type_name: String, fields: &[PostgresField]) -> String {
    let field_defs = fields
        .iter()
        .map(|f| format!(r#""{}" {}"#, f.name, f.ptype.to_string()))
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

impl PostgresTypes {
    pub fn add_struct(&mut self, fields: Vec<PostgresField>) -> String {
        let ptype = PostgresComplexType::Struct(fields);
        let hash = hash_value(&ptype);
        if !self.types.contains_key(&hash) {
            self.types.insert(hash, ptype);
            self.to_create.push(hash);
        }
        get_type_name(hash)
    }
    pub fn add_enum(&mut self, variant_strings: Vec<String>) -> String {
        let hash = hash_value(&variant_strings);
        if !self.types.contains_key(&hash) {
            self.types
                .insert(hash, PostgresComplexType::Enum(variant_strings));
            self.to_create.push(hash);
        }
        get_type_name(hash)
    }

    pub fn create_type_queries(&self) -> Vec<String> {
        self.to_create
            .iter()
            .map(|hash| match &self.types[hash] {
                PostgresComplexType::Struct(fields) => {
                    create_struct_type_query(get_type_name(*hash), fields)
                }
                PostgresComplexType::Enum(variant_strings) => {
                    create_enum_type_query(get_type_name(*hash), variant_strings)
                }
            })
            .collect()
    }

    pub fn consume_create_types(&mut self) -> Vec<String> {
        let queries = self.create_type_queries();
        self.to_create.clear();
        queries
    }
}

fn extract_types_from_tuple(type_defs: &[TypeDef], types: &mut PostgresTypes) -> String {
    let fields = type_defs
        .iter()
        .enumerate()
        .map(|(i, t)| PostgresField::new(format!("_{i}"), t.extract_type(types)))
        .collect();
    types.add_struct(fields)
}

pub trait PostgresTypeExtractor {
    fn extract_type(&self, types: &mut PostgresTypes) -> PostgresType;
    fn extract_type_string(&self, types: &mut PostgresTypes) -> String {
        self.extract_type(types).to_string()
    }
}

impl PostgresTypeExtractor for TypeDef {
    fn extract_type(&self, types: &mut PostgresTypes) -> PostgresType {
        match self {
            TypeDef::None => PostgresType::Text,
            TypeDef::Bool => PostgresType::Boolean,
            TypeDef::U8 | TypeDef::I8 | TypeDef::I16 => PostgresType::SmallInt,
            TypeDef::U16 | TypeDef::I32 => PostgresType::Int,
            TypeDef::U32 | TypeDef::I64 => PostgresType::BigInt,
            TypeDef::U64 => PostgresType::Uint64,
            TypeDef::U128 => PostgresType::Uint128,
            TypeDef::I128 => PostgresType::Int128,
            TypeDef::U256 => PostgresType::Uint256,
            TypeDef::Felt252 => PostgresType::Felt252,
            TypeDef::ContractAddress | TypeDef::ClassHash => PostgresType::StarknetHash,
            TypeDef::EthAddress => PostgresType::EthAddress,
            TypeDef::ByteArray => PostgresType::Text,
            TypeDef::ShortString => PostgresType::Varchar(31),
            TypeDef::Tuple(type_defs) => {
                PostgresType::Composite(extract_types_from_tuple(type_defs, types))
            }
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
    fn extract_type(&self, types: &mut PostgresTypes) -> PostgresType {
        PostgresType::Array(
            Box::new(self.type_def.extract_type(types)),
            Some(self.size as u32),
        )
    }
}

impl PostgresTypeExtractor for StructDef {
    fn extract_type(&self, types: &mut PostgresTypes) -> PostgresType {
        let fields = self
            .fields
            .iter()
            .map(|f| PostgresField::new(f.name.clone(), f.type_def.extract_type(types)))
            .collect();
        PostgresType::Composite(types.add_struct(fields))
    }
}

impl PostgresTypeExtractor for EnumDef {
    fn extract_type(&self, types: &mut PostgresTypes) -> PostgresType {
        let variant_strings = self
            .variants
            .values()
            .map(|v| v.name.clone())
            .collect::<Vec<_>>();
        let mut fields = vec![PostgresField::new(
            "variant".to_string(),
            PostgresType::Composite(types.add_enum(variant_strings)),
        )];
        for variant in self.variants.values() {
            if variant.type_def != TypeDef::None {
                fields.push(PostgresField::new(
                    format!("_{}", variant.name),
                    variant.type_def.extract_type(types),
                ));
            }
        }
        PostgresType::Composite(types.add_struct(fields))
    }
}
