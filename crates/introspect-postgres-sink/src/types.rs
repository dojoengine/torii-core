use crate::sql::{
    create_enum_type_query, create_struct_type_query, create_table_query, create_tuple_type_query,
};
use crate::utils::HasherExt;
use introspect_types::{
    ColumnDef, EnumDef, FixedArrayDef, MemberDef, OptionDef, PrimaryDef, PrimaryTypeDef, StructDef,
    TupleDef, TypeDef, VariantDef,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use torii_introspect::CreateTable;
use xxhash_rust::xxh3::Xxh3;

#[derive(Default, Debug)]
pub struct PgTableStructure {
    columns: HashMap<String, PostgresType>,
    structs: HashMap<String, PgStructDef>,
    tuples: HashMap<String, Vec<PostgresType>>,
    enums: HashMap<String, PgRustEnum>,
}

#[derive(Debug, Error)]
pub enum PgTypeError {
    #[error("Type {0} not found")]
    TypeNotFound(String),
    #[error("Struct {0} not found")]
    StructNotFound(String),
    #[error("Not a struct type: {0}")]
    NotStructType(String),
    #[error("Unsupported type for {0}")]
    UnsupportedType(String),
}

pub type PgTypeResult<T> = std::result::Result<T, PgTypeError>;

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug, Default)]
pub enum PostgresType {
    #[default]
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
    Uint512,
    Felt252,
    StarknetHash,
    EthAddress,
    Numeric(u32, u32),
    Text,
    Char(u32),
    Varchar(u32),
    Char31,
    Json,
    JsonB,
    Array(Box<PostgresType>, Option<u32>),
    Struct(String),
    Tuple(String),
    Enum(String),
    RustEnum(String),
    Bytes31,
    Bytea,
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
            PostgresType::Uint512 => "uint512".to_string(),
            PostgresType::Felt252 => "bytea".to_string(),
            PostgresType::Char31 => "char31".to_string(),
            PostgresType::Bytes31 => "bytea".to_string(),
            PostgresType::StarknetHash => "bytea".to_string(),
            PostgresType::EthAddress => "bytea".to_string(),
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
            PostgresType::Bytea => "BYTEA".to_string(),
        }
    }
}

pub fn variant_member_name(variant: &str) -> String {
    format!("_{}", variant)
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct PgStructDef {
    pub fields: HashMap<String, PostgresType>,
    pub order: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct PgRustEnum {
    pub variants: HashMap<String, PostgresType>,
    pub order: Vec<String>,
    pub variants_type_name: String,
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct PostgresField {
    pub name: String,
    pub pg_type: PostgresType,
}

impl Into<(String, PostgresType)> for PostgresField {
    fn into(self) -> (String, PostgresType) {
        (self.name, self.pg_type)
    }
}

pub fn parse_variant_name(variant: &str) -> String {
    if variant == "Some(T)" {
        "Some".to_string()
    } else {
        variant.to_string()
    }
}

impl PgRustEnum {
    pub fn new(branch: &Xxh3, name: &str, variants: Vec<PostgresField>) -> (String, Self) {
        let struct_name = branch.type_name(name);
        let variants_type_name = branch.branch_to_type_name("variants", name);
        let order = variants
            .iter()
            .map(|f| parse_variant_name(&f.name))
            .collect();
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

pub trait PostgresTypeExtractor {
    fn extract_type(
        &self,
        schema: &mut PgTableStructure,
        branch: &Xxh3,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<PostgresType>;
    fn extract_type_string(
        &self,
        branch: &Xxh3,
        schema: &mut PgTableStructure,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<String> {
        self.extract_type(schema, branch, queries)
            .map(|ty| ty.to_string())
    }
}

pub trait PostgresFieldExtractor {
    fn name(&self) -> &str;
    fn type_def(&self) -> &TypeDef;
    fn extract_type(
        &self,
        schema: &mut PgTableStructure,
        branch: &Xxh3,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<PostgresType> {
        self.type_def()
            .extract_type(schema, &branch.branch(self.name()), queries)
    }
    fn extract_field(
        &self,
        schema: &mut PgTableStructure,
        branch: &Xxh3,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<PostgresField> {
        Ok(PostgresField::new(
            self.name().to_string(),
            self.extract_type(schema, branch, queries)?,
        ))
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

impl PostgresFieldExtractor for ColumnDef {
    fn name(&self) -> &str {
        &self.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
}

impl PostgresFieldExtractor for MemberDef {
    fn name(&self) -> &str {
        &self.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
}

impl PostgresFieldExtractor for VariantDef {
    fn name(&self) -> &str {
        &self.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
}

// impl PostgresFieldExtractor for PrimaryDef {
//     fn name(&self) -> &str {
//         &self.name
//     }

//     fn type_def(&self) -> &TypeDef {
//         &self.type_def
//     }
// }

impl PostgresTypeExtractor for TypeDef {
    fn extract_type(
        &self,
        schema: &mut PgTableStructure,
        branch: &Xxh3,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<PostgresType> {
        match self {
            TypeDef::None => Ok(PostgresType::None),
            TypeDef::Bool => Ok(PostgresType::Boolean),
            TypeDef::I8 | TypeDef::I16 => Ok(PostgresType::SmallInt),
            TypeDef::I32 => Ok(PostgresType::Int),
            TypeDef::I64 => Ok(PostgresType::BigInt),
            TypeDef::U8 => Ok(PostgresType::Uint8),
            TypeDef::U16 => Ok(PostgresType::Uint16),
            TypeDef::U32 => Ok(PostgresType::Uint32),
            TypeDef::U64 => Ok(PostgresType::Uint64),
            TypeDef::U128 => Ok(PostgresType::Uint128),
            TypeDef::I128 => Ok(PostgresType::Int128),
            TypeDef::U256 => Ok(PostgresType::Uint256),
            TypeDef::U512 => Ok(PostgresType::Uint512),
            TypeDef::Felt252 => Ok(PostgresType::Felt252),
            TypeDef::ContractAddress
            | TypeDef::ClassHash
            | TypeDef::StorageAddress
            | TypeDef::StorageBaseAddress => Ok(PostgresType::StarknetHash),
            TypeDef::EthAddress => Ok(PostgresType::EthAddress),
            TypeDef::Utf8String => Ok(PostgresType::Text),
            TypeDef::ShortUtf8 => Ok(PostgresType::Char31),
            TypeDef::ByteArray | TypeDef::ByteArrayEncoded(_) => Ok(PostgresType::Bytea),
            TypeDef::Bytes31 | TypeDef::Bytes31Encoded(_) => Ok(PostgresType::Bytes31),
            TypeDef::Tuple(type_defs) => type_defs.extract_type(schema, branch, queries),
            TypeDef::Enum(enum_def) => enum_def.extract_type(schema, branch, queries),
            TypeDef::Array(array_def) => Ok(PostgresType::Array(
                Box::new(array_def.extract_type(schema, branch, queries)?),
                None,
            )),
            TypeDef::FixedArray(fixed_array_def) => {
                fixed_array_def.extract_type(schema, branch, queries)
            }
            TypeDef::Struct(struct_def) => struct_def.extract_type(schema, branch, queries),
            TypeDef::Option(def) => def.type_def.extract_type(schema, branch, queries),
            TypeDef::Nullable(def) => def.type_def.extract_type(schema, branch, queries),
            TypeDef::Felt252Dict(_) | TypeDef::Result(_) | TypeDef::Ref(_) | TypeDef::Custom(_) => {
                Err(PgTypeError::UnsupportedType(format!("{self:?}")))
            }
        }
    }
}

impl PostgresTypeExtractor for PrimaryTypeDef {
    fn extract_type(
        &self,
        _schema: &mut PgTableStructure,
        _branch: &Xxh3,
        _queries: &mut Vec<String>,
    ) -> PgTypeResult<PostgresType> {
        match self {
            PrimaryTypeDef::Bool => Ok(PostgresType::Boolean),
            PrimaryTypeDef::I8 | PrimaryTypeDef::I16 => Ok(PostgresType::SmallInt),
            PrimaryTypeDef::I32 => Ok(PostgresType::Int),
            PrimaryTypeDef::I64 => Ok(PostgresType::BigInt),
            PrimaryTypeDef::U8 => Ok(PostgresType::Uint8),
            PrimaryTypeDef::U16 => Ok(PostgresType::Uint16),
            PrimaryTypeDef::U32 => Ok(PostgresType::Uint32),
            PrimaryTypeDef::U64 => Ok(PostgresType::Uint64),
            PrimaryTypeDef::U128 => Ok(PostgresType::Uint128),
            PrimaryTypeDef::I128 => Ok(PostgresType::Int128),
            PrimaryTypeDef::Felt252 => Ok(PostgresType::Felt252),
            PrimaryTypeDef::ContractAddress
            | PrimaryTypeDef::ClassHash
            | PrimaryTypeDef::StorageAddress
            | PrimaryTypeDef::StorageBaseAddress => Ok(PostgresType::StarknetHash),
            PrimaryTypeDef::EthAddress => Ok(PostgresType::EthAddress),
            PrimaryTypeDef::ShortUtf8 => Ok(PostgresType::Char31),
            PrimaryTypeDef::Bytes31 | PrimaryTypeDef::Bytes31Encoded(_) => {
                Ok(PostgresType::Bytes31)
            }
        }
    }
}

impl PostgresTypeExtractor for FixedArrayDef {
    fn extract_type(
        &self,
        schema: &mut PgTableStructure,
        branch: &Xxh3,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<PostgresType> {
        Ok(PostgresType::Array(
            Box::new(self.type_def.extract_type(schema, branch, queries)?),
            Some(self.size as u32),
        ))
    }
}

impl PostgresTypeExtractor for StructDef {
    fn extract_type(
        &self,
        schema: &mut PgTableStructure,
        branch: &Xxh3,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<PostgresType> {
        let members = self
            .members
            .iter()
            .map(|f| f.extract_field(schema, branch, queries))
            .collect::<PgTypeResult<Vec<_>>>()?;
        schema
            .add_struct(branch, &self.name, members, queries)
            .map(PostgresType::Struct)
    }
}

impl PostgresTypeExtractor for EnumDef {
    fn extract_type(
        &self,
        schema: &mut PgTableStructure,
        branch: &Xxh3,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<PostgresType> {
        let variants = self
            .order
            .iter()
            .map(|selector| {
                let variant = &self.variants[selector];
                variant.extract_field(schema, branch, queries)
            })
            .collect::<PgTypeResult<Vec<_>>>()?;
        schema
            .add_rust_enum(branch, &self.name, variants, queries)
            .map(PostgresType::RustEnum)
    }
}

impl PostgresTypeExtractor for TupleDef {
    fn extract_type(
        &self,
        schema: &mut PgTableStructure,
        branch: &Xxh3,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<PostgresType> {
        let variants = self
            .iter()
            .enumerate()
            .map(|(i, type_def)| {
                type_def.extract_type(schema, &branch.branch(&format!("_{i}")), queries)
            })
            .collect::<PgTypeResult<Vec<_>>>()?;
        schema
            .add_tuple(branch, variants, queries)
            .map(PostgresType::Tuple)
    }
}

impl PostgresTypeExtractor for OptionDef {
    fn extract_type(
        &self,
        schema: &mut PgTableStructure,
        branch: &Xxh3,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<PostgresType> {
        self.type_def.extract_type(schema, branch, queries)
    }
}

impl PgTableStructure {
    pub fn new(
        _schema: &Option<String>,
        name: &str,
        primary_key: &PrimaryDef,
        columns: &[ColumnDef],
        queries: &mut Vec<String>,
    ) -> PgTypeResult<Self> {
        let mut table = PgTableStructure::default();
        let branch = Xxh3::new_based(&name);
        let type_def = primary_key
            .type_def
            .extract_type(&mut table, &branch, queries)?;
        let mut column_queries = vec![format!(
            r#""{}" {} PRIMARY KEY"#,
            primary_key.name,
            type_def.to_string()
        )];
        table.columns.insert(primary_key.name.clone(), type_def);

        for col in columns.iter() {
            let type_def = col.extract_type(&mut table, &branch, queries)?;
            column_queries.push(format!(r#""{}" {}"#, col.name, type_def.to_string()));
            table.columns.insert(col.name.clone(), type_def);
        }
        queries.push(create_table_query(&name, &column_queries));
        Ok(table)
    }

    pub fn new_from_event(
        namespace: &Option<String>,
        event: &CreateTable,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<Self> {
        PgTableStructure::new(
            namespace,
            &event.name,
            &event.primary,
            &event.columns,
            queries,
        )
    }

    pub fn add_column(&mut self, name: &str, type_def: PostgresType) -> String {
        self.columns.insert(name.to_string(), type_def.clone());
        format!(r#"ADD COLUMN "{}" {}"#, name, type_def.to_string())
    }

    pub fn add_struct(
        &mut self,
        branch: &Xxh3,
        name: &str,
        fields: Vec<PostgresField>,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<String> {
        let struct_name = branch.type_name(name);
        queries.push(create_struct_type_query(&struct_name, &fields));
        self.structs
            .insert(struct_name.clone(), PgStructDef::new(fields));
        Ok(struct_name)
    }

    pub fn add_rust_enum(
        &mut self,
        branch: &Xxh3,
        name: &str,
        variants: Vec<PostgresField>,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<String> {
        let (struct_name, enum_def) = PgRustEnum::new(branch, name, variants);
        queries.push(create_enum_type_query(
            &enum_def.variants_type_name,
            &enum_def.order,
        ));
        queries.push(create_struct_type_query(
            &struct_name,
            &enum_def.all_fields(),
        ));
        self.enums.insert(struct_name.clone(), enum_def);
        Ok(struct_name)
    }

    pub fn add_tuple(
        &mut self,
        branch: &Xxh3,
        elements: Vec<PostgresType>,
        queries: &mut Vec<String>,
    ) -> PgTypeResult<String> {
        let name = branch.type_name("tuple");

        queries.push(create_tuple_type_query(&name, &elements));
        self.tuples.insert(name.clone(), elements);
        Ok(name)
    }
}
