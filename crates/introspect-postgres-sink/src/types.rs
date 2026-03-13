use crate::processor::PgSchema;
use crate::sql::{
    create_enum_type_query, create_struct_type_query, create_table_query, create_tuple_type_query,
};
use crate::utils::HasherExt;
use introspect_types::type_def::TypeName;
use introspect_types::{
    ColumnDef, EnumDef, FixedArrayDef, MemberDef, OptionDef, PrimaryDef, PrimaryTypeDef, StructDef,
    TupleDef, TypeDef, VariantDef,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Display};
use thiserror::Error;
use torii_introspect::CreateTable;
use xxhash_rust::xxh3::Xxh3;

#[derive(Debug)]
pub struct PgTableStructure {
    pub schema: PgSchema,
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
    #[error("Cannot Alter column {0} from {1} to {2}")]
    TypeUpgradeError(String, String, String),
}

impl PgTypeError {
    pub fn type_upgrade_error(column: &str, old: &PostgresType, new: &TypeDef) -> Self {
        Self::TypeUpgradeError(column.to_string(), old.to_string(), new.type_name())
    }
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
    Text,
    Char31,
    Array(Box<PostgresType>, Option<u32>),
    Bytes31,
    Bytea,
    Custom(String),
}

impl Display for PostgresType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresType::None => f.write_str("VOID"),
            PostgresType::Boolean => f.write_str("BOOLEAN"),
            PostgresType::SmallInt => f.write_str("SMALLINT"),
            PostgresType::Int => f.write_str("INTEGER"),
            PostgresType::BigInt => f.write_str("BIGINT"),
            PostgresType::Int128 => f.write_str("int128"),
            PostgresType::Uint8 => f.write_str("uint8"),
            PostgresType::Uint16 => f.write_str("uint16"),
            PostgresType::Uint32 => f.write_str("uint32"),
            PostgresType::Uint64 => f.write_str("uint64"),
            PostgresType::Uint128 => f.write_str("uint128"),
            PostgresType::Uint256 => f.write_str("uint256"),
            PostgresType::Uint512 => f.write_str("uint512"),
            PostgresType::Felt252 => f.write_str("felt252"),
            PostgresType::Char31 => f.write_str("char31"),
            PostgresType::Bytes31 => f.write_str("byte31"),
            PostgresType::StarknetHash => f.write_str("starknet_hash"),
            PostgresType::EthAddress => f.write_str("eth_address"),
            PostgresType::Text => f.write_str("TEXT"),
            PostgresType::Array(element_type, size) => match size {
                Some(s) => write!(f, "{element_type}[{s}]"),
                None => write!(f, "{element_type}[]"),
            },
            PostgresType::Custom(type_name) => f.write_str(type_name),
            PostgresType::Bytea => f.write_str("BYTEA"),
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

impl From<PostgresField> for (String, PostgresType) {
    fn from(val: PostgresField) -> Self {
        (val.name, val.pg_type)
    }
}

impl PgRustEnum {
    pub fn new(branch: &Xxh3, name: &str, variants: Vec<PostgresField>) -> (String, Self) {
        let variants_type_name = branch.branch_to_type_name("variants", name);
        let order = variants.iter().map(|f| f.name.clone()).collect();
        let variants_map = variants.into_iter().map(PostgresField::into).collect();
        (
            branch.type_name(name),
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

    pub fn all_fields(&self, schema: &PgSchema) -> Vec<PostgresField> {
        let mut fields = vec![PostgresField::new_enum(
            "variant".to_string(),
            &schema.qualify(&self.variants_type_name),
        )];
        fields.extend(self.variant_fields());
        fields
    }
}

pub trait PostgresTypeExtractor {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<PostgresType>;
    fn extract_type_string(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<PgCreates>,
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
        branch: &Xxh3,
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<PostgresType> {
        self.type_def()
            .extract_type(schema, &branch.branch(self.name()), queries)
    }
    fn extract_field(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<PgCreates>,
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

pub struct PgStruct {
    pub name: String,
    pub fields: Vec<PostgresField>,
}

pub enum PgCreates {
    Struct(PgStruct),
    Enum(Vec<String>),
}

impl PgCreates {
    fn new_struct(name: String, fields: Vec<PostgresField>) -> Self {
        Self::Struct(PgStruct { name, fields })
    }

    fn new_enum(variants: Vec<String>) -> Self {
        Self::Enum(variants)
    }
}

impl PostgresTypeExtractor for TypeDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<PgCreates>,
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
            TypeDef::Tuple(type_defs) => type_defs.extract_type(branch, creates),
            TypeDef::Enum(enum_def) => enum_def.extract_type(branch, creates),
            TypeDef::Array(array_def) => Ok(PostgresType::Array(
                Box::new(array_def.extract_type(branch, creates)?),
                None,
            )),
            TypeDef::FixedArray(fixed_array_def) => fixed_array_def.extract_type(branch, creates),
            TypeDef::Struct(struct_def) => struct_def.extract_type(branch, creates),
            TypeDef::Option(def) => def.type_def.extract_type(branch, creates),
            TypeDef::Nullable(def) => def.type_def.extract_type(branch, creates),
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
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<PostgresType> {
        Ok(PostgresType::Array(
            Box::new(self.type_def.extract_type(schema, branch, queries)?),
            Some(self.size),
        ))
    }
}

impl PostgresTypeExtractor for StructDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<PostgresType> {
        let members = self
            .members
            .iter()
            .map(|f| f.extract_field(branch, creates))
            .collect::<PgTypeResult<Vec<_>>>()?;

        creates.push(PgCreates::new_struct(self.name.clone(), members));
    }
}

impl PostgresTypeExtractor for EnumDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<PostgresType> {
        let variants = self
            .order
            .iter()
            .map(|selector| {
                let variant = &self.variants[selector];
                variant.extract_field(structure, branch, queries)
            })
            .collect::<PgTypeResult<Vec<_>>>()?;
        structure
            .add_rust_enum(branch, &self.name, variants, queries)
            .map(PostgresType::RustEnum)
    }
}

impl PostgresTypeExtractor for TupleDef {
    fn extract_type(
        &self,
        schema: &mut PgTableStructure,
        branch: &Xxh3,
        creates: &mut Vec<PgCreates>,
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
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<PostgresType> {
        self.type_def.extract_type(schema, branch, queries)
    }
}

impl PgTableStructure {
    pub fn new(
        schema: &PgSchema,
        name: &str,
        primary_key: &PrimaryDef,
        columns: &[ColumnDef],
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<Self> {
        let mut table = PgTableStructure::new_empty(&schema);
        let branch = Xxh3::new_based(name);
        let type_def = primary_key
            .type_def
            .extract_type(&mut table, &branch, queries)?;
        let mut column_queries = vec![format!(r#""{}" {type_def} PRIMARY KEY"#, primary_key.name)];
        table.columns.insert(primary_key.name.clone(), type_def);

        for col in columns.iter() {
            let type_def = col.extract_type(&mut table, &branch, queries)?;
            column_queries.push(format!("\"{}\" {type_def}", col.name));
            table.columns.insert(col.name.clone(), type_def);
        }
        queries.push(create_table_query(schema, name, &column_queries));
        Ok(table)
    }

    pub fn schema(&self) -> &PgSchema {
        &self.schema
    }
    pub fn new_empty(schema: &PgSchema) -> Self {
        Self {
            schema: schema.clone(),
            columns: HashMap::new(),
            structs: HashMap::new(),
            tuples: HashMap::new(),
            enums: HashMap::new(),
        }
    }
    pub fn new_from_event(
        schema: &PgSchema,
        event: &CreateTable,
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<Self> {
        PgTableStructure::new(schema, &event.name, &event.primary, &event.columns, queries)
    }

    pub fn add_column(&mut self, name: &str, type_def: PostgresType) -> String {
        self.columns.insert(name.to_string(), type_def.clone());
        format!("ADD COLUMN \"{name}\" {type_def}")
    }

    pub fn add_struct(
        &mut self,
        branch: &Xxh3,
        name: &str,
        fields: Vec<PostgresField>,
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<String> {
        let name = branch.type_name(name);
        queries.push(create_struct_type_query(&self.schema, &name, &fields));
        let qualified_name = self.schema.qualify(&name);
        self.structs
            .insert(qualified_name.clone(), PgStructDef::new(fields));
        Ok(qualified_name)
    }

    pub fn add_rust_enum(
        &mut self,
        branch: &Xxh3,
        name: &str,
        variants: Vec<PostgresField>,
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<String> {
        let (struct_name, enum_def) = PgRustEnum::new(branch, name, variants);
        queries.push(create_enum_type_query(
            &self.schema,
            &enum_def.variants_type_name,
            &enum_def.order,
        ));
        queries.push(create_struct_type_query(
            &self.schema,
            &struct_name,
            &enum_def.all_fields(&self.schema),
        ));
        let qualified_name = self.schema.qualify(&struct_name);
        self.enums.insert(qualified_name.clone(), enum_def);
        Ok(qualified_name)
    }

    pub fn add_tuple(
        &mut self,
        branch: &Xxh3,
        elements: Vec<PostgresType>,
        creates: &mut Vec<PgCreates>,
    ) -> PgTypeResult<String> {
        let name = branch.type_name("tuple");
        queries.push(create_tuple_type_query(&self.schema, &name, &elements));
        let qualified_name = self.schema.qualify(&name);
        self.tuples.insert(qualified_name.clone(), elements);
        Ok(qualified_name)
    }
}
