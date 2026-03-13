use crate::{
    processor::PgSchema,
    utils::{AsBytes, HasherExt},
};
use introspect_types::{
    ArrayDef, ColumnDef, EnumDef, FixedArrayDef, MemberDef, OptionDef, PrimaryDef, PrimaryTypeDef,
    StructDef, TupleDef, TypeDef, VariantDef,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use std::fmt::{self, Display};
use thiserror::Error;
use torii_introspect::schema::TableInfo;
use xxhash_rust::xxh3::Xxh3;

#[derive(Debug, Error)]
pub enum PgTypeError {
    #[error("Unsupported type for {0}")]
    UnsupportedType(String),
    #[error("Nested arrays are not supported")]
    NestedArrays,
}

pub type PgTypeResult<T> = std::result::Result<T, PgTypeError>;

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
pub enum PostgresType {
    Scalar(PostgresScalar),
    Array(PostgresScalar, Option<Vec<u32>>),
}

impl PostgresType {
    pub fn is_composite(&self) -> bool {
        match self {
            PostgresType::Scalar(PostgresScalar::Composite(_))
            | PostgresType::Array(PostgresScalar::Composite(_), _) => true,
            _ => false,
        }
    }
    pub fn to_array(self, size: Option<Vec<u32>>) -> PgTypeResult<Self> {
        match self {
            PostgresType::Scalar(scalar) => Ok(PostgresType::Array(scalar, size)),
            PostgresType::Array(_, _) => Err(PgTypeError::NestedArrays),
        }
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
pub enum PostgresScalar {
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
    Bytes31,
    Bytea,
    Composite(String),
}

impl From<PostgresScalar> for PostgresType {
    fn from(value: PostgresScalar) -> Self {
        Self::Scalar(value)
    }
}

impl From<String> for PostgresType {
    fn from(value: String) -> Self {
        Self::Scalar(PostgresScalar::Composite(value))
    }
}

impl Display for PostgresScalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresScalar::None => f.write_str("VOID"),
            PostgresScalar::Boolean => f.write_str("BOOLEAN"),
            PostgresScalar::SmallInt => f.write_str("SMALLINT"),
            PostgresScalar::Int => f.write_str("INTEGER"),
            PostgresScalar::BigInt => f.write_str("BIGINT"),
            PostgresScalar::Int128 => f.write_str("int128"),
            PostgresScalar::Uint8 => f.write_str("uint8"),
            PostgresScalar::Uint16 => f.write_str("uint16"),
            PostgresScalar::Uint32 => f.write_str("uint32"),
            PostgresScalar::Uint64 => f.write_str("uint64"),
            PostgresScalar::Uint128 => f.write_str("uint128"),
            PostgresScalar::Uint256 => f.write_str("uint256"),
            PostgresScalar::Uint512 => f.write_str("uint512"),
            PostgresScalar::Felt252 => f.write_str("felt252"),
            PostgresScalar::Char31 => f.write_str("char31"),
            PostgresScalar::Bytes31 => f.write_str("byte31"),
            PostgresScalar::StarknetHash => f.write_str("starknet_hash"),
            PostgresScalar::EthAddress => f.write_str("eth_address"),
            PostgresScalar::Text => f.write_str("TEXT"),
            PostgresScalar::Bytea => f.write_str("BYTEA"),
            PostgresScalar::Composite(name) => f.write_str(name),
        }
    }
}

impl Display for PostgresType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresType::Scalar(scalar) => scalar.fmt(f),
            PostgresType::Array(inner, None) => write!(f, "{}[]", inner),
            PostgresType::Array(inner, Some(sizes)) => {
                write!(f, "{inner}")?;
                sizes
                    .iter()
                    .rev()
                    .try_for_each(|size| write!(f, "[{size}]"))?;
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub struct PrimaryKey {
    pub name: String,
    pub pg_type: PostgresScalar,
}

impl Display for PrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, r#""{}" {} PRIMARY KEY"#, self.name, self.pg_type)
    }
}
#[derive(Debug)]
pub struct CreatePgTable {
    pub name: String,
    pub primary: PrimaryKey,
    pub columns: Vec<PostgresField>,
    pub pg_types: Vec<CreatesType>,
}
#[derive(Debug)]
pub struct PgStruct {
    pub name: String,
    pub fields: Vec<PostgresField>,
}

#[derive(Debug)]
pub struct PgEnum {
    pub name: String,
    pub variants: Vec<String>,
}

#[derive(Debug)]
pub enum CreatesType {
    Struct(PgStruct),
    Enum(PgEnum),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PostgresField {
    pub name: String,
    pub pg_type: PostgresType,
}

impl From<PostgresField> for (String, PostgresType) {
    fn from(val: PostgresField) -> Self {
        (val.name, val.pg_type)
    }
}

pub trait PostgresTypeExtractor {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> PgTypeResult<PostgresType>;
    fn extract_field(
        &self,
        branch: &Xxh3,
        name: String,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> PgTypeResult<PostgresField> {
        let pg_type = self.extract_type(&branch.branch(&name), creates)?;
        Ok(PostgresField::new(name, pg_type))
    }
}

pub trait PostgresFieldExtractor {
    type Id: AsBytes;
    fn name(&self) -> &str;
    fn type_def(&self) -> &TypeDef;
    fn id(&self) -> &Self::Id;
    fn branch(&self, branch: &Xxh3) -> Xxh3 {
        branch.branch(self.id())
    }
    fn extract_field(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> PgTypeResult<PostgresField> {
        Ok(PostgresField::new(
            self.name().to_string(),
            self.type_def()
                .extract_type(&self.branch(branch), creates)?,
        ))
    }
}

impl PostgresField {
    pub fn new(name: String, pg_type: PostgresType) -> Self {
        Self { name, pg_type }
    }

    pub fn new_composite(name: &str, type_name: String) -> Self {
        Self {
            name: name.to_string(),
            pg_type: PostgresType::Scalar(PostgresScalar::Composite(type_name)),
        }
    }

    fn write_qualified(&self, schema: &PgSchema, f: &mut impl fmt::Write) -> fmt::Result {
        write!(f, r#""{}" "#, self.name)?;
        if self.pg_type.is_composite() {
            write!(f, "{schema}.",)?;
        }
        write!(f, "{}", self.pg_type)
    }
}

impl PostgresFieldExtractor for ColumnDef {
    type Id = Felt;
    fn name(&self) -> &str {
        &self.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
    fn id(&self) -> &Self::Id {
        &self.id
    }
}

impl PostgresFieldExtractor for MemberDef {
    type Id = String;
    fn name(&self) -> &str {
        &self.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
    fn id(&self) -> &Self::Id {
        &self.name
    }
}

impl PostgresFieldExtractor for (&Felt, &VariantDef) {
    type Id = Felt;
    fn name(&self) -> &str {
        &self.1.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.1.type_def
    }
    fn id(&self) -> &Self::Id {
        &self.0
    }
}

impl CreatesType {
    fn new_struct(name: String, fields: Vec<PostgresField>) -> Self {
        Self::Struct(PgStruct { name, fields })
    }

    fn new_enum(name: String, variants: Vec<String>) -> Self {
        Self::Enum(PgEnum { name, variants })
    }
}

impl PostgresTypeExtractor for TypeDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> PgTypeResult<PostgresType> {
        match self {
            TypeDef::None => Ok(PostgresScalar::None.into()),
            TypeDef::Bool => Ok(PostgresScalar::Boolean.into()),
            TypeDef::I8 | TypeDef::I16 => Ok(PostgresScalar::SmallInt.into()),
            TypeDef::I32 => Ok(PostgresScalar::Int.into()),
            TypeDef::I64 => Ok(PostgresScalar::BigInt.into()),
            TypeDef::U8 => Ok(PostgresScalar::Uint8.into()),
            TypeDef::U16 => Ok(PostgresScalar::Uint16.into()),
            TypeDef::U32 => Ok(PostgresScalar::Uint32.into()),
            TypeDef::U64 => Ok(PostgresScalar::Uint64.into()),
            TypeDef::U128 => Ok(PostgresScalar::Uint128.into()),
            TypeDef::I128 => Ok(PostgresScalar::Int128.into()),
            TypeDef::U256 => Ok(PostgresScalar::Uint256.into()),
            TypeDef::U512 => Ok(PostgresScalar::Uint512.into()),
            TypeDef::Felt252 => Ok(PostgresScalar::Felt252.into()),
            TypeDef::ContractAddress
            | TypeDef::ClassHash
            | TypeDef::StorageAddress
            | TypeDef::StorageBaseAddress => Ok(PostgresScalar::StarknetHash.into()),
            TypeDef::EthAddress => Ok(PostgresScalar::EthAddress.into()),
            TypeDef::Utf8String => Ok(PostgresScalar::Text.into()),
            TypeDef::ShortUtf8 => Ok(PostgresScalar::Char31.into()),
            TypeDef::ByteArray | TypeDef::ByteArrayEncoded(_) => Ok(PostgresScalar::Bytea.into()),
            TypeDef::Bytes31 | TypeDef::Bytes31Encoded(_) => Ok(PostgresScalar::Bytes31.into()),
            TypeDef::Tuple(type_defs) => type_defs.extract_type(branch, creates),
            TypeDef::Enum(enum_def) => enum_def.extract_type(branch, creates),
            TypeDef::Array(array_def) => array_def.extract_type(branch, creates),
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

impl From<&PrimaryDef> for PrimaryKey {
    fn from(primary: &PrimaryDef) -> Self {
        let pg_type = match primary.type_def {
            PrimaryTypeDef::Bool => PostgresScalar::Boolean,
            PrimaryTypeDef::I8 | PrimaryTypeDef::I16 => PostgresScalar::SmallInt,
            PrimaryTypeDef::I32 => PostgresScalar::Int,
            PrimaryTypeDef::I64 => PostgresScalar::BigInt,
            PrimaryTypeDef::U8 => PostgresScalar::Uint8,
            PrimaryTypeDef::U16 => PostgresScalar::Uint16,
            PrimaryTypeDef::U32 => PostgresScalar::Uint32,
            PrimaryTypeDef::U64 => PostgresScalar::Uint64,
            PrimaryTypeDef::U128 => PostgresScalar::Uint128,
            PrimaryTypeDef::I128 => PostgresScalar::Int128,
            PrimaryTypeDef::Felt252 => PostgresScalar::Felt252,
            PrimaryTypeDef::ContractAddress
            | PrimaryTypeDef::ClassHash
            | PrimaryTypeDef::StorageAddress
            | PrimaryTypeDef::StorageBaseAddress => PostgresScalar::StarknetHash,
            PrimaryTypeDef::EthAddress => PostgresScalar::EthAddress,
            PrimaryTypeDef::ShortUtf8 => PostgresScalar::Char31,
            PrimaryTypeDef::Bytes31 | PrimaryTypeDef::Bytes31Encoded(_) => PostgresScalar::Bytes31,
        };
        PrimaryKey {
            name: primary.name.clone(),
            pg_type,
        }
    }
}

impl PostgresTypeExtractor for ArrayDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> PgTypeResult<PostgresType> {
        match self.type_def.extract_type(branch, creates)? {
            PostgresType::Scalar(primitive) => Ok(PostgresType::Array(primitive, None)),
            PostgresType::Array(_, _) => Err(PgTypeError::NestedArrays),
        }
    }
}

impl PostgresTypeExtractor for FixedArrayDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> PgTypeResult<PostgresType> {
        match self.type_def.extract_type(branch, creates)? {
            PostgresType::Scalar(primitive) => {
                Ok(PostgresType::Array(primitive, Some(vec![self.size])))
            }
            PostgresType::Array(_, _) => Err(PgTypeError::NestedArrays),
        }
    }
}

impl PostgresTypeExtractor for StructDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> PgTypeResult<PostgresType> {
        let members = self
            .members
            .iter()
            .map(|f| f.extract_field(branch, creates))
            .collect::<PgTypeResult<Vec<_>>>()?;
        let name = branch.type_name(&self.name);
        creates.push(CreatesType::new_struct(name.clone(), members).into());
        Ok(name.into())
    }
}

impl PostgresTypeExtractor for EnumDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> PgTypeResult<PostgresType> {
        let name = branch.type_name(&self.name);
        let variants_type = branch.type_name(&format!("v_{}", self.name));
        let variant_names = self.variants.values().map(|v| v.name.clone()).collect_vec();
        creates.push(CreatesType::new_enum(variants_type.clone(), variant_names).into());
        let mut fields = Vec::with_capacity(self.variants.len() + 1);
        fields.push(PostgresField::new_composite("_variant", variants_type));
        for variant in &self.variants {
            fields.push(variant.extract_field(branch, creates)?);
        }
        creates.push(CreatesType::new_struct(name.clone(), fields).into());
        Ok(name.into())
    }
}

impl PostgresTypeExtractor for TupleDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> PgTypeResult<PostgresType> {
        let variants = self
            .iter()
            .enumerate()
            .map(|(i, type_def)| type_def.extract_field(branch, format!("_{i}"), creates))
            .collect::<PgTypeResult<Vec<_>>>()?;
        let name = branch.type_name("tuple");
        creates.push(CreatesType::new_struct(name.clone(), variants).into());
        Ok(name.into())
    }
}

impl PostgresTypeExtractor for OptionDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> PgTypeResult<PostgresType> {
        self.type_def.extract_type(branch, creates)
    }
}

impl CreatePgTable {
    pub fn new(id: &Felt, table: &TableInfo) -> PgTypeResult<Self> {
        let TableInfo {
            name,
            attributes: _,
            primary,
            columns,
        } = table;
        let mut creates = Vec::new();
        let branch = Xxh3::new_based(id);
        let primary = primary.into();
        let columns = columns
            .iter()
            .map(|col| col.extract_field(&branch, &mut creates))
            .collect::<PgTypeResult<Vec<_>>>()?;
        Ok(Self {
            name: name.to_string(),
            primary,
            columns,
            pg_types: creates,
        })
    }
    pub fn make_queries(&self, schema: &PgSchema, queries: &mut Vec<String>) -> PgTypeResult<()> {
        for pg_type in &self.pg_types {
            queries.push(pg_type.create_query(schema));
        }
        queries.push(self.create_query(schema));
        Ok(())
    }
}

pub trait CreateQuery {
    fn create_query(&self, schema: &PgSchema) -> String;
}

impl CreateQuery for CreatesType {
    fn create_query(&self, schema: &PgSchema) -> String {
        match self {
            CreatesType::Struct(s) => s.create_query(schema),
            CreatesType::Enum(e) => e.create_query(schema),
        }
    }
}

impl CreateQuery for CreatePgTable {
    fn create_query(&self, schema: &PgSchema) -> String {
        let CreatePgTable {
            name,
            columns,
            primary,
            ..
        } = self;
        let mut string = format!(r#"CREATE TABLE IF NOT EXISTS "{schema}"."{name}" ({primary}"#,);
        for column in columns {
            string.push_str(",\n");
            column.write_qualified(schema, &mut string).unwrap();
        }
        string + ");"
    }
}

impl CreateQuery for PgStruct {
    fn create_query(&self, schema: &PgSchema) -> String {
        let mut string = format!(
            r#"
        DO $$
        BEGIN
            CREATE TYPE "{schema}"."{}" AS (
        "#,
            self.name
        );
        if let Some((last, batch)) = self.fields.split_last() {
            for field in batch {
                field.write_qualified(schema, &mut string).unwrap();
                string.push_str(", ");
            }
            last.write_qualified(schema, &mut string).unwrap();
        }
        string.push_str(
            r#"
            );
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;"#,
        );
        string
    }
}

impl CreateQuery for PgEnum {
    fn create_query(&self, schema: &PgSchema) -> String {
        let variants = self
            .variants
            .iter()
            .map(|v| format!(r#"'{}'"#, v))
            .join(", ");
        format!(
            r#"DO $$
            BEGIN
                CREATE TYPE "{schema}"."{}" AS ENUM ({variants});
            EXCEPTION
                WHEN duplicate_object THEN NULL;
            END $$;"#,
            self.name
        )
    }
}
