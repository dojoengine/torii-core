use crate::{
    types::{PgRustEnum, PostgresField, PostgresFieldExtractor, PostgresTypeExtractor},
    PostgresComplexType, PostgresType,
};
use introspect_types::{ColumnDef, EnumDef, StructDef, TypeDef, TypeName};
use serde::de::VariantAccess;
use sqlx::{postgres::types, Type};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, hash::Hash, sync::RwLock, vec};
use thiserror::Error;
pub struct SchemaManager(pub HashMap<String, RwLock<HashMap<String, PostgresType>>>);

#[derive(Debug, Error)]
pub enum UpgradeError {
    #[error("Cannot Alter column {0} from {1} to {2}")]
    TypeUpgradeError(String, String, String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Type {0} not found")]
    TypeNotFound(String),
    #[error("Struct {0} not found")]
    StructNotFound(String),
    #[error("Not a struct type: {0}")]
    NotStructType(String),
    #[error("Failed to acquire write lock")]
    LockError(#[]),
}

pub type Result<T> = std::result::Result<T, UpgradeError>;

struct TableSchema {
    columns: HashMap<String, (ColumnDef, PostgresType)>,
    structs: HashMap<String, RwLock<HashMap<String, PostgresType>>>,
    tuples: HashMap<String, Vec<PostgresType>>,
    enums: HashMap<String, PgRustEnum>,
    order: Vec<String>,
}

pub struct PgStructDef {
    pub name: String,
    pub fields: HashMap<String, PostgresType>,
}

pub struct PgEnumDef {
    pub name: String,
    pub variants: HashMap<String, PostgresType>,
    pub order: Vec<String>,
    pub variants_type_name: String,
}

impl PgStructDef {
    pub fn new(name: String, fields: &[PostgresField]) -> Self {
        Self {
            name,
            fields: fields
                .iter()
                .map(|f| (f.name.clone(), f.pg_type.clone()))
                .collect(),
        }
    }
}

enum TableOrType {
    Table(String, Vec<Alter>),
    Type(String, Vec<Alter>),
    Enum(String, Vec<String>),
}
enum Alter {
    Add(PostgresField),
    Modify(PostgresField),
}

fn add_column_query(name: &str, pg_type: &PostgresType) -> String {
    format!(r#"ADD COLUMN "{name}" {}"#, pg_type.to_string())
}

fn modify_column_query(name: &str, pg_type: &PostgresType) -> String {
    format!(r#"ALTER COLUMN "{name}" TYPE {}"#, pg_type.to_string())
}

fn add_member_query(type_name: &str, member_name: &str, pg_type: &PostgresType) -> String {
    format!(
        r#"ALTER TYPE "{type_name}" ADD ATTRIBUTE "{member_name}" {};"#,
        pg_type.to_string()
    )
}

fn modify_member_query(type_name: &str, member_name: &str, pg_type: &PostgresType) -> String {
    format!(
        r#"ALTER TYPE "{type_name}" ALTER ATTRIBUTE "{member_name}" TYPE {};"#,
        pg_type.to_string()
    )
}

impl Alter {
    fn to_query(&self, name: &str) -> String {
        match self {
            Alter::Add(field) => format!(
                r#"ADD {name} "{}" {}"#,
                field.name,
                field.pg_type.to_string()
            ),
            Alter::Modify(field) => format!(
                r#"ALTER {name} "{}" TYPE {}"#,
                field.name,
                field.pg_type.to_string()
            ),
        }
    }
}

impl TableOrType {
    fn to_query(&self) -> Vec<String> {
        match self {
            TableOrType::Table(name, alters) => vec![format!(
                r#"ALTER TABLE "{name}" {}"#,
                alters
                    .iter()
                    .map(|a| a.to_query("COLUMN"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )],
            TableOrType::Type(name, alters) => alters
                .iter()
                .map(|a| format!(r#"ALTER TYPE {name} {}"#, a.to_query("ATTRIBUTE")))
                .collect::<Vec<String>>(),
            TableOrType::Enum(name, variants) => variants
                .iter()
                .map(|v| format!("ALTER TYPE {name} ADD VALUE '{v}'"))
                .collect::<Vec<String>>(),
        }
    }
}

impl TableSchema {
    // fn upgrade_schema(&mut self, name: &str, new_schema: Vec<ColumnDef>) -> Result<Vec<String>> {
    //     let mut cmds = vec![];
    //     let mut new_types = vec![];
    //     let mut field_cmds = vec![];
    //     for column in new {
    //         match old.columns.get(&column.name) {
    //             Some((old_def, old_type)) => {
    //                 self.compare_types(name, column.name, old_def, old_type, new_types);
    //             }
    //             None => {
    //                 cmds.push(Alter::Add(column.extract_field(&mut new_types)));
    //             }
    //         }
    //     }
    //     Ok(vec![])
    // }

    fn add_types(&mut self, new_types: Vec<(String, PostgresComplexType)>) -> Vec<String> {
        let mut queries = Vec::new();
        for (name, ty) in new_types {
            match ty {
                PostgresComplexType::Struct(def) => {
                    self.compound.insert(name, def);
                }
                PostgresComplexType::Enum(variants) => {
                    self.enums.insert(name, variants);
                }
            }
        }
        queries
    }

    fn add_struct_member(&mut self, name: &str, member: &str, pg_type: &PostgresType) -> String {
        let mut members = self
            .structs
            .get_mut(name)
            .ok_or(UpgradeError::StructNotFound(name.to_string()))?;
        members.insert(member.to_string(), pg_type.clone());
        add_member_query(name, member, pg_type)
    }

    fn update_struct_member(&mut self, name: &str, member: &str, pg_type: &PostgresType) -> String {
        let mut members = self
            .structs
            .get_mut(name)
            .ok_or(UpgradeError::StructNotFound(name.to_string()))?;
        members.insert(member.to_string(), pg_type.clone());
        modify_member_query(name, member, pg_type)
    }

    fn compare_type(
        &mut self,
        name: &str,
        old: &PostgresType,
        new: &TypeDef,
        queries: &mut Vec<String>,
    ) -> Result<Option<PostgresType>> {
        use PostgresType::{BigInt, Boolean, Felt252, Int, Int128, SmallInt, Uint128, Uint64};
        match (old, new) {
            (Boolean | SmallInt | Int | BigInt | Uint64 | Int128 | Uint128 | Felt252, _) => {
                upgrade_primitive_type(name, old, new)
            }

            (PostgresType::Struct(struct_name), TypeDef::Struct(new_def)) => {
                self.upgrade_struct(struct_name, new_def, queries);
                Ok(None)
            }
            (PostgresType::RustEnum(enum_name), TypeDef::Enum(new_def)) => {
                // Enums cannot be modified except by adding new variants
                Ok(None)
            }
            (PostgresType::Tuple(tuple_name), TypeDef::Tuple(new_def)) => {
                // Tuples cannot be modified except by adding new variants
                Ok(None)
            }
            (PostgresType::Array(elem_type, _), TypeDef::Array(new_elem_type)) => {}
            _ => new_upgrade_error(name, old, new),
        }
    }

    fn modify_member(
        &mut self,
        struct_name: &str,
        member_name: &str,
        old: &mut PostgresType,
        new: &TypeDef,
        queries: &mut Vec<String>,
    ) -> Result<()> {
        match self.compare_type(&member_name, old, new, queries) {
            Ok(Some(t)) => {
                let mut members = self
                    .structs
                    .get_mut(struct_name)
                    .ok_or(UpgradeError::StructNotFound(struct_name.to_string()))?;
                queries.push(modify_member_query(struct_name, member_name, &t));
                *old = t;

                Ok(())
            }
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn add_member(
        &mut self,
        struct_name: &str,
        member_name: &str,
        new_type: &TypeDef,
        queries: &mut Vec<String>,
    ) -> Result<()> {
        // let pg_type = PostgresType::from_type_def(new_type, &mut vec![])?;
        // self.structs
        //     .get_mut(struct_name)
        //     .ok_or(UpgradeError::StructNotFound(struct_name.to_string()))?
        //     .insert(member_name.to_string(), pg_type.clone());
        // queries.push(self.add_struct_member(struct_name, member_name, &pg_type));
        // Ok(())
    }

    fn upgrade_struct(
        &mut self,
        name: &str,
        new: &StructDef,
        queries: &mut Vec<String>,
    ) -> Result<()> {
        let mut old_fields = self
            .structs
            .get(name)
            .ok_or(UpgradeError::StructNotFound(name.to_string()))?
            .write()?;

        for new_field in new.fields.iter() {
            match old_fields.get_mut(&new_field.name) {
                Some(mut old_field_type) => {
                    self.modify_member(
                        name,
                        &new_field.name,
                        &mut old_field_type,
                        &new_field.type_def,
                        queries,
                    )?;
                }
                None => {
                    self.add_member(name, &new_field.name, &new_field.type_def, queries)?;
                }
            }
        }
        Ok(())
    }

    fn upgrade_enum(&mut self, name: &str, new: &EnumDef, queries: &mut Vec<String>) -> Result<()> {
        let mut old_def = self
            .enums
            .get_mut(name)
            .ok_or(UpgradeError::StructNotFound(name.to_string()))?;
        for variant in new.variants.iter() {
            match old_def.variants.get(&variant.name) {
                Some(_) => {}
            }
        }
    }
}

// trait CompareTypes<T> {
//     type PgTypeDef;
//     fn compare_types(
//         &mut self,
//         table: &str,
//         field: &str,
//         old: &Self::PgTypeDef,
//         new: &T,
//         queries: &mut Vec<String>,
//     ) -> Result<Option<PostgresType>>;
// }

// impl CompareTypes<TypeDef> for TableSchema {
//     type PgTypeDef = PostgresType;
//     fn compare_types(
//         &mut self,
//         table: &str,
//         field: &str,
//         old: &PostgresType,
//         new: &TypeDef,
//         queries: &mut Vec<String>,
//     ) -> Result<Option<PostgresType>> {
//         use PostgresType::{BigInt, Boolean, Felt252, Int, Int128, SmallInt, Uint128, Uint64};
//         match old {
//             Boolean | SmallInt | Int | BigInt | Uint64 | Int128 | Uint128 | Felt252 => {
//                 upgrade_primitive_type(table, field, old, new)
//             }

//             PostgresType::Struct(pg_type_name) => match new {
//                 TypeDef::Struct(new_def) => {
//                     self.compare_types(pg_type_name, field, pg_type_name, new_def, queries)
//                 }
//                 _ => new_upgrade_error(table, field, old, new),
//             },

//             _ => upgrade_primitive_type(table, field, old, new),
//         }
//     }
// }

// impl CompareTypes<StructDef> for TableSchema {
//     type PgTypeDef = PgStructDef;

//     fn compare_types(
//         &mut self,
//         table: &str,
//         field: &str,
//         old: &PgStructDef,
//         new: &StructDef,
//         queries: &mut Vec<String>,
//     ) -> Result<Option<PostgresType>> {
//         let mut new_types = vec![];
//         for new_field in new.fields.iter() {
//             match old.fields.get(&new_field.name) {
//                 Some(old_field_type) => {
//                     self.compare_types(
//                         &old.name,
//                         &new_field.name,
//                         old_field_type,
//                         &new_field.type_def,
//                         queries,
//                     )
//                     .map(|r| {
//                         r.map(|t| queries.push(modify_member_query(&old.name, &new_field.name, &t)))
//                     })?;
//                 }
//                 None => {
//                     queries.push(new.extract_type_string(&mut new_types));
//                 }
//             }
//         }

//         Ok(None)
//     }
// }

pub fn new_upgrade_error<T>(field: &str, old: &PostgresType, new: &TypeDef) -> Result<T> {
    Err(UpgradeError::TypeUpgradeError(
        field.to_string(),
        old.to_string(),
        new.type_name(),
    ))
}

pub fn upgrade_primitive_type(
    field: &str,
    old: &PostgresType,
    new: &TypeDef,
) -> Result<Option<PostgresType>> {
    use PostgresType::{
        BigInt, Boolean, EthAddress as PgEthAddress, Felt252 as PgFelt252, Int, Int128, SmallInt,
        StarknetHash, Uint128, Uint16, Uint32, Uint64, Uint8,
    };
    use TypeDef::{
        Bool, ClassHash, ContractAddress, EthAddress, Felt252, I128, I16, I32, I64, I8, U128, U16,
        U32, U64, U8,
    };
    match (old, new) {
        (Boolean, Bool)
        | (SmallInt, I8 | I16)
        | (Int, I32)
        | (BigInt, I64)
        | (Uint8, U8)
        | (Uint16, U16)
        | (Uint32, U32)
        | (Uint64, U64)
        | (Uint128, U128)
        | (Int128, I128)
        | (PgFelt252, Felt252)
        | (StarknetHash, ClassHash | ContractAddress)
        | (PgEthAddress, EthAddress) => Ok(None),
        (Boolean | Uint8, I8 | I16) => Ok(Some(SmallInt)),
        (Boolean | Uint8 | Uint16 | SmallInt, I32) => Ok(Some(Int)),
        (Boolean | Uint8 | Uint16 | SmallInt | Uint32 | Int, I64) => Ok(Some(BigInt)),
        (Boolean | Uint8 | Uint16 | SmallInt | Uint32 | Int | Uint64 | BigInt, I128) => {
            Ok(Some(Int128))
        }
        (Boolean, U8) => Ok(Some(Uint8)),
        (Boolean | Uint8, U16) => Ok(Some(Uint16)),
        (Boolean | Uint8 | Uint16, U32) => Ok(Some(Uint32)),
        (Boolean | Uint8 | Uint16 | Uint32, U64) => Ok(Some(Uint64)),
        (Boolean | Uint8 | Uint16 | Uint32 | Uint64, U128) => Ok(Some(Uint128)),
        (
            Boolean | Uint8 | Uint16 | Uint32 | Uint64 | Uint128 | StarknetHash | PgEthAddress,
            Felt252,
        ) => Ok(Some(PgFelt252)),
        _ => new_upgrade_error(field, old, new),
    }
}
