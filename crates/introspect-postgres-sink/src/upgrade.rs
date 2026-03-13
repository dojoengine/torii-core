use std::collections::HashMap;

use introspect_types::{type_def::TypeName, ColumnDef, MemberDef, PrimaryDef, StructDef, TypeDef};
use starknet_types_core::felt::Felt;
use xxhash_rust::xxh3::Xxh3;

use crate::{
    processor::PgSchema,
    sql::{add_member_query, modify_member_query, rename_column_query, rename_table_query},
    table::{PgTable, PgTableError, TableResult},
    types::{PgTypeError, PgTypeResult, PostgresFieldExtractor, PostgresTypeExtractor},
    HasherExt, PgStructDef, PgTableStructure, PostgresField, PostgresType,
};

#[derive(Debug, thiserror::Error)]
pub enum UpgradeError {
    TypeUpgradeError {
        old: String,
        new: String,
    },
    StructNotFound(String),
    #[error(transparent)]
    TypeCreationError(#[from] PgTypeError),
    DefMismatch {
        item: &'static str,
        expected: String,
        found: String,
    },
    DefMissing {
        item: &'static str,
        name: String,
    },
    ItemMismatch {
        expected: &'static str,
        found: &'static str,
    },
    ColumnNotFound {
        table_id: Felt,
        name: String,
        column_id: Felt,
    },
}

pub type UpgradeResult<T> = Result<T, UpgradeError>;

impl UpgradeError {
    fn type_upgrade_err<T>(old: &PostgresType, new: &TypeDef) -> UpgradeResult<T> {
        Err(Self::TypeUpgradeError {
            old: format!("{old}"),
            new: new.type_name(),
        })
    }
    fn struct_not_found<T>(name: &str) -> UpgradeResult<T> {
        Err(Self::StructNotFound(name.to_string()))
    }
    fn item_mismatch<T>(expected: &'static str, found: &'static str) -> UpgradeResult<T> {
        Err(Self::ItemMismatch { expected, found })
    }
    fn def_mismatch<T>(item: &'static str, expected: String, found: String) -> UpgradeResult<T> {
        Err(Self::DefMismatch {
            item,
            expected,
            found,
        })
    }
    fn def_missing<T>(item: &'static str, name: String) -> UpgradeResult<T> {
        Err(Self::DefMissing { item, name })
    }
    fn column_not_found<T>(table_id: Felt, name: String, column_id: Felt) -> UpgradeResult<T> {
        Err(Self::ColumnNotFound {
            table_id,
            name,
            column_id,
        })
    }
}

impl PgTable {
    fn update(
        &mut self,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        queries: &mut Vec<String>,
    ) -> UpgradeResult<()> {
        if self.name != name {
            self.rename_table(name, queries);
        }
        Ok(())
    }
    fn rename_table(&mut self, name: &str, queries: &mut Vec<String>) {
        queries.push(rename_table_query(self.schema(), &self.name, name));
        self.name = name.to_string();
    }
    fn rename_column(
        &mut self,
        branch: &Xxh3,
        id: &Felt,
        name: &str,
        queries: &mut Vec<String>,
    ) -> UpgradeResult<()> {
        match self.columns.get_mut(id) {
            Some(column) => {
                queries.push(rename_column_query(
                    self.schema(),
                    &self.name,
                    &column.name,
                    name,
                ));
                column.name = name.to_string();
                Ok(())
            }
            None => UpgradeError::column_not_found(self.id, self.name.clone(), id.clone()),
        }
    }
    fn retype_column(
        &mut self,
        branch: &Xxh3,
        id: &Felt,
        type_def: &TypeDef,
        queries: &mut Vec<String>,
    ) -> UpgradeResult<()> {
        match self.columns.get_mut(id) {
            Some(column) => {
                let new_pg_type = self
                    .postgres
                    .compare_type(
                        branch,
                        &mut column.type_def,
                        &old_type.into(),
                        new_type,
                        queries,
                    )
                    .map_err(|e| PgTableError::TypeError(e))?;
                if let Some(new_pg_type) = new_pg_type {
                    column.type_def = new_type.clone();
                }
                Ok(())
            }
            None => Err(UpgradeError::def_missing("Column", id.to_string())),
        }
    }
}

trait CurrentTypeDef {
    fn update_fixed_array(&mut self, len: u32) -> UpgradeResult<&mut Self>;
    fn get_struct_def(&mut self) -> UpgradeResult<&mut StructDef>;
    fn get_array_type_def(&mut self) -> UpgradeResult<&mut TypeDef>;
    fn update_to(&mut self, new: PostgresType) -> Option<PostgresType>;
}

impl CurrentTypeDef for TypeDef {
    fn get_array_type_def(&mut self) -> UpgradeResult<&mut TypeDef> {
        match self {
            TypeDef::Array(elem_type) => Ok(elem_type),
            item => UpgradeError::item_mismatch("Array", item.item_name()),
        }
    }
    fn update_fixed_array(&mut self, len: u32) -> UpgradeResult<&mut Self> {
        match self {
            TypeDef::FixedArray(def) => {
                def.size = len;
                Ok(&mut def.type_def)
            }
            item => UpgradeError::item_mismatch("FixedArray", item.item_name()),
        }
    }
    fn get_struct_def(&mut self) -> UpgradeResult<&mut StructDef> {
        match self {
            TypeDef::Struct(def) => Ok(def),
            item => UpgradeError::item_mismatch("Struct", item.item_name()),
        }
    }
}

impl PgTableStructure {
    fn compare_type(
        &self,
        branch: &Xxh3,
        current: &mut TypeDef,
        old: &PostgresType,
        new: &TypeDef,
        queries: &mut Vec<String>,
    ) -> UpgradeResult<Option<PostgresType>> {
        fn with_false<T>(value: Option<PostgresType>) -> (Option<PostgresType>, bool) {
            (value, false)
        }
        use crate::PostgresType::{
            Array as PgArray, BigInt, Boolean, Bytea, Bytes31 as PgBytes31, Char31,
            EthAddress as PgEthAddress, Felt252 as PgFelt252, Int, Int128, None as PgNone,
            RustEnum as PgRustEnum, SmallInt, StarknetHash, Struct as PgStruct, Text,
            Tuple as PgTuple, Uint128, Uint16, Uint32, Uint64, Uint8,
        };
        use introspect_types::TypeDef::{
            Array, Bool, ByteArray, ByteArrayEncoded, Bytes31, Bytes31Encoded, ClassHash,
            ContractAddress, Enum, EthAddress, Felt252, FixedArray, None as TDNone, ShortUtf8,
            StorageAddress, StorageBaseAddress, Struct, Tuple, Utf8String, I128, I16, I32, I64, I8,
            U128, U16, U32, U64, U8,
        };
        let new_pg = match (current, new) {
            (TDNone, TDNone)
            | (Utf8String, Utf8String)
            | (ShortUtf8, ShortUtf8)
            | (Bool, Bool)
            | (I8, I8)
            | (I16, I16)
            | (I32, I32)
            | (I64, I64)
            | (U8, U8)
            | (U16, U16)
            | (U32, U32)
            | (U64, U64)
            | (U128, U128)
            | (I128, I128)
            | (Felt252, Felt252)
            | (EthAddress, EthAddress)
            | (ClassHash,ClassHash)
            | (ContractAddress,ContractAddress)
            | (StorageAddress,StorageAddress)
            | (StorageBaseAddress,StorageBaseAddress)
            | (Bytes31,Bytes31)
            | (ByteArray,ByteArray) => None,
            (I8,  I16)
            | (Bytes31, Bytes31Encoded(_))
            | (ByteArray, ByteArrayEncoded(_))
            | (
                ClassHash | ContractAddress | StorageAddress | StorageBaseAddress,
                ClassHash | ContractAddress | StorageAddress | StorageBaseAddress,
            ) => {
                *current = new.clone();
                None
            },
            (Bool | Uint8, I8 | I16) => (Some(SmallInt)),
            // (Boolean | Uint8 | Uint16 | SmallInt, I32) => (Some(Int), true),
            // (Boolean | Uint8 | Uint16 | SmallInt | Uint32 | Int, I64) => (Some(BigInt), true),
            // (Boolean | Uint8 | Uint16 | SmallInt | Uint32 | Int | Uint64 | BigInt, I128) => {
            //     (Some(Int128), true)
            // }
            // (Boolean, U8) => (Some(Uint8), true),
            // (Boolean | Uint8, U16) => (Some(Uint16), true),
            // (Boolean | Uint8 | Uint16, U32) => (Some(Uint32), true),
            // (Boolean | Uint8 | Uint16 | Uint32, U64) => (Some(Uint64), true),
            // (Boolean | Uint8 | Uint16 | Uint32 | Uint64, U128) => (Some(Uint128), true),
            // (
            //     Boolean | Uint8 | Uint16 | Uint32 | Uint64 | Uint128 | StarknetHash | PgEthAddress,
            //     Felt252,
            // ) => (Some(PgFelt252), true),
            // (
            //     Boolean | Uint8 | Uint16 | Uint32 | Uint64 | Uint128 | PgFelt252,
            //     ClassHash | ContractAddress,
            // ) => (Some(StarknetHash), true),
            // // (PgStruct(struct_name), Struct(new_def)) => {
            // //     self.upgrade_struct(branch, struct_name, new_def, queries)?;
            // //     (None, false)
            // // }
            // // (PgRustEnum(enum_name), Enum(new_def)) => {
            // //     self.upgrade_enum(branch, enum_name, new_def, queries)?;
            // //     (None, false)
            // // }
            // // (PgTuple(tuple_name), Tuple(new_def)) => {
            // //     self.upgrade_tuple(branch, tuple_name, new_def, queries)?;
            // //     (None, false)
            // // }
            // (PgArray(elem_type, None), Array(new_elem_type)) => with_false(
            //     self.compare_type(branch, name, elem_type, new_elem_type, queries)?
            //         .map(|t| PgArray(Box::new(t), None)),
            // ),
            // (PgArray(elem_type, Some(_)), Array(new_elem_type)) => with_false(
            //     self.compare_type(branch, name, elem_type, new_elem_type, queries)?
            //         .map(|t| PgArray(Box::new(t), None)),
            // ),
            // (PgArray(elem_type, Some(size)), FixedArray(new_def)) => {
            //     if new_def.size < *size {
            //         return Err(PgTypeError::type_upgrade_error(name, old, new));
            //     }
            //     let inner = current.update_fixed_array(new_def.size)?;
            //     with_false(
            //         self.compare_type(branch, inner, elem_type, &new_def.type_def, queries)?
            //             .map(|t| PgArray(Box::new(t), Some(new_def.size))),
            //     )
            // }
            // _ => return UpgradeError::type_upgrade_err(old, new),
        }
        Ok(new_pg)
    }

    fn upgrade_to_variable_array(
        &mut self,
        branch: &Xxh3,
        current: &mut TypeDef,
        old: &PostgresType,
        new: &TypeDef,
        queries: &mut Vec<String>,
    ) -> UpgradeResult<()> {
        match (old, new) {
            (PgArray(elem_type, Some(size)), Array(new_elem_type)) => {
                let inner = current.update_fixed_array(*size)?;
                self.compare_type(branch, inner, elem_type, new_elem_type, queries)?
                    .map(|t| PgArray(Box::new(t), None));
                Ok(())
            }
            _ => UpgradeError::type_upgrade_err(old, new),
        }
    }

    fn upgrade_struct(
        &mut self,
        branch: &Xxh3,
        type_name: &str,
        current: &mut StructDef,
        new: &StructDef,
        queries: &mut Vec<String>,
    ) -> UpgradeResult<()> {
        let pg_current = self
            .structs
            .get_mut(type_name)
            .ok_or_else(|| UpgradeError::struct_not_found(type_name))?;
        let mut current_map: HashMap<String, MemberDef> = std::mem::take(&mut current.members)
            .into_iter()
            .map(|m| (m.name.clone(), m))
            .collect();
        for member_def in new.members.iter() {
            let new_field = if let Some(pg_field) = pg_current.fields.get(&member_def.name) {
                if let Some(mut current_field) = current_map.remove(&member_def.name) {
                    let branch = branch.branch(&member_def.name);
                    if let Some(pg_type) = self.compare_type(
                        &branch,
                        &mut current_field.type_def,
                        pg_field,
                        &member_def.type_def,
                        queries,
                    )? {
                        queries.push(modify_member_query(
                            self.schema(),
                            type_name,
                            &member_def.name,
                            &pg_type,
                        ));
                        pg_current.fields.insert(name.clone(), pg_type);
                    }
                    current_field
                } else {
                    return UpgradeError::def_missing("Struct Member", name.clone());
                }
            } else {
                let pg_type = member_def.extract_type(self, &branch, queries)?;
                queries.push(add_member_query(
                    self.schema(),
                    type_name,
                    &member_def.name,
                    &pg_type,
                ));
                pg_current.add_member(&member_def.name, pg_type);
                member_def.clone()
            };
            current.members.push(new_field);
        }
        Ok(())
    }
}

impl PgStructDef {}
