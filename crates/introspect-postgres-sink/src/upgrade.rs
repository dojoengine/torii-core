use crate::{
    processor::PgSchema,
    sql::{add_member_query, modify_member_query, rename_column_query, rename_table_query},
    table::{PgTable, PgTableError, TableResult},
    types::{
        CreatesType, PgTypeError, PgTypeResult, PostgresFieldExtractor, PostgresScalar,
        PostgresTypeExtractor,
    },
    HasherExt, PostgresField, PostgresType,
};
use introspect_types::{
    ArrayDef, ColumnDef, EnumDef, FixedArrayDef, MemberDef, PrimaryDef, ResultInto, StructDef, TupleDef, TypeDef, VariantDef, type_def::TypeName
};
use itertools::Itertools;
use starknet::core::types::U256;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use xxhash_rust::xxh3::Xxh3;

#[derive(Debug, thiserror::Error)]
pub enum UpgradeError {
    #[error("Failed to upgrade type from {old} to {new}")]
    TypeUpgradeError {
        old: &'static str,
        new: &'static str,
    },
    #[error(transparent)]
    TypeCreationError(#[from] PgTypeError),
    #[error("Array length cannot be decreased from {old} to {new}")]
    ArrayLengthDecreaseError { old: u32, new: u32 },
}

pub type UpgradeResult<T> = Result<T, UpgradeError>;

pub enum StructMod {
    Add(PostgresField),
    Alter(PostgresField),
    Rename(String, String),
}

pub enum TypeMod {
    Struct(StructUpgrade),
    Enum(EnumUpgrade),
    Create(CreatesType),
}

impl From<CreatesType> for TypeMod {
    fn from(value: CreatesType) -> Self {
        TypeMod::Create(value)
    }
}

pub struct StructUpgrade {
    name: String,
    mods: Vec<StructMod>,
}

pub struct EnumUpgrade {
    name: String,
    rename: Vec<(String, String)>,
    add: Vec<String>,
}

impl UpgradeError {
    fn type_upgrade_err<T>(old: &TypeDef, new: &TypeDef) -> UpgradeResult<T> {
        Err(Self::TypeUpgradeError {
            old: old.item_name(),
            new: new.item_name(),
        })
    }
    fn type_upgrade_to_err<T>(old: &TypeDef, new: &'static str) -> UpgradeResult<T> {
        Err(Self::TypeUpgradeError {
            old: old.item_name(),
            new,
        })
    }
    fn array_shorten_err<T>(old: u32, new: u32) -> UpgradeResult<T> {
        Err(Self::ArrayLengthDecreaseError { old, new })
    }
}

// impl PgTable {
//     fn update(
//         &mut self,
//         name: &str,
//         primary: &PrimaryDef,
//         columns: &[ColumnDef],
//         queries: &mut Vec<String>,
//     ) -> UpgradeResult<()> {
//         if self.name != name {
//             self.rename_table(name, queries);
//         }
//         Ok(())
//     }
//     fn rename_table(&mut self, name: &str, queries: &mut Vec<String>) {
//         queries.push(rename_table_query(self.schema(), &self.name, name));
//         self.name = name.to_string();
//     }
//     fn rename_column(
//         &mut self,
//         branch: &Xxh3,
//         id: &Felt,
//         name: &str,
//         queries: &mut Vec<String>,
//     ) -> UpgradeResult<()> {
//         match self.columns.get_mut(id) {
//             Some(column) => {
//                 queries.push(rename_column_query(
//                     self.schema(),
//                     &self.name,
//                     &column.name,
//                     name,
//                 ));
//                 column.name = name.to_string();
//                 Ok(())
//             }
//             None => UpgradeError::column_not_found(self.id, self.name.clone(), id.clone()),
//         }
//     }
//     fn retype_column(
//         &mut self,
//         branch: &Xxh3,
//         id: &Felt,
//         type_def: &TypeDef,
//         queries: &mut Vec<String>,
//     ) -> UpgradeResult<()> {
//         match self.columns.get_mut(id) {
//             Some(column) => {
//                 let new_pg_type = self
//                     .postgres
//                     .compare_type(
//                         branch,
//                         &mut column.type_def,
//                         &old_type.into(),
//                         new_type,
//                         queries,
//                     )
//                     .map_err(|e| PgTableError::TypeError(e))?;
//                 if let Some(new_pg_type) = new_pg_type {
//                     column.type_def = new_type.clone();
//                 }
//                 Ok(())
//             }
//             None => Err(UpgradeError::def_missing("Column", id.to_string())),
//         }
//     }
// }

// trait CurrentTypeDef {
//     fn update_fixed_array(&mut self, len: u32) -> UpgradeResult<&mut Self>;
//     fn get_struct_def(&mut self) -> UpgradeResult<&mut StructDef>;
//     fn get_array_type_def(&mut self) -> UpgradeResult<&mut TypeDef>;
//     fn update_to(&mut self, new: PostgresType) -> Option<PostgresType>;
// }

// impl CurrentTypeDef for TypeDef {
//     fn get_array_type_def(&mut self) -> UpgradeResult<&mut TypeDef> {
//         match self {
//             TypeDef::Array(elem_type) => Ok(elem_type),
//             item => UpgradeError::item_mismatch("Array", item.item_name()),
//         }
//     }
//     fn update_fixed_array(&mut self, len: u32) -> UpgradeResult<&mut Self> {
//         match self {
//             TypeDef::FixedArray(def) => {
//                 def.size = len;
//                 Ok(&mut def.type_def)
//             }
//             item => UpgradeError::item_mismatch("FixedArray", item.item_name()),
//         }
//     }
//     fn get_struct_def(&mut self) -> UpgradeResult<&mut StructDef> {
//         match self {
//             TypeDef::Struct(def) => Ok(def),
//             item => UpgradeError::item_mismatch("Struct", item.item_name()),
//         }
//     }
// }

pub trait ExtractItem {
    fn as_struct(&mut self) -> UpgradeResult<&mut StructDef>;
    fn as_enum(&mut self) -> UpgradeResult<&mut EnumDef>;
    fn as_fixed_array(&mut self) -> UpgradeResult<&mut FixedArrayDef>;
    fn as_array(&mut self) -> UpgradeResult<&mut TypeDef>;
    fn as_tuple(&mut self) -> UpgradeResult<&mut TupleDef>;
    fn update_as_array(
        &mut self,
        branch: &Xxh3,
        new: &ArrayDef,
        dead: &mut HashMap<u128, MemberDef>,
        queries: &mut Vec<TypeMod>,
    ) -> UpgradeResult<Option<PostgresType>>;
}

impl ExtractItem for TypeDef {
    fn as_struct(&mut self) -> UpgradeResult<&mut StructDef> {
        match self {
            TypeDef::Struct(def) => Ok(def),
            item => UpgradeError::type_upgrade_to_err(item, "Struct"),
        }
    }
    fn as_enum(&mut self) -> UpgradeResult<&mut EnumDef> {
        match self {
            TypeDef::Enum(def) => Ok(def),
            item => UpgradeError::type_upgrade_to_err(item, "Enum"),
        }
    }
    fn as_fixed_array(&mut self) -> UpgradeResult<&mut FixedArrayDef> {
        match self {
            TypeDef::FixedArray(def) => Ok(def),
            item => UpgradeError::type_upgrade_to_err(item, "FixedArray"),
        }
    }
    fn as_array(&mut self) -> UpgradeResult<&mut TypeDef> {
        match self {
            TypeDef::Array(def) => Ok(&mut def.type_def),
            TypeDef::FixedArray(def) => Ok(&mut def.type_def),
            item => UpgradeError::type_upgrade_to_err(item, "Array"),
        }
    }
    fn as_tuple(&mut self) -> UpgradeResult<&mut TupleDef> {
        match self {
            TypeDef::Tuple(def) => Ok(def),
            item => UpgradeError::type_upgrade_to_err(item, "Tuple"),
        }
    }
    fn update_as_array(
        &mut self,
        branch: &Xxh3,
        new: &ArrayDef,
        dead: &mut HashMap<u128, MemberDef>,
        queries: &mut Vec<TypeMod>,
    ) -> UpgradeResult<Option<PostgresType>> {
        self.as_array()?
            .compare_type(branch, new, dead, queries)?
            .map(|a| a.to_array(None))
            .transpose()
            .err_into()
    }
}

pub trait UpgradeType: Clone {
    fn compare_type(
        &mut self,
        branch: &Xxh3,
        new: &Self,
        dead: &mut HashMap<u128, MemberDef>,
        queries: &mut Vec<TypeMod>,
    ) -> UpgradeResult<Option<PostgresType>>;

    fn update_no_change(&mut self, new: &Self) -> UpgradeResult<Option<PostgresType>> {
        *self = new.clone();
        Ok(None)
    }
    fn update_to(
        &mut self,
        def: &Self,
        pg: impl Into<PostgresType>,
    ) -> UpgradeResult<Option<PostgresType>> {
        *self = def.clone();
        Ok(Some(pg.into()))
    }
    fn update_as(
        &mut self,
        def: Self,
        pg: impl Into<PostgresType>,
    ) -> UpgradeResult<Option<PostgresType>> {
        *self = def;
        Ok(Some(pg.into()))
    }
}

pub trait UpgradeField {
    fn type_def(&self) -> &TypeDef;
    fn type_def_mut(&mut self) -> &mut TypeDef;
    fn name(&self) -> &str;
    fn upgrade_field(
        &mut self,
        branch: &Xxh3,
        new: &Self,
        dead: &mut HashMap<u128, MemberDef>,
        queries: &mut Vec<TypeMod>,
        mods: &mut Vec<StructMod>,
    ) -> UpgradeResult<()> {
        if let Some(pg_type) =
            self.type_def_mut()
                .compare_type(&branch, &new.type_def(), dead, queries)?
        {
            mods.push(StructMod::Alter(PostgresField::new(
                self.name().to_string(),
                pg_type,
            )));
        }
        Ok(())
    }
    fn add_field(&self, branch: &Xxh3, queries: &mut Vec<TypeMod>) -> UpgradeResult<StructMod> {
        let pg_type = self.type_def().extract_type(&branch, queries)?;
        Ok(StructMod::Add(PostgresField::new(
            self.name().to_string(),
            pg_type,
        )))
    }
}

impl UpgradeField for MemberDef {
    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
    fn type_def_mut(&mut self) -> &mut TypeDef {
        &mut self.type_def
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl UpgradeField for VariantDef {
    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
    fn type_def_mut(&mut self) -> &mut TypeDef {
        &mut self.type_def
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl UpgradeType for TypeDef {
    fn compare_type(
        &mut self,
        branch: &Xxh3,
        new: &TypeDef,
        dead: &mut HashMap<u128, MemberDef>,
        queries: &mut Vec<TypeMod>,
    ) -> UpgradeResult<Option<PostgresType>> {
        use introspect_types::TypeDef::{
            Array, Bool, ByteArray, ByteArrayEncoded, Bytes31, Bytes31Encoded, ClassHash,
            ContractAddress, Enum, EthAddress, Felt252, FixedArray, None as TDNone, ShortUtf8,
            StorageAddress, StorageBaseAddress, Struct, Tuple, Utf8String, I128, I16, I32, I64, I8,
            U128, U16, U256, U32, U512, U64, U8,
        };
        use PostgresScalar::{
            BigInt, Felt252 as PgFelt252, Int, Int128, SmallInt, Uint128, Uint16, Uint256, Uint32,
            Uint512, Uint64, Uint8,
        };
        match (&*self, new) {
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
            | (ClassHash, ClassHash)
            | (ContractAddress, ContractAddress)
            | (StorageAddress, StorageAddress)
            | (StorageBaseAddress, StorageBaseAddress)
            | (Bytes31, Bytes31)
            | (ByteArray, ByteArray) => Ok(None),
            (I8, I16)
            | (Bytes31, Bytes31Encoded(_))
            | (ByteArray, ByteArrayEncoded(_))
            | (
                ClassHash | ContractAddress | StorageAddress | StorageBaseAddress,
                ClassHash | ContractAddress | StorageAddress | StorageBaseAddress,
            ) => self.update_no_change(new),
            (Bool, U8) => self.update_as(U8, Uint8),
            (Bool | U8, U16) => self.update_as(U16, Uint16),
            (Bool | U8 | U16, U32) => self.update_as(U32, Uint32),
            (Bool | U8 | U16 | U32, U64) => self.update_as(U64, Uint64),
            (Bool | U8 | U16 | U32 | U64, U128) => self.update_as(U128, Uint128),
            (Bool | U8 | U16 | U32 | U64 | U128, U256) => self.update_as(U256, Uint256),
            (Bool | U8 | U16 | U32 | U64 | U128 | U256, U512) => self.update_as(U512, Uint512),
            (Bool | U8, I8 | I16) => self.update_to(new, SmallInt),
            (Bool | U8 | U16 | I8 | I16, I32) => self.update_as(I32, Int),
            (Bool | U8 | U16 | U32 | I8 | I16 | I32, I64) => self.update_as(I64, BigInt),
            (Bool | U8 | U16 | U32 | U64 | I8 | I16 | I32 | I64, I128) => {
                self.update_as(I128, Int128)
            }
            (ClassHash | ContractAddress | StorageAddress | StorageBaseAddress, Felt252) => {
                self.update_as(Felt252, PgFelt252)
            }
            (_, Struct(new_def)) => self
                .as_struct()?
                .compare_type(branch, new_def, dead, queries),
            (_, Enum(new_def)) => self.as_enum()?.compare_type(branch, new_def, dead, queries),
            (_, FixedArray(new_def)) => self
                .as_fixed_array()?
                .compare_type(branch, new_def, dead, queries),
            (_, Array(new_def)) => self.update_as_array(branch, new_def, dead, queries),
            _ => UpgradeError::type_upgrade_err(self, new),
        }
    }
}

impl UpgradeType for StructDef {
    fn compare_type(
        &mut self,
        branch: &Xxh3,
        new: &StructDef,
        dead: &mut HashMap<u128, MemberDef>,
        queries: &mut Vec<TypeMod>,
    ) -> UpgradeResult<Option<PostgresType>> {
        let mut mods = Vec::new();
        let mut current_map: HashMap<String, MemberDef> = std::mem::take(&mut self.members)
            .into_iter()
            .map(|m| (m.name.clone(), m))
            .collect();
        for member in &new.members {
            let branch = branch.branch(&member.name);
            if let Some(mut current) = current_map
                .remove(&member.name)
                .or_else(|| dead.remove(&branch.digest128()))
            {
                current.upgrade_field(&branch, member, dead, queries, &mut mods)?;
                self.members.push(current);
            } else {
                mods.push(member.add_field(&branch, queries)?);
                self.members.push(member.clone());
            }
        }
        for (_, dead_member) in current_map.drain() {
            dead.insert(branch.branch(&dead_member.name).digest128(), dead_member);
        }
        if !mods.is_empty() {
            queries.push(TypeMod::Struct(StructUpgrade {
                name: branch.type_name(&self.name),
                mods,
            }));
        }
        Ok(None)
    }
}

impl UpgradeType for EnumDef {
    fn compare_type(
        &mut self,
        branch: &Xxh3,
        new: &EnumDef,
        dead: &mut HashMap<u128, MemberDef>,
        queries: &mut Vec<TypeMod>,
    ) -> UpgradeResult<Option<PostgresType>> {
        let mut rename = Vec::new();
        let mut add = Vec::new();
        let mut mods = Vec::new();
        for (id, variant) in &new.variants {
            let branch = branch.branch(id);
            if let Some(mut current) = self.variants.remove(id) {
                if current.name != variant.name {
                    rename.push((current.name.clone(), variant.name.clone()));
                    mods.push(StructMod::Rename(
                        current.name.clone(),
                        variant.name.clone(),
                    ));
                }
                current.upgrade_field(&branch, variant, dead, queries, &mut mods)?;
                self.variants.insert(id.clone(), current);
            } else {
                add.push(variant.name.clone());
                variant.add_field(&branch, queries)?;
                self.variants.insert(id.clone(), variant.clone());
                self.order.push(id.clone());
            }
        }
        if !rename.is_empty() || !add.is_empty() {
            queries.push(TypeMod::Enum(EnumUpgrade {
                name: branch.type_name(&format!("v_{}", self.name)),
                rename,
                add,
            }));
        }
        if !mods.is_empty() {
            queries.push(TypeMod::Struct(StructUpgrade {
                name: branch.type_name(&self.name),
                mods,
            }));
        }
        Ok(None)
    }
}

impl UpgradeType for FixedArrayDef {
    fn compare_type(
        &mut self,
        branch: &Xxh3,
        new: &Self,
        dead: &mut HashMap<u128, MemberDef>,
        queries: &mut Vec<TypeMod>,
    ) -> UpgradeResult<Option<PostgresType>> {
        let pg_type = self
            .type_def
            .compare_type(branch, &new.type_def, dead, queries)?;
        if self.size > new.size {
            return UpgradeError::array_shorten_err(self.size, new.size);
        }
        self.size = new.size;
        pg_type
            .map(|pg_type| pg_type.to_array(Some(vec![self.size])))
            .transpose()
            .err_into()
    }
}

impl UpgradeType for TupleDef{
    fn compare_type(
        &mut self,
        branch: &Xxh3,
        new: &Self,
        dead: &mut HashMap<u128, MemberDef>,
        queries: &mut Vec<TypeMod>,
    ) -> UpgradeResult<Option<PostgresType>> {
        
}
