use crate::error::{SchemaError, SchemaResult};
use crate::utils::felt_to_schema;
use crate::{DbError, DbResult};
use introspect_types::ResultInto;
use itertools::Itertools;
use starknet_types_core::felt::Felt;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub enum SchemaMode {
    Single(Arc<str>),
    Address,
    Named(HashMap<Felt, Arc<str>>),
    Addresses(HashSet<Felt>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableKey {
    schema: SchemaKey,
    id: Felt,
}

impl TableKey {
    pub fn new(schema: SchemaKey, id: Felt) -> Self {
        Self { schema, id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaKey {
    Single(Arc<str>),
    Address(Felt),
    Named(Arc<str>),
}

impl Hash for TableKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.id.hash(state);
    }
}

impl Hash for SchemaKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            SchemaKey::Address(addr) => addr.hash(state),
            SchemaKey::Named(name) => name.hash(state),
            SchemaKey::Single(_) => {}
        }
    }
}

impl Display for SchemaKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaKey::Address(addr) => write!(f, "{addr:063x}"),
            SchemaKey::Named(name) | SchemaKey::Single(name) => name.fmt(f),
        }
    }
}

impl Display for TableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !matches!(self.schema, SchemaKey::Single(_)) {
            write!(f, "{} ", self.schema)?;
        }
        write!(f, "{:#063x}", self.id)
    }
}

impl From<String> for SchemaMode {
    fn from(value: String) -> Self {
        SchemaMode::Single(value.into())
    }
}

impl From<&str> for SchemaMode {
    fn from(value: &str) -> Self {
        SchemaMode::Single(value.into())
    }
}

impl From<HashMap<Felt, String>> for SchemaMode {
    fn from(value: HashMap<Felt, String>) -> Self {
        SchemaMode::Named(
            value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
        )
    }
}

impl<const N: usize> From<[(Felt, &str); N]> for SchemaMode {
    fn from(value: [(Felt, &str); N]) -> Self {
        SchemaMode::Named(
            value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
        )
    }
}

impl<const N: usize> From<[(Felt, String); N]> for SchemaMode {
    fn from(value: [(Felt, String); N]) -> Self {
        SchemaMode::Named(
            value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
        )
    }
}

impl<const N: usize> From<[Felt; N]> for SchemaMode {
    fn from(value: [Felt; N]) -> Self {
        SchemaMode::Addresses(value.into_iter().collect())
    }
}

impl From<Vec<Felt>> for SchemaMode {
    fn from(value: Vec<Felt>) -> Self {
        SchemaMode::Addresses(value.into_iter().collect())
    }
}

fn felt_try_from_schema(schema: &str) -> SchemaResult<Felt> {
    match schema.len() == 63 {
        true => Felt::from_hex(schema).err_into(),
        false => Err(SchemaError::InvalidAddressLength(schema.to_string())),
    }
}

impl SchemaMode {
    pub fn schemas(&self) -> Option<Vec<Arc<str>>> {
        match self {
            SchemaMode::Single(name) => Some(vec![name.clone()]),
            SchemaMode::Address => None,
            SchemaMode::Named(map) => Some(map.values().cloned().collect()),
            SchemaMode::Addresses(set) => Some(set.iter().map(felt_to_schema).map_into().collect()),
        }
    }

    pub fn get_schema_key(&self, schema: String, owner: &Felt) -> SchemaResult<SchemaKey> {
        match self {
            SchemaMode::Single(s) => match **s == *schema {
                true => Ok(SchemaKey::Single(s.clone())),
                false => Err(SchemaError::SchemaMismatch(schema, s.to_string())),
            },
            SchemaMode::Address => felt_try_from_schema(&schema).map(SchemaKey::Address),
            SchemaMode::Named(map) => match map.get(owner) {
                Some(s) if **s == *schema => Ok(SchemaKey::Named(s.clone())),
                Some(s) => Err(SchemaError::SchemaMismatch(schema, s.to_string())),
                None => Err(SchemaError::AddressNotFound(*owner, schema)),
            },
            SchemaMode::Addresses(set) => {
                let address = felt_try_from_schema(&schema)?;
                match set.contains(&address) {
                    true => Ok(SchemaKey::Address(address)),
                    false => Err(SchemaError::AddressNotFound(address, schema)),
                }
            }
        }
    }

    pub fn get_key(&self, schema: String, id: Felt, owner: &Felt) -> SchemaResult<TableKey> {
        self.get_schema_key(schema, owner)
            .map(|k| TableKey::new(k, id))
    }

    pub fn to_schema(&self, from_address: &Felt) -> DbResult<SchemaKey> {
        match self {
            SchemaMode::Single(name) => Ok(SchemaKey::Single(name.clone())),
            SchemaMode::Address => Ok(SchemaKey::Address(*from_address)),
            SchemaMode::Named(map) => match map.get(from_address) {
                Some(schema) => Ok(SchemaKey::Named(schema.clone())),
                None => Err(DbError::SchemaNotFound(*from_address)),
            },
            SchemaMode::Addresses(set) => match set.contains(from_address) {
                true => Ok(SchemaKey::Address(*from_address)),
                false => Err(DbError::SchemaNotFound(*from_address)),
            },
        }
    }
}
