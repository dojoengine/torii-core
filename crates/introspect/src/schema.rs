use introspect_types::{Attribute, ColumnDef, PrimaryDef};
use starknet_types_core::felt::Felt;

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct ColumnKey {
    pub owner: Option<Felt>,
    pub table: Felt,
    pub id: Felt,
}

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct TableKey {
    pub owner: Option<Felt>,
    pub id: Felt,
}

impl<K: From<(Option<Felt>, Felt)>> From<ColumnKey> for (K, Felt) {
    fn from(value: ColumnKey) -> Self {
        (K::from((value.owner, value.table)), value.id)
    }
}

impl From<ColumnKey> for (Option<Felt>, Felt, Felt) {
    fn from(value: ColumnKey) -> Self {
        (value.owner, value.table, value.id)
    }
}

impl From<TableKey> for (Option<Felt>, Felt) {
    fn from(value: TableKey) -> Self {
        (value.owner, value.id)
    }
}

impl<K: Into<(Option<Felt>, Felt)>> From<(K, Felt)> for ColumnKey {
    fn from(value: (K, Felt)) -> Self {
        let (owner, table) = value.0.into();
        ColumnKey {
            owner,
            table,
            id: value.1,
        }
    }
}

impl From<(Option<Felt>, Felt, Felt)> for ColumnKey {
    fn from(value: (Option<Felt>, Felt, Felt)) -> Self {
        ColumnKey {
            owner: value.0,
            table: value.1,
            id: value.2,
        }
    }
}

impl From<(Option<Felt>, Felt)> for TableKey {
    fn from(value: (Option<Felt>, Felt)) -> Self {
        TableKey {
            owner: value.0,
            id: value.1,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
    pub owner: Option<Felt>,
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

impl TableKey {
    pub fn new(owner: Option<Felt>, id: Felt) -> Self {
        Self { owner, id }
    }
}

pub trait TableKeyTrait {
    fn as_parts(&self) -> (Option<&Felt>, &Felt);
    fn from_parts(owner: Option<Felt>, id: Felt) -> Self;
}

pub trait ColumnKeyTrait {
    fn as_parts(&self) -> (Option<&Felt>, &Felt, &Felt);
    fn from_parts(owner: Option<Felt>, table: Felt, id: Felt) -> Self;
}

impl TableKeyTrait for TableKey {
    fn as_parts(&self) -> (Option<&Felt>, &Felt) {
        (self.owner.as_ref(), &self.id)
    }
    fn from_parts(owner: Option<Felt>, id: Felt) -> Self {
        TableKey { owner, id }
    }
}

impl TableKeyTrait for (Option<Felt>, Felt) {
    fn as_parts(&self) -> (Option<&Felt>, &Felt) {
        (self.0.as_ref(), &self.1)
    }
    fn from_parts(owner: Option<Felt>, id: Felt) -> Self {
        (owner, id)
    }
}

impl ColumnKeyTrait for ColumnKey {
    fn as_parts(&self) -> (Option<&Felt>, &Felt, &Felt) {
        (self.owner.as_ref(), &self.table, &self.id)
    }
    fn from_parts(owner: Option<Felt>, table: Felt, id: Felt) -> Self {
        ColumnKey { owner, table, id }
    }
}

impl ColumnKeyTrait for (Option<Felt>, Felt, Felt) {
    fn as_parts(&self) -> (Option<&Felt>, &Felt, &Felt) {
        (self.0.as_ref(), &self.1, &self.2)
    }
    fn from_parts(owner: Option<Felt>, table: Felt, id: Felt) -> Self {
        (owner, table, id)
    }
}

impl<K: TableKeyTrait> ColumnKeyTrait for (K, Felt) {
    fn as_parts(&self) -> (Option<&Felt>, &Felt, &Felt) {
        let (owner, table) = self.0.as_parts();
        (owner, table, &self.1)
    }
    fn from_parts(owner: Option<Felt>, table: Felt, id: Felt) -> Self {
        (K::from_parts(owner, table), id)
    }
}
