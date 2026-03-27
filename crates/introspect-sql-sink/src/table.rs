use crate::{TableError, TableResult};
use introspect_types::{ColumnInfo, MemberDef, TypeDef};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::rc::Rc;
use torii_introspect::schema::TableInfo;
use torii_introspect::tables::RecordSchema;

#[derive(Debug)]
pub struct Table {
    pub id: Felt,
    pub namespace: String,
    pub name: String,
    pub owner: Felt,
    pub primary: ColumnInfo,
    pub columns: HashMap<Felt, ColumnInfo>,
    pub alive: bool,
    pub dead: HashMap<u128, DeadField>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeadField {
    pub name: String,
    pub type_def: TypeDef,
}

#[derive(Debug)]
pub struct DeadFieldDef {
    pub id: u128,
    pub name: String,
    pub type_def: TypeDef,
}

impl From<MemberDef> for DeadField {
    fn from(value: MemberDef) -> Self {
        DeadField {
            name: value.name,
            type_def: value.type_def,
        }
    }
}

impl From<DeadField> for MemberDef {
    fn from(value: DeadField) -> Self {
        MemberDef {
            name: value.name,
            attributes: Vec::new(),
            type_def: value.type_def,
        }
    }
}

impl From<DeadFieldDef> for (u128, DeadField) {
    fn from(value: DeadFieldDef) -> Self {
        (
            value.id,
            DeadField {
                name: value.name,
                type_def: value.type_def,
            },
        )
    }
}

impl From<(u128, DeadField)> for DeadFieldDef {
    fn from(value: (u128, DeadField)) -> Self {
        DeadFieldDef {
            id: value.0,
            name: value.1.name,
            type_def: value.1.type_def,
        }
    }
}

impl Table {
    pub fn column(&self, id: &Felt) -> TableResult<&ColumnInfo> {
        self.columns
            .get(id)
            .ok_or_else(|| TableError::ColumnNotFound(*id, self.name.clone()))
    }

    pub fn namespace(&self) -> Rc<str> {
        self.namespace.as_str().into()
    }

    pub fn columns(&self, ids: &[Felt]) -> TableResult<Vec<&ColumnInfo>> {
        ids.iter()
            .map(|id| self.column(id))
            .collect::<TableResult<Vec<&ColumnInfo>>>()
    }

    pub fn columns_with_ids<'a>(
        &'a self,
        ids: &'a [Felt],
    ) -> TableResult<Vec<(&'a Felt, &'a ColumnInfo)>> {
        ids.iter()
            .map(|id| self.column(id).map(|col| (id, col)))
            .collect::<TableResult<Vec<(&Felt, &ColumnInfo)>>>()
    }

    pub fn new(
        id: Felt,
        namespace: String,
        owner: Felt,
        info: TableInfo,
        dead: Option<Vec<(u128, DeadField)>>,
    ) -> Self {
        Table {
            id,
            namespace,
            owner,
            name: info.name,
            primary: info.primary.into(),
            columns: info.columns.into_iter().map_into().collect(),
            alive: true,
            dead: dead.unwrap_or_default().into_iter().collect(),
        }
    }

    pub fn get_record_schema(&self, columns: &[Felt]) -> TableResult<RecordSchema<'_>> {
        Ok(RecordSchema::new(&self.primary, self.columns(columns)?))
    }
}
