use crate::{DbResult, TableError, TableResult};
use introspect_types::{ColumnDef, ColumnInfo, MemberDef, PrimaryDef, TypeDef};
use itertools::Itertools;
use sqlx::Database;
use starknet_types_core::felt::Felt;
use std::{collections::HashMap, rc::Rc};
use torii_common::sql::FlexQuery;
use torii_introspect::{schema::TableInfo, tables::RecordSchema, Record};

#[derive(Debug)]
pub struct DbTable {
    pub schema: String,
    pub name: String,
    pub owner: Felt,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnInfo>,
    pub alive: bool,
    pub dead: HashMap<u128, DeadField>,
}

#[derive(Debug)]
pub struct DeadField {
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

pub trait DbTableTrait<DB: Database> {
    fn create_table_queries(
        &self,
        id: &Felt,
        order: &[Felt],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<DB>>,
    ) -> DbResult<()>;
    fn update_table_queries(
        &mut self,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<DB>>,
    ) -> DbResult<()>;
    fn insert_fields_queries(
        &self,
        columns: &[Felt],
        records: &[Record],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<DB>>,
    ) -> DbResult<()>;
}

impl DbTable {
    pub fn column(&self, id: &Felt) -> TableResult<&ColumnInfo> {
        self.columns
            .get(id)
            .ok_or_else(|| TableError::ColumnNotFound(*id, self.name.clone()))
    }

    pub fn schema(&self) -> Rc<str> {
        self.schema.as_str().into()
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
        schema: String,
        owner: Felt,
        info: TableInfo,
        dead: Option<Vec<(u128, DeadField)>>,
    ) -> Self {
        DbTable {
            schema,
            owner,
            name: info.name,
            primary: info.primary,
            columns: info.columns.into_iter().map_into().collect(),
            alive: true,
            dead: dead.unwrap_or_default().into_iter().collect(),
        }
    }

    pub fn get_record_schema(&self, columns: &[Felt]) -> TableResult<RecordSchema<'_>> {
        Ok(RecordSchema::new(&self.primary, self.columns(columns)?))
    }
}
