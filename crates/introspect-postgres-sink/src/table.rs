use introspect_types::{ColumnInfo, FeltIds, MemberDef, PrimaryDef};
use itertools::Itertools;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use torii_introspect::{schema::TableInfo, tables::RecordSchema};

use crate::{PgSchema, PgTableError, TableResult};

#[derive(Debug)]
pub struct PgTable {
    pub schema: PgSchema,
    pub name: String,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnInfo>,
    pub order: Vec<Felt>,
    pub alive: bool,
    pub dead: HashMap<u128, MemberDef>,
}

impl PgTable {
    pub fn column(&self, id: &Felt) -> TableResult<&ColumnInfo> {
        self.columns
            .get(id)
            .ok_or_else(|| PgTableError::ColumnNotFound(*id, self.name.clone()))
    }

    pub fn columns(&self, ids: &[Felt]) -> TableResult<Vec<&ColumnInfo>> {
        ids.iter()
            .map(|id| self.column(id))
            .collect::<TableResult<Vec<&ColumnInfo>>>()
    }

    pub fn new(schema: &PgSchema, info: TableInfo) -> Self {
        let order = info.columns.ids();
        PgTable {
            schema: schema.clone(),
            name: info.name,
            primary: info.primary,
            columns: info.columns.into_iter().map_into().collect(),
            order,
            alive: true,
            dead: HashMap::new(),
        }
    }
    pub fn get_record_schema(&self, columns: &[Felt]) -> TableResult<RecordSchema<'_>> {
        Ok(RecordSchema::new(&self.primary, self.columns(columns)?))
    }
}
