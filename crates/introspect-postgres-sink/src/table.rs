use introspect_types::{ColumnInfo, MemberDef, PrimaryDef, TypeDef};
use itertools::Itertools;
use sqlx::Error::Encode as EncodeError;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use torii_common::sql::PgQuery;
use torii_introspect::{schema::TableInfo, tables::RecordSchema};

use crate::{
    query::{insert_columns_query, insert_table_query},
    PgDbResult, PgSchema, PgTableError, TableResult,
};

#[derive(Debug)]
pub struct PgTable {
    pub schema: PgSchema,
    pub name: String,
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

    pub fn new(schema: &PgSchema, info: TableInfo, dead: Option<Vec<(u128, DeadField)>>) -> Self {
        PgTable {
            schema: schema.clone(),
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
    pub fn insert_queries(
        &self,
        id: &Felt,
        column_ids: Option<&[Felt]>,
        block_number: u64,
        transaction_hash: Felt,
        queries: &mut Vec<PgQuery>,
    ) -> PgDbResult<()> {
        queries.push(
            insert_table_query(
                &self.schema,
                id,
                &self.name,
                &self.primary,
                block_number,
                &transaction_hash,
            )
            .map_err(EncodeError)?,
        );
        let columns = match column_ids {
            Some(ids) => ids.iter().zip(self.columns(ids)?).collect_vec(),
            None => self.columns.iter().collect_vec(),
        };
        queries.push(
            insert_columns_query(&self.schema, id, columns, block_number, &transaction_hash)
                .map_err(EncodeError)?,
        );
        Ok(())
    }
}
