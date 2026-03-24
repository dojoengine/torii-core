use crate::{
    query::{insert_columns_query, insert_table_query, CreatePgTable},
    PgDbResult, PgTableError, TableResult,
};
use introspect_types::{ColumnInfo, MemberDef, PrimaryDef, ResultInto, TypeDef};
use itertools::Itertools;
use sqlx::Error::Encode as EncodeError;
use starknet_types_core::felt::Felt;
use std::{collections::HashMap, rc::Rc};
use torii_common::sql::PgQuery;
use torii_introspect::{schema::TableInfo, tables::RecordSchema};

#[derive(Debug)]
pub struct PgTable {
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

impl PgTable {
    pub fn column(&self, id: &Felt) -> TableResult<&ColumnInfo> {
        self.columns
            .get(id)
            .ok_or_else(|| PgTableError::ColumnNotFound(*id, self.name.clone()))
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
        PgTable {
            schema,
            owner,
            name: info.name,
            primary: info.primary,
            columns: info.columns.into_iter().map_into().collect(),
            alive: true,
            dead: dead.unwrap_or_default().into_iter().collect(),
        }
    }

    pub fn create_table(&self, id: &Felt, order: &[Felt]) -> TableResult<CreatePgTable> {
        CreatePgTable::new(
            &self.schema(),
            id,
            &self.name,
            &self.primary,
            self.columns_with_ids(order)?,
        )
        .err_into()
    }

    pub fn create_table_queries(
        &self,
        id: &Felt,
        order: &[Felt],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> PgDbResult<()> {
        self.create_table(id, order)?.make_queries(queries);
        self.insert_queries(
            id,
            None,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
    }

    pub fn get_record_schema(&self, columns: &[Felt]) -> TableResult<RecordSchema<'_>> {
        Ok(RecordSchema::new(&self.primary, self.columns(columns)?))
    }
    pub fn insert_queries(
        &self,
        id: &Felt,
        column_ids: Option<&[Felt]>,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> PgDbResult<()> {
        queries.push(
            insert_table_query(
                &self.schema,
                id,
                &self.name,
                &self.primary,
                from_address,
                block_number,
                transaction_hash,
            )
            .map_err(EncodeError)?,
        );
        let columns = match column_ids {
            Some(ids) => ids.iter().zip(self.columns(ids)?).collect_vec(),
            None => self.columns.iter().collect_vec(),
        };
        queries.push(
            insert_columns_query(&self.schema, id, columns, block_number, transaction_hash)
                .map_err(EncodeError)?,
        );
        Ok(())
    }
}
