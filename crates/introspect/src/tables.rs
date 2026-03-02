use std::{collections::HashMap, sync::Arc};

use introspect_types::{
    Attribute, CairoDeserializer, ColumnDef, ColumnInfo, DecodeError, GetRefTypeDef, PrimaryDef, PrimaryTypeDef, PrimaryValue, TableSchema, TypeDef, serialize_def::serialize_with_type_def, transcode::CairoSerializer
};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;

use crate::Record;

#[derive(Debug, thiserror::Error)]
pub enum TableError {
    #[error("Column not found: {0} in table {1}")]
    ColumnNotFound(Felt, String),
    #[error(transparent)]
    DecodeError(#[from] DecodeError),
}

pub type TableResult<T> = Result<T, TableError>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Table {
    pub id: Felt,
    pub name: String,
    pub attributes: Arc<Vec<Attribute>>,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnDef>,
    pub order: Vec<Felt>,
    pub alive: bool,
}

pub struct RecordSchema<'a> {
    pub name: &'a str,
    pub primary: &'a PrimaryDef,
    pub columns: Vec<&'a ColumnDef>,
}

impl Table{
    pub fn get_columns(&self, selectors: &[Felt]) -> TableResult<Vec<&ColumnDef>> {
        selectors
            .into_iter()
            .map(|selector| self.get_column(selector))
            .collect()
    }

    pub fn get_column(&self, selector: &Felt) -> TableResult<&ColumnDef> {
        self.columns
            .get(selector)
            .ok_or_else(|| TableError::ColumnNotFound(*selector, self.name.clone()))
    }
    pub fn get_schema(&self, column_ids: &[Felt]) -> TableResult<RecordSchema> {
        Ok(RecordSchema {
            name: &self.name,
            primary: &self.primary,
            columns: self.get_columns(column_ids)?,
        })
    }
}


impl<'a> RecordSchema<'a>{
    pub fn parse_record<S: CairoSerializer>(&self, record: &Record, serializer: &mut S) -> Result<(), S::Error>{
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        serialize_with_type_def()
    }
}

// pub trait TableStore {
//     type Error;
//     fn read_table(&self, id: Felt) -> Result<Arc<Table>, Self::Error>;
//     fn write_table(&mut self, id: Felt, table: Table) -> Result<(), Self::Error>;
// }

pub trait TablesTrait {
    fn get_table(&self, id: Felt) -> Result<Arc<Table>, TableError>;
    fn parse_records<S: CairoSerializer>(
        &self,
        table_id: Felt,
        columns: Vec<Felt>,
        records: Vec<Record>,
        deserializer: &mut S,
    ) -> Result<(), TableError>{
        let table = self.get_table(table_id)?;
        let columns 
        for record in records {
            let mut deserializer = CairoDeserializer::new(&record.values);
            for column_id in &columns {
                let column = table
                    .columns
                    .get(column_id)
                    .ok_or(TableError {})?;
                let value = deserializer.deserialize_type(&column.type_def)?;
                println!("Parsed value for column {}: {:?}", column.name, value);
            }
        }
        Ok(())
    }
}
