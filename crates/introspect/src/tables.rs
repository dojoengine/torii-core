use std::{cell::RefCell, collections::HashMap, sync::Arc};

use introspect_types::{
    bytes::IntoByteSource,
    serialize::{CairoSeFrom, CairoSerialization},
    Attribute, ColumnDef, DecodeError, PrimaryDef,
};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Serialize, Serializer,
};
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
    pub primary: Arc<PrimaryDef>,
    pub columns: HashMap<Felt, Arc<ColumnDef>>,
    pub order: Vec<Felt>,
    pub alive: bool,
}

pub struct RecordSchema {
    name: String,
    primary: Arc<PrimaryDef>,
    columns: Vec<Arc<ColumnDef>>,
}

impl Table {
    pub fn get_columns(&self, selectors: &[Felt]) -> TableResult<Vec<Arc<ColumnDef>>> {
        selectors
            .into_iter()
            .map(|selector| self.get_column(selector))
            .collect()
    }

    pub fn get_column(&self, selector: &Felt) -> TableResult<Arc<ColumnDef>> {
        self.columns
            .get(selector)
            .ok_or_else(|| TableError::ColumnNotFound(*selector, self.name.clone()))
            .cloned()
    }

    pub fn get_schema(&self, column_ids: &[Felt]) -> TableResult<RecordSchema> {
        let columns = self.get_columns(column_ids)?;
        Ok(RecordSchema {
            name: self.name.clone(),
            primary: self.primary.clone(),
            columns,
        })
    }
}

impl RecordSchema {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[Arc<ColumnDef>] {
        &self.columns
    }

    pub fn primary(&self) -> &PrimaryDef {
        &self.primary
    }

    pub fn parse_primary_as_entry<'a, S: Serializer, C: CairoSerialization>(
        &self,
        record: &Record,
        serializer: &mut <S as Serializer>::SerializeMap,
        cairo_se: &'a C,
    ) -> Result<(), S::Error> {
        let mut id = record.id.into_source();
        let de = RefCell::new(&mut id);
        serializer.serialize_entry(
            &self.primary.name,
            &CairoSeFrom::new(&(&self.primary.type_def).into(), &de, cairo_se),
        )
    }

    pub fn parse_record_entries<'a, S: Serializer, C: CairoSerialization>(
        &self,
        record: &Record,
        map: &mut <S as Serializer>::SerializeMap,
        cairo_se: &'a C,
    ) -> Result<(), S::Error> {
        let mut values = record.values.as_slice().into_source();
        let de = RefCell::new(&mut values);
        for col in &self.columns {
            map.serialize_entry(&col.name, &CairoSeFrom::new(&col.type_def, &de, cairo_se))?;
        }
        Ok(())
    }

    pub fn parse_record<'a, S: Serializer, C: CairoSerialization>(
        &self,
        record: &Record,
        serializer: S,
        cairo_se: &'a C,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1 + self.columns.len()))?;
        self.parse_primary_as_entry::<S, C>(record, &mut map, cairo_se)?;
        self.parse_record_entries::<S, C>(record, &mut map, cairo_se)?;
        map.end()
    }

    pub fn parse_records<'a, S: Serializer, C: CairoSerialization>(
        &self,
        records: &[Record],
        serializer: S,
        cairo_se: &'a C,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(records.len()))?;
        for record in records {
            seq.serialize_element(&RecordAndSchema::new(record, self, cairo_se))?;
        }
        seq.end()
    }
}

pub trait IntrospectTables {
    fn get_table(&self, id: Felt) -> Result<Arc<Table>, TableError>;
    fn get_record_schema<'a>(&'a self, id: Felt, column_ids: &[Felt]) -> TableResult<RecordSchema> {
        let table = self.get_table(id)?.clone();
        table.get_schema(column_ids)
    }
}

struct RecordAndSchema<'a, C: CairoSerialization> {
    record: &'a Record,
    schema: &'a RecordSchema,
    cairo_se: &'a C,
}

impl<'a, C: CairoSerialization> RecordAndSchema<'a, C> {
    pub fn new(record: &'a Record, schema: &'a RecordSchema, cairo_se: &'a C) -> Self {
        Self {
            record,
            schema,
            cairo_se,
        }
    }
}

impl<'a, C: CairoSerialization> Serialize for RecordAndSchema<'a, C> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.schema
            .parse_record(self.record, serializer, self.cairo_se)
    }
}
