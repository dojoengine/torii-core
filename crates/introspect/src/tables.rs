use crate::Record;
use introspect_types::{
    bytes::IntoByteSource,
    serialize::{CairoSeFrom, CairoSerialization},
    Attribute, CairoDeserializer, ColumnDef, DecodeError, PrimaryDef, PrimaryTypeDef, TypeDef,
};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Serialize, Serializer,
};
use starknet_types_core::felt::Felt;
use std::{cell::RefCell, collections::HashMap, ops::Deref, sync::Arc};

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

    pub fn to_frame<'a, C: CairoSerialization, M: SerializeEntries>(
        &'a self,
        record: &'a Record,
        metadata: &'a M,
        cairo_se: &'a C,
    ) -> RecordFrame<'a, C, M> {
        RecordFrame::new(record, self, metadata, cairo_se)
    }

    pub fn parse_records_with_metadata<
        'a,
        S: Serializer,
        C: CairoSerialization,
        M: SerializeEntries,
    >(
        &self,
        records: &[Record],
        metadata: &'a M,
        serializer: S,
        cairo_se: &'a C,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(records.len()))?;
        for record in records {
            seq.serialize_element(&self.to_frame(record, metadata, cairo_se))?;
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

pub trait SerializeEntries {
    fn entry_count(&self) -> usize;
    fn serialize_entries<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error>;
}

pub trait AsEntryPair {
    type Key;
    type Value;
    fn to_entry_pair(&self) -> (&Self::Key, &Self::Value);
}

impl<'a, 'de, T, D, C, E> SerializeEntries for CairoSeFrom<'a, 'de, T, D, C>
where
    E: AsEntryPair + 'a,
    C: CairoSerialization,
    D: CairoDeserializer,
    T: Deref<Target = [E]>,
    <E as AsEntryPair>::Key: Serialize,
    CairoSeFrom<'a, 'de, <E as AsEntryPair>::Value, D, C>: Serialize,
{
    fn entry_count(&self) -> usize {
        self.schema().deref().len()
    }

    fn serialize_entries<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        for entry in self.schema().deref() {
            let (key, value) = entry.to_entry_pair();
            map.serialize_entry(key, &self.to_schema(value))?;
        }
        Ok(())
    }
}

impl AsEntryPair for Arc<ColumnDef> {
    type Key = String;
    type Value = TypeDef;
    fn to_entry_pair(&self) -> (&Self::Key, &Self::Value) {
        (&self.name, &self.type_def)
    }
}

impl AsEntryPair for Arc<PrimaryDef> {
    type Key = String;
    type Value = PrimaryTypeDef;
    fn to_entry_pair(&self) -> (&Self::Key, &Self::Value) {
        (&self.name, &self.type_def)
    }
}

pub struct RecordWithMetadata<'a, M: SerializeEntries> {
    record: &'a Record,
    metadata: &'a M,
}

impl<'a, M: SerializeEntries> RecordWithMetadata<'a, M> {
    pub fn to_frame<C: CairoSerialization>(
        &self,
        schema: &'a RecordSchema,
        cairo_se: &'a C,
    ) -> RecordFrame<'a, C, M> {
        RecordFrame::new(self.record, schema, self.metadata, cairo_se)
    }
}

pub struct RecordFrame<'a, C: CairoSerialization, M: SerializeEntries> {
    record: &'a Record,
    schema: &'a RecordSchema,
    cairo_se: &'a C,
    metadata: &'a M,
}

impl<'a, C: CairoSerialization, M: SerializeEntries> RecordFrame<'a, C, M> {
    pub fn new(
        record: &'a Record,
        schema: &'a RecordSchema,
        metadata: &'a M,
        cairo_se: &'a C,
    ) -> Self {
        Self {
            record,
            schema,
            metadata,
            cairo_se,
        }
    }

    pub fn parse_primary_as_entry<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        let mut id = self.record.id.into_source();
        let de = RefCell::new(&mut id);
        map.serialize_entry(
            &self.schema.primary.name,
            &CairoSeFrom::new(&(&self.schema.primary.type_def).into(), &de, self.cairo_se),
        )
    }

    pub fn parse_record_entries<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        CairoSeFrom::new(
            &self.schema.columns,
            &RefCell::new(&mut self.record.values.as_slice().into_source()),
            self.cairo_se,
        )
        .serialize_entries::<S>(map)
    }
}

impl<'a, C: CairoSerialization, M: SerializeEntries> SerializeEntries for RecordFrame<'a, C, M> {
    fn entry_count(&self) -> usize {
        1 + self.schema.columns().len() + self.metadata.entry_count()
    }

    fn serialize_entries<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        self.parse_primary_as_entry::<S>(map)?;
        self.parse_record_entries::<S>(map)?;
        self.metadata.serialize_entries::<S>(map)
    }
}

impl<'a, C: CairoSerialization, M: SerializeEntries> Serialize for RecordFrame<'a, C, M> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.entry_count()))?;
        self.serialize_entries::<S>(&mut map)?;
        map.end()
    }
}
