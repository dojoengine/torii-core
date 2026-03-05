use crate::Record;
use introspect_types::{
    bytes::IntoByteSource,
    serialize::{CairoSeFrom, CairoSerialization},
    CairoDeserializer, ColumnDef, PrimaryDef, PrimaryTypeDef, TypeDef,
};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize, Serializer,
};
use std::{cell::RefCell, ops::Deref};

pub struct RecordSchema<'a> {
    primary: &'a PrimaryDef,
    columns: Vec<&'a ColumnDef>,
}

impl<'a> RecordSchema<'a> {
    pub fn new(primary: &'a PrimaryDef, columns: Vec<&'a ColumnDef>) -> Self {
        Self { primary, columns }
    }
    pub fn columns(&self) -> &[&'a ColumnDef] {
        &self.columns
    }

    pub fn primary(&self) -> &PrimaryDef {
        &self.primary
    }

    pub fn to_frame<C: CairoSerialization, M: SerializeEntries>(
        &'a self,
        record: &'a Record,
        metadata: &'a M,
        cairo_se: &'a C,
    ) -> RecordFrame<'a, C, M> {
        RecordFrame::new(record, self, metadata, cairo_se)
    }

    pub fn parse_records_with_metadata<
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
    T: Deref<Target = [&'a E]>,
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

impl AsEntryPair for ColumnDef {
    type Key = String;
    type Value = TypeDef;
    fn to_entry_pair(&self) -> (&Self::Key, &Self::Value) {
        (&self.name, &self.type_def)
    }
}

impl AsEntryPair for PrimaryDef {
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
        schema: &'a RecordSchema<'a>,
        cairo_se: &'a C,
    ) -> RecordFrame<'a, C, M> {
        RecordFrame::new(self.record, schema, self.metadata, cairo_se)
    }
}

pub struct RecordFrame<'a, C: CairoSerialization, M: SerializeEntries> {
    primary: &'a PrimaryDef,
    columns: &'a [&'a ColumnDef],
    id: &'a [u8; 32],
    values: &'a [u8],
    cairo_se: &'a C,
    metadata: &'a M,
}

impl<'a, C: CairoSerialization, M: SerializeEntries> RecordFrame<'a, C, M> {
    pub fn new(
        record: &'a Record,
        schema: &'a RecordSchema<'a>,
        metadata: &'a M,
        cairo_se: &'a C,
    ) -> Self {
        Self {
            primary: &schema.primary,
            columns: &schema.columns,
            id: &record.id,
            values: &record.values,
            metadata,
            cairo_se,
        }
    }

    pub fn parse_primary_as_entry<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        let mut id = self.id.into_source();
        let de = RefCell::new(&mut id);
        map.serialize_entry(
            &self.primary.name,
            &CairoSeFrom::new(&(&self.primary.type_def).into(), &de, self.cairo_se),
        )
    }

    pub fn parse_record_entries<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        CairoSeFrom::new(
            &self.columns,
            &RefCell::new(&mut self.values.into_source()),
            self.cairo_se,
        )
        .serialize_entries::<S>(map)
    }
}

impl<'a, C: CairoSerialization, M: SerializeEntries> SerializeEntries for RecordFrame<'a, C, M> {
    fn entry_count(&self) -> usize {
        1 + self.columns.len() + self.metadata.entry_count()
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

impl SerializeEntries for () {
    fn entry_count(&self) -> usize {
        0
    }

    fn serialize_entries<S: Serializer>(
        &self,
        _map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        Ok(())
    }
}
