use crate::error::DojoToriiError;
use crate::DojoToriiResult;
use dojo_introspect::selector::compute_selector_from_namespace_and_name;
use dojo_introspect::{DojoSchema, DojoSerde};
use introspect_types::transcode::Transcode;
use introspect_types::{Attribute, Attributes, CairoSerde, ColumnDef, PrimaryDef, TableSchema};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;

const LEGACY_ATTRIBUTE: &str = "legacy";
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct DojoTable {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnDef>,
    pub key_fields: Vec<Felt>,
    pub value_fields: Vec<Felt>,
    pub legacy: bool,
}

pub fn sort_columns(columns: Vec<ColumnDef>) -> (HashMap<Felt, ColumnDef>, Vec<Felt>, Vec<Felt>) {
    let mut field_map = HashMap::new();
    let mut value_fields = Vec::new();
    let mut key_fields = Vec::new();
    for column in columns {
        match column.has_attribute("key") {
            true => key_fields.push(column.id),
            false => value_fields.push(column.id),
        }
        field_map.insert(column.id, column);
    }
    (field_map, key_fields, value_fields)
}

impl From<TableSchema> for DojoTable {
    fn from(value: TableSchema) -> Self {
        let (columns, key_fields, value_fields) = sort_columns(value.columns);
        let legacy = value.attributes.has_attribute(LEGACY_ATTRIBUTE);
        DojoTable {
            id: value.id,
            name: value.name,
            attributes: value.attributes,
            primary: value.primary,
            columns,
            key_fields,
            value_fields,
            legacy: legacy,
        }
    }
}

impl From<DojoTable> for TableSchema {
    fn from(value: DojoTable) -> Self {
        let DojoTable {
            id,
            name,
            mut attributes,
            primary,
            mut columns,
            key_fields,
            value_fields,
            legacy,
        } = value;
        if legacy && !attributes.has_attribute(LEGACY_ATTRIBUTE) {
            attributes.push(Attribute::new_empty(LEGACY_ATTRIBUTE.to_string()));
        }
        TableSchema {
            id: id,
            name: name,
            primary: primary,
            columns: key_fields
                .into_iter()
                .chain(value_fields.into_iter())
                .map(|selector| columns.remove(&selector).unwrap())
                .collect(),
            attributes,
        }
    }
}

impl DojoTable {
    pub fn from_schema(
        schema: DojoSchema,
        namespace: &str,
        name: &str,
        primary: PrimaryDef,
    ) -> Self {
        let (columns, key_fields, value_fields) = sort_columns(schema.columns);
        Self {
            id: compute_selector_from_namespace_and_name(namespace, name),
            name: format!("{}-{}", namespace, name),
            attributes: schema.attributes.clone(),
            primary,
            columns,
            key_fields,
            value_fields,
            legacy: schema.legacy,
        }
    }

    pub fn get_columns(&self, selectors: &[Felt]) -> DojoToriiResult<Vec<&ColumnDef>> {
        selectors
            .into_iter()
            .map(|selector| self.get_column(selector))
            .collect()
    }

    pub fn get_column(&self, selector: &Felt) -> DojoToriiResult<&ColumnDef> {
        self.columns
            .get(selector)
            .ok_or_else(|| DojoToriiError::ColumnNotFound(*selector, self.name.clone()))
    }

    pub fn selectors(&self) -> impl Iterator<Item = &Felt> + '_ {
        self.key_fields.iter().chain(self.value_fields.iter())
    }

    pub fn to_schema(&self) -> TableSchema {
        TableSchema {
            id: self.id,
            name: self.name.clone(),
            attributes: self.attributes.clone(),
            primary: self.primary.clone(),
            columns: self
                .selectors()
                .map(|selector| self.get_column(selector).cloned().unwrap())
                .collect(),
        }
    }

    pub fn parse_keys(&self, keys: Vec<Felt>) -> DojoToriiResult<Vec<u8>> {
        let mut keys: CairoSerde<_> = keys.into();
        let columns = self.get_columns(&self.key_fields)?;
        columns
            .transcode_complete(&mut keys)
            .map_err(DojoToriiError::TranscodeError)
    }

    pub fn parse_values(&self, values: Vec<Felt>) -> DojoToriiResult<(Vec<Felt>, Vec<u8>)> {
        let mut output = Vec::new();
        self.add_parsed_values(values, &mut output)?;
        Ok((self.value_fields.clone(), output))
    }

    pub fn add_parsed_values(
        &self,
        values: Vec<Felt>,
        output: &mut Vec<u8>,
    ) -> DojoToriiResult<()> {
        let mut values: DojoSerde<_> = DojoSerde::new_from_source(values, self.legacy);
        let columns = self.get_columns(&self.value_fields)?;
        columns
            .transcode(&mut values, output)
            .map_err(DojoToriiError::TranscodeError)
    }

    pub fn parse_record(
        &self,
        keys: Vec<Felt>,
        values: Vec<Felt>,
    ) -> DojoToriiResult<(Vec<Felt>, Vec<u8>)> {
        let mut data = self.parse_keys(keys)?;
        self.add_parsed_values(values, &mut data)?;
        Ok((
            [self.key_fields.clone(), self.value_fields.clone()].concat(),
            data,
        ))
    }

    pub fn parse_field(&self, selector: Felt, data: Vec<Felt>) -> DojoToriiResult<Vec<u8>> {
        let mut data: DojoSerde<_> = DojoSerde::new_from_source(data, self.legacy);
        let column = self.get_column(&selector)?;
        column
            .transcode_complete(&mut data)
            .map_err(DojoToriiError::TranscodeError)
    }

    pub fn parse_fields(&self, selectors: &[Felt], data: &[Felt]) -> DojoToriiResult<Vec<u8>> {
        let mut data: DojoSerde<_> = DojoSerde::new_from_source(data, self.legacy);
        let columns = self.get_columns(selectors)?;
        columns
            .transcode_complete(&mut data)
            .map_err(DojoToriiError::TranscodeError)
    }
}
