use introspect_types::{Attribute, Attributes, ColumnDef, ParseValue, PrimaryDef, TableSchema};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use torii_introspect::IdValue;

use crate::{error::DojoToriiError, DojoToriiResult};

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct DojoTable {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnDef>,
    pub key_fields: Vec<Felt>,
    pub value_fields: Vec<Felt>,
}

impl From<TableSchema> for DojoTable {
    fn from(value: TableSchema) -> Self {
        let mut field_map = HashMap::new();
        let mut value_fields = Vec::new();
        let mut key_fields = Vec::new();
        for column in value.columns {
            match column.has_attribute("key") {
                true => key_fields.push(column.id),
                false => value_fields.push(column.id),
            }
            field_map.insert(column.id, column);
        }
        DojoTable {
            id: value.id,
            name: value.name,
            attributes: value.attributes,
            primary: value.primary,
            columns: field_map,
            key_fields,
            value_fields,
        }
    }
}

impl From<DojoTable> for TableSchema {
    fn from(value: DojoTable) -> Self {
        let DojoTable {
            id,
            name,
            attributes,
            primary,
            mut columns,
            key_fields,
            value_fields,
        } = value;

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

    pub fn parse_keys(&self, keys: Vec<Felt>) -> DojoToriiResult<Vec<IdValue>> {
        let mut keys = keys.into_iter();
        let values = self
            .key_fields
            .iter()
            .map(|selector| {
                self.get_column(selector)?
                    .type_def
                    .parse(&mut keys)
                    .map(|v| IdValue::new(*selector, v))
            })
            .collect::<Option<Vec<_>>>();
        match keys.next() {
            None => values
                .ok_or_else(|| DojoIntrospectResult::ParseValuesError(self.name.clone()).into()),
            _ => Err(DojoIntrospectResult::ParseValuesError(self.name.clone()).into()),
        }
    }

    pub fn parse_values(&self, values: Vec<Felt>) -> DojoToriiResult<Vec<IdValue>> {
        let mut values = values.into_iter();
        let vals = self
            .value_fields
            .iter()
            .map(|selector| {
                self.get_column(selector)?
                    .type_def
                    .parse(&mut values)
                    .map(|v| IdValue::new(*selector, v))
            })
            .collect::<Option<Vec<_>>>();

        match values.next() {
            None => {
                vals.ok_or_else(|| DojoIntrospectResult::ParseValuesError(self.name.clone()).into())
            }
            Some(_) => Err(DojoIntrospectResult::ParseValuesError(self.name.clone()).into()),
        }
    }

    pub fn parse_key_values(
        &self,
        keys: Vec<Felt>,
        values: Vec<Felt>,
    ) -> DojoToriiResult<Vec<IdValue>> {
        let mut k = self.parse_keys(keys)?;
        let mut v = self.parse_values(values)?;
        k.append(&mut v);
        Ok(k)
    }

    pub fn parse_field(&self, selector: Felt, data: Vec<Felt>) -> DojoToriiResult<Value> {
        let mut data = data.into_iter();
        let column_def = self
            .columns
            .get(&selector)
            .ok_or_else(|| DojoIntrospectResult::FieldNotFound(selector, self.name.clone()))?;
        let field = column_def
            .type_def
            .parse(&mut data)
            .ok_or_else(|| DojoIntrospectResult::FieldParseError(selector, self.name.clone()))?;
        match data.next() {
            None => Ok(field),
            _ => Err(DojoIntrospectResult::TooManyFieldValues(selector)),
        }
    }

    pub fn parse_fields(
        &self,
        selectors: &[Felt],
        data: &mut FeltIterator,
    ) -> DojoToriiResult<Vec<Field>> {
        let fields = selectors
            .iter()
            .map(|selector| {
                let field_def = self.columns.get(selector)?;
                field_def.parse(data)
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| DojoIntrospectResult::ParseValuesError(self.name.clone()))?;

        match data.next() {
            None => Ok(fields),
            _ => Err(DojoIntrospectResult::ParseValuesError(format!(
                "Too many values for table {}",
                self.name
            ))),
        }
    }
}
