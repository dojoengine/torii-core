use introspect_types::{Attribute, Attributes, ColumnDef, TableSchema};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DojoTable {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
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
            name: value.name.clone(),
            attributes: value.attributes.clone(),
            columns: field_map,
            key_fields,
            value_fields,
        }
    }
}

impl From<DojoTable> for TableSchema {
    fn from(value: DojoTable) -> Self {
        TableSchema {
            id: value.id,
            name: value.name.clone(),
            attributes: value.attributes.clone(),
            primary: primary_field_def(),
            columns: value
                .key_fields
                .iter()
                .chain(value.value_fields.iter())
                .map(|selector| value.columns.get(selector).cloned().unwrap())
                .collect(),
        }
    }
}

impl DojoTable {
    pub fn parse_keys(&self, keys: Vec<Felt>) -> TableResult<Vec<IdValue>> {
        let mut keys = keys.into_iter();
        let values = self
            .key_fields
            .iter()
            .map(|selector| {
                self.columns
                    .get(selector)?
                    .type_def
                    .to_value(&mut keys)
                    .map(|v| IdValue::new(*selector, v))
            })
            .collect::<Option<Vec<_>>>();
        match keys.next() {
            None => {
                values.ok_or_else(|| DojoTableErrors::ParseValuesError(self.name.clone()).into())
            }
            _ => Err(DojoTableErrors::ParseValuesError(self.name.clone()).into()),
        }
    }

    pub fn parse_values(&self, values: Vec<Felt>) -> TableResult<Vec<IdValue>> {
        let mut values = values.into_iter();
        let vals = self
            .value_fields
            .iter()
            .map(|selector| {
                self.columns
                    .get(selector)?
                    .type_def
                    .to_value(&mut values)
                    .map(|v| IdValue::new(*selector, v))
            })
            .collect::<Option<Vec<_>>>();

        match values.next() {
            None => vals.ok_or_else(|| DojoTableErrors::ParseValuesError(self.name.clone()).into()),
            Some(_) => Err(DojoTableErrors::ParseValuesError(self.name.clone()).into()),
        }
    }

    pub fn parse_key_values(
        &self,
        keys: Vec<Felt>,
        values: Vec<Felt>,
    ) -> TableResult<Vec<IdValue>> {
        let mut k = self.parse_keys(keys)?;
        let mut v = self.parse_values(values)?;
        k.append(&mut v);
        Ok(k)
    }

    pub fn parse_field(&self, selector: Felt, data: Vec<Felt>) -> TableResult<Value> {
        let mut data = data.into_iter();
        let column_def = self
            .columns
            .get(&selector)
            .ok_or_else(|| DojoTableErrors::FieldNotFound(selector, self.name.clone()))?;
        let field = column_def
            .type_def
            .to_value(&mut data)
            .ok_or_else(|| DojoTableErrors::FieldParseError(selector, self.name.clone()))?;
        match data.next() {
            None => Ok(field),
            _ => Err(DojoTableErrors::TooManyFieldValues(selector)),
        }
    }

    pub fn parse_fields(
        &self,
        selectors: &[Felt],
        data: &mut FeltIterator,
    ) -> TableResult<Vec<Field>> {
        let fields = selectors
            .iter()
            .map(|selector| {
                let field_def = self.columns.get(selector)?;
                field_def.to_value(data)
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| DojoTableErrors::ParseValuesError(self.name.clone()))?;

        match data.next() {
            None => Ok(fields),
            _ => Err(DojoTableErrors::ParseValuesError(format!(
                "Too many values for table {}",
                self.name
            ))),
        }
    }
}
