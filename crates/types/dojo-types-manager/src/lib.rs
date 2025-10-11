use anyhow::{anyhow, Ok, Result};
use dojo_introspect_utils::selector::compute_selector_from_namespace_and_name;
use introspect_events::types::TableSchema;
use introspect_types::{ColumnDef, FieldDef};
use introspect_value::{FeltIterator, Field, ToValue};
use serde::{Deserialize, Serialize};
use starknet::core::utils::get_selector_from_name;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

const KEY_ATTR: &str = "key";

pub struct DojoManager<Store> {
    pub tables: HashMap<Felt, DojoTable>,
    pub store: Store,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DojoTable {
    pub id: Felt,
    pub name: String,
    pub attrs: Vec<String>,
    pub fields: HashMap<Felt, ColumnDef>,
    pub key_fields: Vec<Felt>,
    pub value_fields: Vec<Felt>,
}

impl DojoTable {
    pub fn schema(&self) -> TableSchema {
        TableSchema {
            table_id: self.id,
            table_name: self.name.clone(),
            attrs: self.attrs.clone(),
            fields: self
                .key_fields
                .iter()
                .chain(self.value_fields.iter())
                .map(|selector| self.fields.get(selector).cloned().unwrap())
                .collect(),
        }
    }

    pub fn parse_keys(&self, keys: Vec<Felt>) -> Option<Vec<Field>> {
        let mut keys = keys.into_iter();
        let values = self
            .key_fields
            .iter()
            .map(|selector| self.fields.get(selector)?.to_value(&mut keys))
            .collect::<Option<Vec<_>>>();
        match keys.next() {
            None => values,
            _ => None,
        }
    }

    pub fn parse_values(&self, values: Vec<Felt>) -> Option<Vec<Field>> {
        let mut values = values.into_iter();
        let vals = self
            .value_fields
            .iter()
            .map(|selector| self.fields.get(selector)?.to_value(&mut values))
            .collect::<Option<Vec<_>>>();
        match values.next() {
            None => vals,
            _ => None,
        }
    }

    pub fn parse_key_values(&self, keys: Vec<Felt>, values: Vec<Felt>) -> Option<Vec<Field>> {
        match (self.parse_keys(keys), self.parse_values(values)) {
            (Some(mut k), Some(mut v)) => {
                k.append(&mut v);
                Some(k)
            }
            _ => None,
        }
    }

    pub fn parse_field(&self, selector: Felt, data: Vec<Felt>) -> Option<Field> {
        let mut data = data.into_iter();
        let field = self.fields.get(&selector)?.to_value(&mut data);
        match data.next() {
            None => field,
            _ => None,
        }
    }

    pub fn parse_fields(&self, selectors: &[Felt], data: &mut FeltIterator) -> Option<Vec<Field>> {
        let fields = selectors
            .iter()
            .map(|selector| self.fields.get(selector)?.to_value(data))
            .collect::<Option<Vec<_>>>();
        match data.next() {
            None => fields,
            _ => None,
        }
    }
}

impl DojoManager<JsonStore> {
    pub fn new(path: &Path) -> Self {
        let store = JsonStore::new(path);
        let mut manager = Self {
            tables: HashMap::new(),
            store,
        };
        manager.load_tables();
        manager
    }

    pub fn load_tables(&mut self) {
        let paths = fs::read_dir(&self.store.path).unwrap();
        for path in paths {
            let path = path.unwrap().path();
            let table_id = path
                .file_name()
                .and_then(|p| json_file_name_to_felt(p.to_str()?));
            let data: Option<DojoTable> =
                serde_json::from_str(&fs::read_to_string(&path).unwrap()).ok();
            match (table_id, data) {
                (Some(id), Some(table)) => {
                    self.tables.insert(id, table);
                }
                _ => {}
            }
        }
    }
}

pub struct JsonStore {
    pub path: PathBuf,
}

impl JsonStore {
    pub fn new(path: &Path) -> Self {
        if !path.exists() {
            std::fs::create_dir_all(path).expect("Unable to create directory");
        }
        Self {
            path: path.to_path_buf(),
        }
    }
}

pub trait StoreTrait {
    type Table;
    fn dump(&self, table_id: Felt, data: &Self::Table) -> Result<()>;
    fn load(&self, table_id: Felt) -> Result<Self::Table>;
}

fn felt_to_fixed_hex_string(felt: &Felt) -> String {
    format!("0x{:0>32x}", felt)
}
fn felt_to_json_file_name(felt: &Felt) -> String {
    format!("{}.json", felt_to_fixed_hex_string(felt))
}

fn json_file_name_to_felt(file_name: &str) -> Option<Felt> {
    let hex_str = file_name.strip_suffix(".json")?;
    Felt::from_hex(hex_str).ok()
}

impl StoreTrait for JsonStore {
    type Table = DojoTable;

    fn dump(&self, table_id: Felt, data: &Self::Table) -> Result<()> {
        let file_path = self.path.join(felt_to_json_file_name(&table_id));
        std::fs::write(file_path, serde_json::to_string(data).unwrap())
            .expect("Unable to write file");
        Ok(())
    }

    fn load(&self, table_id: Felt) -> Result<Self::Table> {
        let file_path = self.path.join(felt_to_json_file_name(&table_id));
        let data = std::fs::read_to_string(file_path).expect("Unable to read file");
        serde_json::from_str(&data).map_err(|e| anyhow!("Failed to parse JSON: {}", e))
    }
}

impl<Store> DojoManager<Store>
where
    Store: StoreTrait<Table = DojoTable>,
{
    pub fn register_table(
        &mut self,
        namespace: String,
        name: String,
        attrs: Vec<String>,
        fields: Vec<FieldDef>,
    ) -> Result<TableSchema> {
        let id = compute_selector_from_namespace_and_name(&namespace, &name);
        let table_name = format!("{namespace}-{name}");
        if self.tables.contains_key(&id) {
            return Err(anyhow!("Table already exists"));
        }

        let mut field_map = HashMap::new();
        let mut value_fields = Vec::new();
        let mut key_fields = Vec::new();
        for field in fields {
            let selector = get_selector_from_name(&field.name)?;
            match field.attrs.contains(&KEY_ATTR.to_string()) {
                true => key_fields.push(selector),
                false => value_fields.push(selector),
            }

            field_map.insert(
                selector,
                ColumnDef {
                    selector,
                    name: field.name,
                    attrs: field.attrs,
                    type_def: field.type_def,
                },
            );
        }
        let table = DojoTable {
            id,
            name: table_name,
            attrs,
            fields: field_map,
            value_fields,
            key_fields,
        };

        let schema = table.schema();
        self.store.dump(id, &table)?;
        self.tables.insert(id, table);
        Ok(schema)
    }

    pub fn update_table(&mut self, id: Felt, fields: Vec<FieldDef>) -> Result<TableSchema> {
        if !self.tables.contains_key(&id) {
            return Err(anyhow!("Table not found"));
        }
        let table = match self.tables.get_mut(&id) {
            Some(t) => t,
            None => return Err(anyhow!("Table not found")),
        };
        let mut key_fields = Vec::new();
        let mut value_fields = Vec::new();
        for field in fields {
            let selector = get_selector_from_name(&field.name)?;
            match field.attrs.contains(&KEY_ATTR.to_string()) {
                true => key_fields.push(selector),
                false => value_fields.push(selector),
            }
            table.fields.insert(
                selector,
                ColumnDef {
                    selector,
                    name: field.name,
                    attrs: field.attrs,
                    type_def: field.type_def,
                },
            );
        }
        table.key_fields = key_fields;
        table.value_fields = value_fields;
        self.store.dump(id, &table)?;
        Ok(table.schema())
    }

    pub fn get_table(&self, id: Felt) -> Option<&DojoTable> {
        self.tables.get(&id)
    }
}
