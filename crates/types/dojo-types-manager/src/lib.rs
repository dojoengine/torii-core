use introspect_events::types::TableFieldsDef;
use introspect_types::FieldDef;
use serde::{Deserialize, Serialize};
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
    pub fields: HashMap<Felt, FieldDef>,
    pub key_fields: Vec<Felt>,
    pub value_fields: Vec<Felt>,
}

pub struct DojoModel {
    pub id: Felt,
    pub name: String,
    pub attrs: Vec<String>,
    pub keys: Vec<FieldDef>,
    pub values: Vec<FieldDef>,
}
impl DojoTable {
    pub fn get_entity(&self) -> TableFieldsDef {
        let fields = self
            .value_fields
            .iter()
            .filter_map(|selector| self.fields.get(selector).cloned())
            .collect();

        TableFieldsDef {
            id: self.id,
            name: self.name.clone(),
            attrs: self.attrs.clone(),
            fields,
        }
    }
    pub fn get_model(&self) -> DojoModel {
        DojoModel {
            id: self.id,
            name: self.name.clone(),
            attrs: vec![],
            keys: self
                .key_fields
                .iter()
                .filter_map(|selector| self.fields.get(selector).cloned())
                .collect(),
            values: self
                .value_fields
                .iter()
                .filter_map(|selector| self.fields.get(selector).cloned())
                .collect(),
        }
    }

    pub fn get_schema(&self) -> Option<TableFieldsDef> {
        Some(TableFieldsDef {
            id: self.id,
            name: self.name.clone(),
            attrs: vec![],
            fields: self
                .key_fields
                .iter()
                .chain(self.value_fields.iter())
                .map(|selector| self.fields.get(selector).cloned())
                .collect::<Option<Vec<_>>>()?
                .into(),
        })
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
    fn dump(&self, table_id: Felt, data: &Self::Table) -> bool;
    fn load(&self, table_id: Felt) -> Option<Self::Table>;
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

    fn dump(&self, table_id: Felt, data: &Self::Table) -> bool {
        let file_path = self.path.join(felt_to_json_file_name(&table_id));
        std::fs::write(file_path, serde_json::to_string(data).unwrap())
            .expect("Unable to write file");
        true
    }

    fn load(&self, table_id: Felt) -> Option<Self::Table> {
        let file_path = self.path.join(felt_to_json_file_name(&table_id));
        let data = std::fs::read_to_string(file_path).expect("Unable to read file");
        serde_json::from_str(&data).ok()
    }
}

pub trait IntrospectManager {
    type Field;
    type Table;
    fn register_table(
        &mut self,
        id: Felt,
        name: &str,
        attrs: Vec<String>,
        fields: Vec<Self::Field>,
    ) -> bool;
    fn update_table(
        &mut self,
        id: Felt,
        name: &str,
        attrs: Vec<String>,
        fields: Vec<Self::Field>,
    ) -> bool;
    fn get_model(&self, id: Felt) -> Option<DojoModel>;
    fn get_entity(&self, id: Felt) -> Option<TableFieldsDef>;
    fn get_table_name(&self, table_id: Felt) -> Option<String>;
    fn get_table_field(&self, table_id: Felt, field_selector: Felt) -> Option<Self::Field>;
    fn get_table_fields(&self, table_id: Felt) -> Option<Vec<Self::Field>>;
}

impl<Store> IntrospectManager for DojoManager<Store>
where
    Store: StoreTrait<Table = DojoTable>,
{
    type Field = FieldDef;
    type Table = DojoTable;
    fn register_table(
        &mut self,
        id: Felt,
        name: &str,
        attrs: Vec<String>,
        fields: Vec<Self::Field>,
    ) -> bool {
        if self.tables.contains_key(&id) {
            return false;
        }
        let mut field_map = HashMap::new();
        let mut value_fields = Vec::new();
        let mut key_fields = Vec::new();
        for field in fields {
            match field.attrs.contains(&KEY_ATTR.to_string()) {
                true => key_fields.push(field.selector),
                false => value_fields.push(field.selector),
            }
            field_map.insert(field.selector, field);
        }
        let table = DojoTable {
            id,
            name: name.to_string(),
            attrs,
            fields: field_map,
            value_fields,
            key_fields,
        };
        self.store.dump(id, &table);
        self.tables.insert(id, table);
        true
    }

    fn update_table(
        &mut self,
        id: Felt,
        name: &str,
        _attrs: Vec<String>,
        fields: Vec<Self::Field>,
    ) -> bool {
        if !self.tables.contains_key(&id) {
            return false;
        }
        let table = match self.tables.get_mut(&id) {
            Some(t) => t,
            None => return false,
        };
        table.name = name.to_string();
        let mut record_order = Vec::new();
        for field in fields {
            record_order.push(field.selector);
            table.fields.insert(field.selector, field);
        }
        self.store.dump(id, &table);
        true
    }

    fn get_model(&self, id: Felt) -> Option<DojoModel> {
        let table = self.tables.get(&id)?;
        Some(table.get_model())
    }

    fn get_entity(&self, id: Felt) -> Option<TableFieldsDef> {
        let table = self.tables.get(&id)?;
        Some(table.get_entity())
    }

    fn get_table_name(&self, table_id: Felt) -> Option<String> {
        self.tables.get(&table_id).map(|table| table.name.clone())
    }

    fn get_table_field(&self, table_id: Felt, field_selector: Felt) -> Option<Self::Field> {
        self.tables
            .get(&table_id)
            .and_then(|table| table.fields.get(&field_selector).cloned())
    }
    fn get_table_fields(&self, table_id: Felt) -> Option<Vec<Self::Field>> {
        self.tables
            .get(&table_id)
            .map(|table| table.fields.values().cloned().collect::<Vec<_>>())
    }
}
