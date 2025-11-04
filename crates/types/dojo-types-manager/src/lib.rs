use dojo_introspect_utils::selector::compute_selector_from_namespace_and_name;
use introspect_events::types::TableSchema;
use introspect_types::{ColumnDef, FieldDef};
use introspect_value::{FeltIterator, Field, ToValue};
use serde::{Deserialize, Serialize};
use starknet::core::utils::{get_selector_from_name, NonAsciiNameError};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fs;

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::RwLock;
use thiserror::Error;

const KEY_ATTR: &str = "key";

#[derive(Debug, Error)]
pub enum DojoTableErrors {
    #[error("Field {0} not found in table {1}")]
    FieldNotFound(Felt, String),
    #[error("Failed to parse field {0} in table {1}")]
    FieldParseError(Felt, String),
    #[error("Too many values provided for field {0}")]
    TooManyFieldValues(Felt),
    #[error("Failed to parse values for table {0}")]
    ParseValuesError(String),
}

#[derive(Debug, Error)]
pub enum DojoManagerError {
    #[error("Table Error")]
    TableError(#[from] DojoTableErrors),
    #[error("Table already exists with id {0}")]
    TableAlreadyExists(Felt),
    #[error("Table not found with id {0}")]
    TableNotFoundById(Felt),
    #[error("Failed to acquire lock: {0}")]
    LockError(String),
    #[error("Store error: {0}")]
    StoreError(String),
    #[error("Starknet selector error: {0}")]
    StarknetSelectorError(#[from] NonAsciiNameError),
    #[error("Lock poisoned: {0}")]
    LockPoisoned(String),
}

// impl From<serde_json::Error> for DojoManagerErrors<JsonStore> {
//     fn from(err: serde_json::Error) -> Self {
//         DojoManagerErrors::StoreError(err)
//     }
// }

pub type TableResult<T> = std::result::Result<T, DojoTableErrors>;
pub type ManagerResult<T> = std::result::Result<T, DojoManagerError>;
pub struct DojoManagerInner<Store>
where
    Store: Send + Sync,
{
    pub tables: HashMap<Felt, RwLock<DojoTable>>,
    pub store: Store,
}

pub struct DojoManager<Store>(pub RwLock<DojoManagerInner<Store>>)
where
    Store: Send + Sync;

impl<Store> Deref for DojoManager<Store>
where
    Store: Send + Sync,
{
    type Target = RwLock<DojoManagerInner<Store>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
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

    pub fn parse_keys(&self, keys: Vec<Felt>) -> TableResult<Vec<Field>> {
        let mut keys = keys.into_iter();
        let values = self
            .key_fields
            .iter()
            .map(|selector| self.fields.get(selector)?.to_value(&mut keys))
            .collect::<Option<Vec<_>>>();
        match keys.next() {
            None => {
                values.ok_or_else(|| DojoTableErrors::ParseValuesError(self.name.clone()).into())
            }
            _ => Err(DojoTableErrors::ParseValuesError(self.name.clone()).into()),
        }
    }

    pub fn parse_values(&self, values: Vec<Felt>) -> TableResult<Vec<Field>> {
        dbg!(&values);

        let mut values = values.into_iter();
        let vals = self
            .value_fields
            .iter()
            .map(|selector| self.fields.get(selector)?.to_value(&mut values))
            .collect::<Option<Vec<_>>>();

        dbg!(&vals);

        match values.next() {
            None => vals.ok_or_else(|| DojoTableErrors::ParseValuesError(self.name.clone()).into()),
            Some(v) => {
                dbg!(&v);
                Err(DojoTableErrors::ParseValuesError(self.name.clone()).into())
            }
        }
    }

    pub fn parse_key_values(&self, keys: Vec<Felt>, values: Vec<Felt>) -> TableResult<Vec<Field>> {
        let mut k = self.parse_keys(keys)?;
        let mut v = self.parse_values(values)?;
        k.append(&mut v);
        Ok(k)
    }

    pub fn parse_field(&self, selector: Felt, data: Vec<Felt>) -> TableResult<Field> {
        let mut data = data.into_iter();
        let field_def = self
            .fields
            .get(&selector)
            .ok_or_else(|| DojoTableErrors::FieldNotFound(selector, self.name.clone()))?;
        let field = field_def
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
                let field_def = self.fields.get(selector)?;
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

impl<Store> DojoManagerInner<Store>
where
    Store: StoreTrait<Table = DojoTable> + Send + Sync + Sized + 'static,
{
    pub fn new(store: Store) -> ManagerResult<Self> {
        let tables = store
            .load_all()
            .map_err(|e| DojoManagerError::StoreError(e.to_string()))?
            .into_iter()
            .map(|(id, table)| (id, RwLock::new(table)))
            .collect();
        Ok(Self { tables, store })
    }
}

pub struct JsonStore {
    pub path: PathBuf,
}

impl JsonStore {
    pub fn new(path: &PathBuf) -> Self {
        if !path.exists() {
            std::fs::create_dir_all(path).expect("Unable to create directory");
        }

        // A temporary flag to clean the store on start, useful when debugging
        // to avoid existing table error.
        // TODO: @bengineer42 can be removed or kept being configurable.
        let clean_on_start = true;
        if clean_on_start {
            std::fs::remove_dir_all(path).expect("Unable to clean directory");
            std::fs::create_dir_all(path).expect("Unable to create directory");
        }

        Self {
            path: path.to_path_buf(),
        }
    }
}
pub trait StoreTrait
where
    Self: Send + Sync + 'static + Sized,
{
    type Table;
    type Error: std::error::Error;

    fn dump(&self, table_id: Felt, data: &Self::Table) -> Result<(), Self::Error>;
    fn load(&self, table_id: Felt) -> Result<Self::Table, Self::Error>;
    fn load_all(&self) -> Result<Vec<(Felt, Self::Table)>, Self::Error>;
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
    type Error = serde_json::Error;
    fn dump(&self, table_id: Felt, data: &Self::Table) -> Result<(), Self::Error> {
        let file_path = self.path.join(felt_to_json_file_name(&table_id));
        std::fs::write(file_path, serde_json::to_string_pretty(data).unwrap())
            .expect("Unable to write file");
        Ok(())
    }

    fn load(&self, table_id: Felt) -> Result<Self::Table, Self::Error> {
        let file_path = self.path.join(felt_to_json_file_name(&table_id));
        let data = std::fs::read_to_string(file_path).expect("Unable to read file");
        Ok(serde_json::from_str(&data)?)
    }

    fn load_all(&self) -> Result<Vec<(Felt, Self::Table)>, Self::Error> {
        let mut tables: Vec<(Felt, Self::Table)> = Vec::new();
        let paths = fs::read_dir(&self.path).unwrap();
        for path in paths {
            let path = path.unwrap().path();
            let table_id = path
                .file_name()
                .and_then(|p| json_file_name_to_felt(p.to_str()?));
            let data: Option<DojoTable> =
                serde_json::from_str(&fs::read_to_string(&path).unwrap()).ok();
            match (table_id, data) {
                (Some(id), Some(table)) => {
                    tables.push((id, table));
                }
                _ => {}
            }
        }
        Ok(tables)
    }
}

impl<Store> DojoManager<Store>
where
    Store: StoreTrait<Table = DojoTable> + Send + Sync,
{
    pub fn new(store: Store) -> ManagerResult<Self> {
        Ok(Self(RwLock::new(DojoManagerInner::new(store)?)))
    }
    pub fn register_table(
        &self,
        namespace: String,
        name: String,
        attrs: Vec<String>,
        fields: Vec<FieldDef>,
    ) -> ManagerResult<TableSchema> {
        let id = compute_selector_from_namespace_and_name(&namespace, &name);
        let table_name = format!("{namespace}-{name}");

        if self
            .read()
            .map_err(|e| DojoManagerError::LockError(e.to_string()))?
            .tables
            .contains_key(&id)
        {
            return Err(DojoManagerError::TableAlreadyExists(id));
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
        let mut manager = self
            .write()
            .map_err(|e| DojoManagerError::LockError(e.to_string()))?;
        manager
            .store
            .dump(id, &table)
            .map_err(|e| DojoManagerError::StoreError(e.to_string()))?;
        manager.tables.insert(id, RwLock::new(table));
        Ok(schema)
    }

    pub fn update_table(&self, id: Felt, fields: Vec<FieldDef>) -> ManagerResult<TableSchema> {
        let manager = self
            .read()
            .map_err(|e| DojoManagerError::LockError(e.to_string()))?;
        let mut table = match manager.tables.get(&id) {
            Some(t) => t
                .write()
                .map_err(|e| DojoManagerError::LockError(e.to_string())),
            None => return Err(DojoManagerError::TableNotFoundById(id)),
        }?;
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
        self.read()
            .map_err(|e| DojoManagerError::LockError(e.to_string()))?
            .store
            .dump(id, &table)
            .map_err(|e| DojoManagerError::StoreError(e.to_string()))?;
        Ok(table.schema())
    }

    pub fn with_table<F, R>(&self, id: Felt, f: F) -> ManagerResult<R>
    where
        F: FnOnce(&DojoTable) -> R,
    {
        let manager = self
            .read()
            .map_err(|e| DojoManagerError::LockError(e.to_string()))?;
        let table = manager
            .tables
            .get(&id)
            .ok_or_else(|| DojoManagerError::TableNotFoundById(id))?;
        let table_guard = table
            .read()
            .map_err(|e| DojoManagerError::LockError(e.to_string()))?;
        Ok(f(&*table_guard))
    }
}
