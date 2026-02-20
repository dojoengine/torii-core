use crate::{DojoTable, DojoToriiError, DojoToriiResult};
use dojo_introspect_types::DojoSchema;
use introspect_types::schema::PrimaryInfo;
use introspect_types::{Attributes, PrimaryDef, PrimaryTypeDef, TableSchema};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fs;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::RwLock;

pub const DOJO_ID_FIELD_NAME: &str = "entity_id";

// impl From<serde_json::Error> for DojoToriiErrors<JsonStore> {
//     fn from(err: serde_json::Error) -> Self {
//         DojoToriiErrors::StoreError(err)
//     }
// }

pub struct DojoManagerInner<Store>
where
    Store: Send + Sync,
{
    pub tables: HashMap<Felt, RwLock<DojoTable>>,
    pub store: Store,
}

pub struct DojoTableStore<Store>(pub RwLock<DojoManagerInner<Store>>)
where
    Store: Send + Sync;

impl<Store> Deref for DojoTableStore<Store>
where
    Store: Send + Sync,
{
    type Target = RwLock<DojoManagerInner<Store>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn primary_field_def() -> PrimaryDef {
    PrimaryDef {
        name: DOJO_ID_FIELD_NAME.to_string(),
        attributes: vec![],
        type_def: PrimaryTypeDef::Felt252,
    }
}

pub fn primary_field_info() -> PrimaryInfo {
    PrimaryInfo {
        name: DOJO_ID_FIELD_NAME.to_string(),
        attributes: vec![],
    }
}

impl<Store> DojoManagerInner<Store>
where
    Store: StoreTrait<Table = DojoTable> + Send + Sync + Sized + 'static,
{
    pub fn new(store: Store) -> DojoToriiResult<Self> {
        let tables = store
            .load_all()
            .map_err(|e| DojoToriiError::StoreError(e.to_string()))?
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

pub trait DojoTableManager {
    fn register_table(
        &self,
        namespace: &str,
        name: &str,
        schema: DojoSchema,
    ) -> DojoToriiResult<TableSchema>;
    fn update_table(&self, id: Felt, schema: DojoSchema) -> DojoToriiResult<TableSchema>;
    fn with_table<F, R>(&self, id: Felt, f: F) -> DojoToriiResult<R>
    where
        F: FnOnce(&DojoTable) -> R;
}

impl<Store> DojoTableManager for DojoTableStore<Store>
where
    Store: StoreTrait<Table = DojoTable> + Send + Sync,
{
    fn register_table(
        &self,
        namespace: &str,
        name: &str,
        schema: DojoSchema,
    ) -> DojoToriiResult<TableSchema> {
        let table = schema.to_table_schema(namespace, name);
        if self
            .read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?
            .tables
            .contains_key(&table.id)
        {
            return Err(DojoToriiError::TableAlreadyExists(table.id));
        }
        let dojo_table: DojoTable = table.clone().into();

        let mut manager = self
            .write()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?;
        manager
            .store
            .dump(table.id, &dojo_table)
            .map_err(|e| DojoToriiError::StoreError(e.to_string()))?;
        manager.tables.insert(table.id, RwLock::new(dojo_table));
        Ok(table)
    }

    fn update_table(&self, id: Felt, schema: DojoSchema) -> DojoToriiResult<TableSchema> {
        let manager = self
            .read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?;
        let mut table = match manager.tables.get(&id) {
            Some(t) => t
                .write()
                .map_err(|e| DojoToriiError::LockError(e.to_string())),
            None => return Err(DojoToriiError::TableNotFoundById(id)),
        }?;
        let mut key_fields = Vec::new();
        let mut value_fields = Vec::new();
        for column in schema.columns {
            match column.has_attribute("key") {
                true => key_fields.push(column.id),
                false => value_fields.push(column.id),
            }
            table.columns.insert(column.id, column);
        }
        table.key_fields = key_fields;
        table.value_fields = value_fields;
        self.read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?
            .store
            .dump(id, &table)
            .map_err(|e| DojoToriiError::StoreError(e.to_string()))?;
        Ok(table.to_schema())
    }

    fn with_table<F, R>(&self, id: Felt, f: F) -> DojoToriiResult<R>
    where
        F: FnOnce(&DojoTable) -> R,
    {
        let manager = self
            .read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?;
        let table = manager
            .tables
            .get(&id)
            .ok_or_else(|| DojoToriiError::TableNotFoundById(id))?;
        let table_guard = table
            .read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?;
        Ok(f(&*table_guard))
    }
}
