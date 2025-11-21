use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use introspect_events::database::{IdData, IdName};
use introspect_types::schema::SchemaInfo;
use introspect_types::{
    Attribute, ColumnDef, ColumnInfo, DerefDefTrait, FeltIterator, Field, GetRefTypeDef, Primary,
    PrimaryDef, PrimaryTypeDef, Record, RecordValues, TableSchema, ToValue, TypeDef, Value,
};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use torii_types_introspect::ColumnRename;

pub enum Error {}

pub struct TableNameAnd<T> {
    pub table_name: String,
    pub value: T,
}

pub trait StoreTrait {
    fn dump_table(&self, table: &Table);

    fn load_table(&self, id: Felt) -> Option<Table>;
    fn load_all_tables(&self) -> Vec<Table>;

    fn dump_type(&self, id: Felt, type_def: &TypeDef);
    fn dump_types(&self, types: &[(Felt, TypeDef)]);

    fn load_type(&self, id: Felt) -> Option<TypeDef>;
    fn load_types(&self, ids: &[Felt]) -> Option<Vec<TypeDef>>;
    fn load_all_types(&self) -> Vec<(Felt, TypeDef)>;

    fn dump_group(&self, id: Felt, columns: &[Felt]);
    fn dump_groups(&self, groups: &[(Felt, Vec<Felt>)]);

    fn load_group(&self, id: Felt) -> Option<Vec<Felt>>;
    fn load_groups(&self, ids: &[Felt]) -> Option<Vec<Vec<Felt>>>;
    fn load_all_groups(&self) -> Vec<(Felt, Vec<Felt>)>;
}

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

pub struct ManagerInner<Store>
where
    Store: Send + Sync,
{
    pub tables: HashMap<Felt, RwLock<Arc<Table>>>,
    pub types: HashMap<Felt, TypeDef>,
    pub groups: HashMap<Felt, Vec<Felt>>,
    pub store: Store,
}

impl<Store> GetRefTypeDef for ManagerInner<Store>
where
    Store: Send + Sync,
{
    fn get_type_def(&self, id: Felt) -> Option<TypeDef> {
        if let Some(type_def_lock) = self.types.get(&id) {
            if let Ok(type_def) = type_def_lock.read() {
                return Some(type_def.clone());
            }
        }
        None
    }
}

pub struct Manager<Store>(pub RwLock<ManagerInner<Store>>)
where
    Store: Send + Sync;

impl<Store> Deref for Manager<Store>
where
    Store: Send + Sync,
{
    type Target = RwLock<ManagerInner<Store>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Store> Manager<Store>
where
    Store: StoreTrait + Send + Sync,
{
    pub fn new(store: Store) -> Self {
        Self(RwLock::new(ManagerInner {
            tables: HashMap::new(),
            types: HashMap::new(),
            groups: HashMap::new(),
            store,
        }))
    }
}

impl<Store> Manager<Store>
where
    Store: StoreTrait + Send + Sync,
{
    pub fn table(&self, id: Felt) -> Option<Arc<Table>> {
        let manager = self.read().unwrap();
        let table_lock = manager.tables.get(&id)?;
        let table = table_lock.read().unwrap();
        Some(table.clone())
    }

    pub fn modify_table<F, R>(&self, id: Felt, f: F) -> Option<TableNameAnd<R>>
    where
        F: FnOnce(&mut Table) -> Option<R>,
    {
        let manager = self.read().unwrap();
        let table_lock = manager.tables.get(&id)?;
        let mut table_ref = table_lock.write().unwrap();
        let table = Arc::make_mut(&mut table_ref);
        let result = f(table)?;
        manager.store.dump_table(&table);
        Some(TableNameAnd {
            table_name: table.name.clone(),
            value: result,
        })
    }

    pub fn schema(&self, id: Felt, columns: &[Felt]) -> Option<SchemaRef> {
        self.table(id)?.schema_ref(columns)
    }

    pub fn full_schema(&self, id: Felt) -> Option<SchemaRef> {
        self.table(id)?.full_schema_ref()
    }

    pub fn group(&self, id: Felt) -> Option<Vec<Felt>> {
        let manager = self.read().unwrap();
        manager.groups.get(&id).map(Clone::clone)
    }

    pub fn add_column_group(&self, id: Felt, columns: Vec<Felt>) {
        let mut manager = self.write().unwrap();
        manager.store.dump_group(id, &columns);
        manager.groups.insert(id, columns);
    }

    pub fn create_type_def(&mut self, id: Felt, type_def: TypeDef) {
        let mut manager = self.write().unwrap();
        manager.store.dump_type(id, &type_def);
        manager.types.insert(id, type_def);
    }

    pub fn create_table(&self, schema: TableSchema) {
        let mut manager = self.write().unwrap();
        let table: Table = schema.into();
        manager.store.dump_table(&table);
        manager
            .tables
            .insert(table.id.clone(), RwLock::new(table.into()));
    }

    fn dump_table(&self, id: Felt) -> Option<()> {
        let manager = self.read().unwrap();
        manager.store.dump_table(self.table(id)?.as_ref());
        Some(())
    }

    pub fn drop_table(&self, id: Felt) -> Option<String> {
        // TODO: implement drop in store
        let mut manager = self.write().unwrap();
    }

    pub fn rename_table(&self, id: Felt, new_name: String) -> Option<TableNameAnd<()>> {
        let rename_func = |table: &mut Table| {
            std::mem::replace(&mut table.name, new_name);
            Some(())
        };
        self.modify_table(id, rename_func)
    }

    pub fn rename_primary(
        &self,
        id: Felt,
        new_primary_name: String,
    ) -> Option<TableNameAnd<String>> {
        let rename_func = |table: &mut Table| {
            Some(std::mem::replace(
                &mut Arc::get_mut(&mut table.primary).unwrap().name,
                new_primary_name,
            ))
        };
        self.modify_table(id, rename_func)
    }

    pub fn retype_primary(
        &self,
        id: Felt,
        attributes: Vec<Attribute>,
        type_def: PrimaryTypeDef,
    ) -> Option<TableNameAnd<String>> {
        let retype_func = |table: &mut Table| {
            let primary = Arc::get_mut(&mut table.primary).unwrap();
            primary.attributes = attributes;
            primary.type_def = type_def;
            Some(primary.name.clone())
        };
        self.modify_table(id, retype_func)
    }

    pub fn add_columns(&self, id: Felt, columns: Vec<ColumnDef>) -> Option<TableNameAnd<()>> {
        let add_func = |table: &mut Table| {
            for column in columns.into_iter() {
                table.order.push(column.id);
                table.columns.insert(column.id, Arc::new(column));
            }
            Some(())
        };
        self.modify_table(id, add_func)
    }

    pub fn rename_columns(
        &self,
        id: Felt,
        renames: Vec<(Felt, String)>,
    ) -> Option<TableNameAnd<Vec<ColumnRename>>> {
        let rename_func = |table: &mut Table| {
            let mut old_names = Vec::new();
            for (id, new_name) in renames.into_iter() {
                if let Some(column) = table.columns.get(&id) {
                    let old_name = std::mem::replace(
                        &mut Arc::get_mut(&mut column.clone()).unwrap().name,
                        new_name.clone(),
                    );
                    old_names.push(ColumnRename {
                        id,
                        old_name,
                        new_name,
                    });
                }
            }

            old_names
        };
        self.modify_table(id, rename_func)
    }

    pub fn retype_columns(
        &self,
        id: Felt,
        retypes: Vec<(Felt, TypeDef)>,
    ) -> Option<TableNameAnd<Vec<ColumnDef>>> {
        let retype_func = |table: &mut Table| {
            let mut retyped_columns = Vec::new();
            for (id, new_type) in retypes.into_iter() {
                if let Some(column) = table.columns.get(&id) {
                    Arc::get_mut(&mut column.clone()).unwrap().type_def = new_type;
                    retyped_columns.push(column.deref().clone());
                }
            }
            Some(retyped_columns)
        };
        self.modify_table(id, retype_func)
    }

    pub fn drop_columns(
        &self,
        id: Felt,
        column_ids: Vec<Felt>,
    ) -> Option<TableNameAnd<Vec<IdName>>> {
        let drop_func = |table: &mut Table| {
            let mut dropped = Vec::new();
            for column_id in column_ids.iter() {
                let column = table.columns.remove(column_id)?;
                table.order.retain(|cid| cid != column_id);
                dropped.push(IdName {
                    id: column_id.clone(),
                    name: column.name.clone(),
                });
            }
            Some(dropped)
        };
        self.modify_table(id, drop_func)
    }
}

impl From<TableSchema> for Table {
    fn from(schema: TableSchema) -> Self {
        let order = schema.columns.iter().map(|col| col.id.clone()).collect();
        let columns = schema
            .columns
            .into_iter()
            .map(|col| (col.id.clone(), Arc::new(col)))
            .collect();

        Self {
            id: schema.id,
            name: schema.name,
            attributes: Arc::new(schema.attributes),
            primary: Arc::new(schema.primary),
            columns,
            order,
            alive: true,
        }
    }
}

impl Table {
    pub fn column(&self, id: Felt) -> Option<Arc<ColumnDef>> {
        self.columns.get(&id).cloned()
    }
    pub fn columns<T: AsRef<[Felt]>>(&self, ids: T) -> Option<Vec<Arc<ColumnDef>>> {
        ids.as_ref().iter().map(|id| self.column(*id)).collect()
    }

    pub fn all_columns(&self) -> Option<Vec<Arc<ColumnDef>>> {
        self.columns(&self.order)
    }

    pub fn column_info(&self, id: Felt) -> Option<ColumnInfo> {
        self.column(id).map(From::from)
    }

    pub fn columns_info<T: AsRef<[Felt]>>(&self, ids: T) -> Option<Vec<ColumnInfo>> {
        ids.as_ref()
            .iter()
            .map(|id| self.column_info(*id))
            .collect()
    }

    pub fn schema_ref(&self, ids: &[Felt]) -> Option<SchemaRef> {
        Some(SchemaRef {
            id: self.id.clone(),
            name: self.name.clone(),
            attributes: self.attributes.clone(),
            primary: self.primary.clone(),
            columns: ids
                .iter()
                .map(|id| self.columns.get(id).cloned())
                .collect::<Option<Vec<_>>>()?,
        })
    }

    pub fn full_schema_ref(&self) -> Option<SchemaRef> {
        self.schema_ref(&self.order)
    }
}

pub struct SchemaRef {
    pub id: Felt,
    pub name: String,
    pub attributes: Arc<Vec<Attribute>>,
    pub primary: Arc<PrimaryDef>,
    pub columns: Vec<Arc<ColumnDef>>,
}

impl SchemaRef {
    pub fn to_record<'a>(&self, primary: Felt, data: Vec<Felt>) -> Option<Record> {
        let mut data = data.into_iter();
        Some(Record {
            table_id: self.id.clone(),
            table_name: self.name.clone(),
            attributes: self.attributes.deref().clone(),
            primary: self.primary.to_primary(primary)?,
            fields: self.columns.to_value(&mut data)?,
        })
    }

    pub fn to_record_values(&self, primary: Felt, data: Vec<Felt>) -> Option<RecordValues> {
        let mut data = data.into_iter();
        Some(RecordValues {
            primary: self.primary.to_primary_value(primary)?,
            fields: self
                .columns
                .iter()
                .map(|col| col.type_def.to_value(&mut data))
                .collect::<Option<Vec<_>>>()?,
        })
    }

    pub fn to_records_values(&self, id_datas: Vec<IdData>) -> Option<Vec<RecordValues>> {
        id_datas
            .into_iter()
            .map(|id_data| self.to_record_values(id_data.id, id_data.data))
            .collect()
    }

    pub fn to_info(&self) -> SchemaInfo {
        SchemaInfo {
            table_id: self.id.clone(),
            table_name: self.name.clone(),
            attributes: self.attributes.deref().clone(),
            primary: self.primary.to_primary_info(),
            columns: self.columns.iter().map(ColumnInfo::from).collect(),
        }
    }
}
