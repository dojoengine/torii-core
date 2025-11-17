use std::collections::HashMap;
use std::ops::Deref;
use std::sync::RwLock;

use introspect_types::{
    Attribute, ColumnDef, Field, Primary, PrimaryDef, Record, RecordValues, TableSchema, ToValue, TypeDef, Value
};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Table {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnDef>,
    pub order: Vec<Felt>,
}

pub struct ManagerInner<Store>
where
    Store: Send + Sync,
{
    pub tables: HashMap<Felt, RwLock<Table>>,
    pub types: HashMap<Felt, RwLock<TypeDef>>,
    pub groups: HashMap<Felt, RwLock<Vec<Felt>>>,
    pub store: Store,
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

impl From<TableSchema> for Table {
    fn from(schema: TableSchema) -> Self {
        let columns = schema
            .columns
            .into_iter()
            .map(|col| (col.id.clone(), col))
            .collect();
        let order = schema.columns.into_iter().map(|col| col.id).collect();
        Self {
            id: schema.id,
            name: schema.name,
            attributes: schema.attributes,
            primary: schema.primary,
            columns,
            order,
        }
    }
}

impl Into<TableSchema> for Table {
    fn into(self) -> TableSchema {
        let columns = self
            .order
            .into_iter()
            .filter_map(|id| self.columns.get(&id).cloned())
            .collect();
        TableSchema {
            id: self.id,
            name: self.name,
            attributes: self.attributes,
            primary: self.primary,
            columns,
        }
    }
}

impl Table {
    pub fn get_column(&self, id: Felt) -> Option<ColumnDef> {
        self.get_column_ref(id).cloned()
    }

    pub fn get_column_ref<'a>(&'a self, id: Felt) -> Option<&'a ColumnDef> {
        self.columns.get(&id)
    }

    pub fn get_columns<T: AsRef<[Felt]>>(&self, ids: T) -> Option<Vec<ColumnDef>> {
        ids.as_ref().iter().map(|id| self.get_column(*id)).collect()
    }

    pub fn get_all_columns(&self) -> Option<Vec<ColumnDef>> {
        self.get_columns(&self.order)
    }

    pub fn get_all_columns_ref<'a>(&'a self) -> Option<Vec<&'a ColumnDef>> {
        self.get_columns_ref(&self.order)
    }

    pub fn get_columns_ref<'a, T: AsRef<[Felt]>>(&'a self, ids: T) -> Option<Vec<&'a ColumnDef>> {
        ids.as_ref()
            .iter()
            .map(|id| self.get_column_ref(*id))
            .collect()
    }

    pub fn parse_columns<T: AsRef<[Felt]>>(
        &self,
        columns: T,
        data: &mut impl Iterator<Item = Felt>,
    ) -> Option<Vec<Field>> {
        columns
            .as_ref()
            .iter()
            .map(|id| self.get_column_ref(*id)?.to_value(data))
            .collect()
    }

    pub fn parse_primary(&self, felt: Felt) -> Option<Primary> {
        self.primary.to_primary(felt)
    }

    pub fn parse_record<T: AsRef<[Felt]>>(
        &self,
        columns: T,
        primary: Felt,
        data: &mut impl Iterator<Item = Felt>,
    ) -> Option<Record> {
        Some(Record {
            table_id: self.id.clone(),
            table_name: self.name.clone(),
            attributes: self.attributes.clone(),
            primary: self.parse_primary(primary)?,
            fields: self.parse_columns(columns, data)?,
        })
    }

    pub fn parse_full_record(
        &self,
        primary: Felt,
        data: &mut impl Iterator<Item = Felt>,
    ) -> Option<Record> {
        self.parse_record(&self.order, primary, data)
    }

    pub fn parse_record_values(
        &self,
        primary: Felt,
        data: &mut impl Iterator<Item = Felt>,
    ) -> Option<RecordValues> {
        Some(RecordValues {
            primary: self.primary.to_primary_value(primary)?,
            fields: self
                .order
                .iter()
                .map(|id| self.get_column_ref(*id)?.to_value(data))
                .collect::<Option<Vec<Value>>>()?,
        })
    }
}

impl<Store> ManagerInner<Store>
where
    Store: Send + Sync,
{
    fn parse_record(
        &self,
        table_id: Felt,
        primary: Felt,
        data: &mut impl Iterator<Item = Felt>,
    ) -> Option<Record> {
        let table = self.tables.get(&table_id)?.read().ok()?;
        table.parse_record(primary, data)
    }

    fn parse
}
