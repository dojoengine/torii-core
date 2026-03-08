use crate::store::DojoStoreTrait;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
use async_trait::async_trait;
use dojo_introspect::serde::dojo_primary_def;
use dojo_introspect::DojoSchema;
use introspect_types::{Attributes, PrimaryDef, PrimaryTypeDef, TableSchema};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;

pub const DOJO_ID_FIELD_NAME: &str = "entity_id";

// impl From<serde_json::Error> for DojoToriiErrors<JsonStore> {
//     fn from(err: serde_json::Error) -> Self {
//         DojoToriiErrors::StoreError(err)
//     }
// }

pub struct DojoTableStore<Store> {
    pub tables: HashMap<Felt, DojoTable>,
    pub store: Store,
}

impl<Store: DojoStoreTrait> DojoTableStore<Store> {
    pub async fn new(store: Store) -> DojoToriiResult<Self> {
        Ok(Self {
            tables: store
                .load_table_map()
                .await
                .map_err(DojoToriiError::store_error)?,
            store,
        })
    }
    pub async fn register_table(
        &mut self,
        namespace: &str,
        name: &str,
        schema: DojoSchema,
    ) -> DojoToriiResult<TableSchema> {
        let table = DojoTable::from_schema(schema, namespace, name, dojo_primary_def());
        self.save_table(&table).await?;
        if let Some(existing) = self.tables.get(&table.id) {
            return Err(DojoToriiError::TableAlreadyExists(
                table.id,
                existing.name.clone(),
                name.to_string(),
            ));
        }
        self.tables.insert(table.id, table.clone());
        Ok(table.into())
    }

    pub async fn update_table(
        &mut self,
        id: Felt,
        schema: DojoSchema,
    ) -> DojoToriiResult<TableSchema> {
        let table = match self.tables.get_mut(&id) {
            Some(t) => t,
            None => return Err(DojoToriiError::TableNotFoundById(id)),
        };
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
        self.store
            .save_table(&table)
            .await
            .map_err(DojoToriiError::store_error)?;
        Ok(table.to_schema())
    }
    pub fn get_table(&self, id: &Felt) -> DojoToriiResult<&DojoTable> {
        self.tables
            .get(id)
            .ok_or_else(|| DojoToriiError::TableNotFoundById(*id))
    }
}

#[async_trait]
impl<Store> DojoStoreTrait for DojoTableStore<Store>
where
    Store: DojoStoreTrait + Send + Sync,
    Store::Error: ToString,
{
    type Error = DojoToriiError;

    async fn save_table(&self, table: &DojoTable) -> DojoToriiResult<()> {
        self.store
            .save_table(table)
            .await
            .map_err(DojoToriiError::store_error)
    }

    async fn load_tables(&self) -> DojoToriiResult<Vec<DojoTable>> {
        self.store
            .load_tables()
            .await
            .map_err(DojoToriiError::store_error)
    }

    async fn load_table_map(&self) -> DojoToriiResult<HashMap<Felt, DojoTable>> {
        self.store
            .load_table_map()
            .await
            .map_err(DojoToriiError::store_error)
    }
}

pub fn primary_field_def() -> PrimaryDef {
    PrimaryDef {
        name: DOJO_ID_FIELD_NAME.to_string(),
        attributes: vec![],
        type_def: PrimaryTypeDef::Felt252,
    }
}
