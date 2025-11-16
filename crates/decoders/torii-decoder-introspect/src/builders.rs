use crate::IntrospectDecoder;
use anyhow::anyhow;
use dojo_introspect_events::{
    DojoEvent, EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use dojo_introspect_types::contract::DojoSchemaResponse;
use dojo_introspect_types::{DojoSchema, DojoSchemaFetcher, DojoTypeDefSerde, make_dojo_table};
use dojo_types_manager::{DojoManagerError, DojoTable, DojoTableErrors};
use introspect_events::types::TableSchema;
use introspect_types::{FieldDef, PrimaryDef, PrimaryTypeDef, StructDef, TypeDef};
use introspect_value::{Field, Value};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use torii_core::{Envelope, Event};
use torii_types_introspect::{
    DeclareTableV1, DeleteRecordsV1, UpdateRecordFieldsV1, UpdateTableV1,
};

fn declare_from_schema(schema: TableSchema) -> DeclareTableV1 {
    DeclareTableV1 {
        id: schema.table_id,
        name: schema.table_name,
        attributes: schema.attributes,
        primary: primary_field_def(),
        columns: schema.fields,
    }
}

fn update_from_schema(schema: TableSchema) -> UpdateTableV1 {
    UpdateTableV1 {
        id: schema.table_id,
        name: schema.table_name,
        attributes: schema.attributes,
        primary: primary_field_def(),
        fields: schema.fields,
    }
}

fn make_entity_id_field(entity_id: Felt) -> Field {
    Field {
        attributes: vec![],
        name: DOJO_ID_FIELD_NAME.to_string(),
        value: Value::Felt252(entity_id),
    }
}

fn make_entity_id_for_event(keys: &Vec<Felt>) -> Field {
    let mut hasher = DefaultHasher::new();
    keys.hash(&mut hasher);
    let entity_id = hasher.finish();
    make_entity_id_field(entity_id.into())
}

use thiserror::Error;
#[derive(Debug, Error)]
pub enum DojoEventBuilderError {
    #[error("Failed to decode DojoEvent: {0}")]
    RawEventDecodeError(String),
    #[error("Manager error: {0}")]
    ManagerError(#[from] DojoManagerError),
    #[error("Table Error: {0}")]
    TableError(#[from] DojoTableErrors),
    #[error("Schema fetch error: {0}")]
    SchemaFetchError(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, DojoEventBuilderError>;

pub trait DojoEventBuilder {
    async fn build_model_registered(&self, raw: &EmittedEvent) -> Result<Envelope>;
    async fn build_model_with_schema_registered(&self, raw: &EmittedEvent) -> Result<Envelope>;
    async fn build_model_upgraded(&self, raw: &EmittedEvent) -> Result<Envelope>;
    async fn build_event_registered(&self, raw: &EmittedEvent) -> Result<Envelope>;
    async fn build_event_upgraded(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_set_record(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_update_record(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_update_member(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_del_record(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_emit_event(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn with_table<F, R>(&self, id: Felt, f: F) -> Result<R>
    where
        F: FnOnce(&DojoTable) -> Result<R>;
}

fn raw_event_to_event<T>(raw: &EmittedEvent) -> Result<T>
where
    T: DojoEvent,
{
    DojoEvent::new(raw.keys[1..].to_vec(), raw.data.clone())
        .ok_or_else(|| DojoEventBuilderError::RawEventDecodeError(T::NAME.to_string()))
}

impl<F> DojoEventBuilder for IntrospectDecoder<F>
where
    F: DojoSchemaFetcher + Sync + Send,
{
    fn with_table<Fn, R>(&self, id: Felt, f: Fn) -> Result<R>
    where
        Fn: FnOnce(&DojoTable) -> Result<R>,
    {
        self.manager.with_table(id, f)?
    }

    async fn build_model_registered(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<ModelRegistered>(raw)?;
        let DojoSchemaResponse { legacy, data } = self.fetcher.schema(event.address).await?;
        let schema = make_dojo_table(&event.namespace, &event.name, data, legacy)?;
        self.manager.register_table(schema)?;
        Ok(declare_from_schema(schema).to_envelope(raw))
    }

    async fn build_model_with_schema_registered(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<ModelWithSchemaRegistered>(raw)?;
        let schema = DojoSchema::dojo_deserialize(&mut event.schema.into_iter(), true)
            .expect("Could not decode schema");
        let table = self
            .manager
            .register_table(&event.namespace, &event.name, schema)?;
        Ok(declare_from_schema(table).to_envelope(raw))
    }

    async fn build_model_upgraded(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<ModelUpgraded>(raw)?;
        let dojo_schema = self.fetcher.schema(event.address).await?;
        let table = self.manager.update_table(event.selector, dojo_schema)?;
        Ok(update_from_schema(table).to_envelope(raw))
    }

    async fn build_event_registered(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<EventRegistered>(raw)?;
        let struct_def = self.fetcher.schema(event.address).await?;
        let schema = make_dojo_table(&event.namespace, &event.name, event.schema, event.legacy)?;

        let schema = self.manager.register_table(
            event.namespace,
            event.name,
            struct_def.attributes,
            struct_def.fields,
        )?;
        Ok(declare_from_schema(schema).to_envelope(raw))
    }

    async fn build_event_upgraded(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<EventUpgraded>(raw)?;
        let struct_def = self.fetcher.schema(event.address).await?;
        let schema = self
            .manager
            .update_table(event.selector, struct_def.fields)?;
        Ok(update_from_schema(schema).to_envelope(raw))
    }

    fn build_set_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreSetRecord>(raw)?;
        let id_field = make_entity_id_field(event.entity_id);
        let (table_id, table_name, fields) = self.with_table(event.selector, |table| {
            let fields = table.parse_key_values(event.keys.clone(), event.values.clone())?;
            Ok((table.id, table.name.clone(), fields))
        })?;
        let data = UpdateRecordFieldsV1::new(table_id, table_name, id_field, fields);
        Ok(data.to_envelope(raw))
    }

    fn build_update_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreUpdateRecord>(raw)?;
        let id_field = make_entity_id_field(event.entity_id);
        let (table_id, table_name, fields) = self.with_table(event.selector, |table| {
            let fields = table.parse_values(event.values)?;
            Ok((table.id, table.name.clone(), fields))
        })?;
        let data = UpdateRecordFieldsV1::new(table_id, table_name, id_field, fields);
        Ok(data.to_envelope(raw))
    }

    fn build_update_member(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreUpdateMember>(raw)?;
        let id_field = make_entity_id_field(event.entity_id);
        let (table_id, table_name, field) = self.with_table(event.selector, |table| {
            let field = table.parse_field(event.member_selector, event.values)?;
            Ok((table.id, table.name.clone(), field))
        })?;

        let data = UpdateRecordFieldsV1::new(table_id, table_name, id_field, vec![field]);
        Ok(data.to_envelope(raw))
    }

    fn build_del_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreDelRecord>(raw)?;
        let (table_id, table_name) =
            self.with_table(event.selector, |table| Ok((table.id, table.name.clone())))?;
        let data = DeleteRecordsV1::new(
            table_id,
            table_name,
            DOJO_ID_FIELD_NAME.to_string(),
            vec![Value::Felt252(event.entity_id)],
        );
        Ok(data.to_envelope(raw))
    }

    fn build_emit_event(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<EventEmitted>(raw)?;
        let id_field = make_entity_id_for_event(&event.keys);
        let (table_id, table_name, fields) = self.with_table(event.selector, |table| {
            let fields = match table.parse_key_values(event.keys.clone(), event.values.clone()) {
                Ok(fields) => fields,
                Err(e) => {
                    println!(
                        "\nError Parsing keys: {:#?} values: {:#?}",
                        event.keys, event.values
                    );
                    println!("\nError Parsing table: {table:#?}");
                    return Err(DojoEventBuilderError::TableError(e));
                }
            };
            Ok((table.id, table.name.clone(), fields))
        })?;
        let data = UpdateRecordFieldsV1::new(table_id, table_name, id_field, fields);
        Ok(data.to_envelope(raw))
    }
}
