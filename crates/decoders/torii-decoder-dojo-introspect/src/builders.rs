use crate::DojoIntrospectDecoder;
use dojo_introspect_events::{
    DojoEvent, EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use dojo_introspect_types::{DojoSchema, DojoSchemaFetcher, DojoTypeDefSerde};
use dojo_types_manager::{
    DOJO_ID_FIELD_NAME, DojoManagerError, DojoTable, DojoTableErrors, primary_field_info,
};
use introspect_types::{Primary, PrimaryValue};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use torii_core::{Envelope, Event};
use torii_types_introspect::{
    CreateTableV1, DeleteRecordsV1, IdValue, InsertFieldsV1, UpdateTableV1,
};

fn make_entity_id_value(entity_id: Felt) -> PrimaryValue {
    PrimaryValue::Felt252(entity_id)
}

fn make_entity_id_for_event(keys: &Vec<Felt>) -> Primary {
    let mut hasher = DefaultHasher::new();
    keys.hash(&mut hasher);
    let entity_id = hasher.finish();
    make_entity_id_value(entity_id.into())
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

impl<F> DojoEventBuilder for DojoIntrospectDecoder<F>
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
        let schema = self.fetcher.schema(event.address).await?;
        let table = self
            .manager
            .register_table(&event.namespace, &event.name, schema)?;
        Ok(Into::<CreateTableV1>::into(table).to_envelope(raw))
    }

    async fn build_model_with_schema_registered(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<ModelWithSchemaRegistered>(raw)?;
        let schema = DojoSchema::dojo_deserialize(&mut event.schema.into_iter(), true)
            .expect("Could not decode schema");
        let table = self
            .manager
            .register_table(&event.namespace, &event.name, schema)?;
        Ok(Into::<CreateTableV1>::into(table).to_envelope(raw))
    }

    async fn build_model_upgraded(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<ModelUpgraded>(raw)?;
        let dojo_schema = self.fetcher.schema(event.address).await?;
        let table = self.manager.update_table(event.selector, dojo_schema)?;
        Ok(Into::<UpdateTableV1>::into(table).to_envelope(raw))
    }

    async fn build_event_registered(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<EventRegistered>(raw)?;
        let schema = self.fetcher.schema(event.address).await?;
        let table = self
            .manager
            .register_table(&event.namespace, &event.name, schema)?;
        Ok(Into::<CreateTableV1>::into(table).to_envelope(raw))
    }

    async fn build_event_upgraded(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<EventUpgraded>(raw)?;
        let schema = self.fetcher.schema(event.address).await?;
        let table = self.manager.update_table(event.selector, schema)?;
        Ok(Into::<UpdateTableV1>::into(table).to_envelope(raw))
    }

    fn build_set_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreSetRecord>(raw)?;
        let id_field = make_entity_id_value(event.entity_id);
        let (table_id, table_name, fields) = self.with_table(event.selector, |table| {
            let fields = table.parse_key_values(event.keys.clone(), event.values.clone())?;
            Ok((table.id, table.name.clone(), fields))
        })?;
        let data = InsertFieldsV1::new(table_id, PrimaryValue::Felt252(event.entity_id), fields);
        data.to_ok_envelope(raw)
    }

    fn build_update_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreUpdateRecord>(raw)?;
        let id_field = make_entity_id_value(event.entity_id);
        let fields = self.with_table(event.selector, |table| table.parse_values(event.values))?;
        let data = InsertFieldsV1::new(
            event.selector,
            PrimaryValue::Felt252(event.entity_id),
            fields,
        );
        data.to_ok_envelope(raw)
    }

    fn build_update_member(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreUpdateMember>(raw)?;
        let field = self.with_table(event.selector, |table| {
            table.parse_field(event.member_selector, event.values)
        })?;

        let data = InsertFieldsV1::new(
            event.selector,
            PrimaryValue::Felt252(event.entity_id),
            vec![field],
        );
        data.to_ok_envelope(raw)
    }

    fn build_del_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreDelRecord>(raw)?;
        let (table_id, table_name) =
            self.with_table(event.selector, |table| Ok((table.id, table.name.clone())))?;
        let data = DeleteRecordsV1::new(table_id, vec![PrimaryValue::Felt252(event.entity_id)]);
        data.to_ok_envelope(raw)
    }

    fn build_emit_event(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<EventEmitted>(raw)?;
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
            Ok(fields)
        })?;
        let data = InsertFieldsV1::new(
            event.selector,
            PrimaryValue::Felt252(event.entity_id),
            fields,
        );
        data.to_ok_envelope(raw)
    }
}
