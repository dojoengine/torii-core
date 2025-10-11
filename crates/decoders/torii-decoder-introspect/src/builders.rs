use crate::IntrospectDecoder;
use anyhow::{anyhow, Result};
use dojo_introspect_events::{
    DojoEvent, EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    StoreDelRecord, StoreSetRecord, StoreUpdateMember, StoreUpdateRecord,
};
use dojo_introspect_types::DojoSchemaFetcher;
use introspect_value::{Field, Value};
use starknet::{core::types::EmittedEvent, providers::Provider};
use starknet_types_core::felt::Felt;
use std::sync::Arc;
use torii_core::StaticEvent;
use torii_core::{Body, Envelope, Event};
use torii_types_introspect::{DeclareTableV1, DeleteRecordsV1, UpdateRecordFieldsV1};
const DOJO_ID_FIELD_NAME: &str = "entity_id";

fn make_entity_id_field(entity_id: Felt) -> Field {
    Field {
        attrs: vec![],
        name: DOJO_ID_FIELD_NAME.to_string(),
        value: Value::Felt252(entity_id),
    }
}

pub trait DojoEventBuilder {
    async fn build_model_registered(&mut self, raw: &EmittedEvent) -> Result<Envelope>;
    async fn build_model_upgraded(&mut self, raw: &EmittedEvent) -> Result<Envelope>;
    async fn build_event_registered(&mut self, raw: &EmittedEvent) -> Result<Envelope>;
    async fn build_event_upgraded(&mut self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_set_record(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_update_record(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_update_member(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_del_record(&self, raw: &EmittedEvent) -> Result<Envelope>;
    fn build_emit_event(&self, raw: &EmittedEvent) -> Result<Envelope>;
}

fn raw_event_to_event<T>(raw: &EmittedEvent) -> Result<T>
where
    T: DojoEvent,
{
    DojoEvent::new(raw.keys[1..].to_vec(), raw.data.clone())
        .ok_or_else(|| anyhow!("failed to decode DojoEvent"))
}

impl<P> DojoEventBuilder for IntrospectDecoder<P>
where
    P: Provider,
{
    async fn build_model_registered(&mut self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<ModelRegistered>(raw)?;
        let struct_def = self.provider.schema(event.address).await?;

        let schema = self.manager.register_table(
            event.namespace,
            event.name,
            struct_def.attrs,
            struct_def.fields,
        )?;
        let data = DeclareTableV1 {
            id: schema.table_id,
            name: schema.table_name,
            attrs: schema.attrs,
            id_field: DOJO_ID_FIELD_NAME.to_string(),
            fields: schema.fields,
        };
        Ok(data.to_envelope(raw))
    }

    async fn build_model_upgraded(&mut self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<ModelUpgraded>(raw)?;
        unimplemented!()
    }

    async fn build_event_registered(&mut self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<EventRegistered>(raw)?;
        unimplemented!()
    }

    async fn build_event_upgraded(&mut self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<EventUpgraded>(raw)?;
        unimplemented!()
    }

    fn build_set_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreSetRecord>(raw)?;
        let table = self
            .manager
            .get_table(event.selector)
            .ok_or_else(|| anyhow!("no table found for selector {}", event.selector.to_string()))?;
        let fields = table
            .parse_key_values(event.keys, event.values)
            .ok_or_else(|| {
                anyhow!(
                    "failed to parse key/values for table {}",
                    table.name.clone()
                )
            })?;
        let id_field = make_entity_id_field(event.entity_id);
        let data = UpdateRecordFieldsV1::new(table.id, table.name.clone(), id_field, fields);
        Ok(data.to_envelope(raw))
    }

    fn build_update_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreUpdateRecord>(raw)?;
        let table = self
            .manager
            .get_table(event.selector)
            .ok_or_else(|| anyhow!("no table found for selector {}", event.selector.to_string()))?;
        let fields = table
            .parse_values(event.values)
            .ok_or_else(|| anyhow!("failed to parse values for table {}", table.name.clone()))?;
        let id_field = make_entity_id_field(event.entity_id);
        let data = UpdateRecordFieldsV1::new(table.id, table.name.clone(), id_field, fields);
        Ok(data.to_envelope(raw))
    }

    fn build_update_member(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreUpdateMember>(raw)?;
        let table = self
            .manager
            .get_table(event.selector)
            .ok_or_else(|| anyhow!("no table found for selector {}", event.selector.to_string()))?;
        let value = table
            .parse_field(event.selector, event.values)
            .ok_or_else(|| anyhow!("failed to parse field for table {}", table.name.clone()))?;
        let id_field = make_entity_id_field(event.entity_id);
        let data = UpdateRecordFieldsV1::new(table.id, table.name.clone(), id_field, vec![value]);
        Ok(data.to_envelope(raw))
    }

    fn build_del_record(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<StoreDelRecord>(raw)?;
        let table = self
            .manager
            .get_table(event.selector)
            .ok_or_else(|| anyhow!("no table found for selector {}", event.selector.to_string()))?;
        let id_field = make_entity_id_field(event.entity_id);
        let data = DeleteRecordsV1::new(table.id, table.name.clone(), vec![id_field]);
        Ok(Envelope {
            type_id: torii_types_introspect::DELETE_RECORDS_ID,
            raw: Arc::new(raw.clone()),
            body: Body::Typed(Arc::new(data) as Arc<dyn Event>),
        })
    }

    fn build_emit_event(&self, raw: &EmittedEvent) -> Result<Envelope> {
        let event = raw_event_to_event::<EventEmitted>(raw)?;
        unimplemented!()
    }
}
