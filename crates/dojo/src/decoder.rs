use crate::manager::DojoTableManager;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
use dojo_introspect_types::events::{
    EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use dojo_introspect_types::DojoSchemaFetcher;
use introspect_types::{CairoEvent, CairoEventInfo, CairoSerde, FeltIds, PrimaryDef};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use torii::etl::event::EmittedEventExt;
use torii::etl::Envelope;
use torii_introspect::events::{CreateTable, DeleteRecords, InsertFields, UpdateTable};
use torii_introspect::{IdValue, IntrospectEvent};

pub struct DojoDecoder<M, F> {
    pub manager: M,
    pub fetcher: F,
    pub primary_field: PrimaryDef,
}

impl<M: DojoTableManager, F: DojoSchemaFetcher> DojoDecoder<M, F> {
    fn with_table<Fn, R>(&self, id: Felt, f: Fn) -> DojoToriiResult<R>
    where
        Fn: FnOnce(&DojoTable) -> DojoToriiResult<R>,
    {
        self.manager.with_table(id, f)?
    }

    async fn build_model_registered(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event: ModelRegistered = CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;
        let schema = self.fetcher.schema(event.address).await?;
        let table = self
            .manager
            .register_table(&event.namespace, &event.name, schema)?;
        Ok(Into::<CreateTable>::into(table).to_envelope(raw.metadata()))
    }

    async fn build_model_with_schema_registered(
        &self,
        raw: &EmittedEvent,
    ) -> DojoToriiResult<Envelope> {
        let event: ModelWithSchemaRegistered =
            CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;

        let table = self
            .manager
            .register_table(&event.namespace, &event.name, event.schema)?;

        Ok(Into::<CreateTable>::into(table).to_envelope(raw.metadata()))
    }

    async fn build_event_registered(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event: EventRegistered = CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;
        let schema = self.fetcher.schema(event.address).await?;
        let table = self
            .manager
            .register_table(&event.namespace, &event.name, schema)?;
        Ok(Into::<CreateTable>::into(table).to_envelope(raw.metadata()))
    }

    async fn build_model_upgraded(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event: ModelUpgraded = CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;
        let dojo_schema = self.fetcher.schema(event.address).await?;
        let table = self.manager.update_table(event.selector, dojo_schema)?;

        Ok(Into::<UpdateTable>::into(table).to_envelope(raw.metadata()))
    }

    async fn build_event_upgraded(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event: EventUpgraded = CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;
        let schema = self.fetcher.schema(event.address).await?;
        let table = self.manager.update_table(event.selector, schema)?;
        Ok(Into::<UpdateTable>::into(table).to_envelope(raw.metadata()))
    }

    fn build_set_record(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event: StoreSetRecord = CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;
        let fields = self.with_table(event.selector, |table| {
            Ok(table.parse_key_values(event.keys.clone(), event.values.clone()))
        })??;
        let data = InsertFields::new(event.selector, event.entity_id.into(), fields);
        Ok(data.to_envelope(raw.metadata()))
    }

    fn build_update_record(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event: StoreUpdateRecord = CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;
        let fields =
            self.with_table(event.selector, |table| Ok(table.parse_values(event.values)))??;
        let data = InsertFields::new(event.selector, event.entity_id.into(), fields);
        Ok(data.to_envelope(raw.metadata()))
    }

    fn build_emit_event(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event: EventEmitted = CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;

        let primary = event.keys.hash().into();
        let fields = self.with_table(event.selector, |table| {
            table.parse_key_values(event.keys, event.values)
        })?;
        let data = InsertFields::new(event.selector, primary, fields);
        Ok(data.to_envelope(raw.metadata()))
    }

    fn build_update_member(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event: StoreUpdateMember = CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;
        let field = self.with_table(event.selector, |table| {
            Ok(table.parse_field(event.member_selector, event.values))
        })??;

        let data = InsertFields::new(
            event.selector,
            event.entity_id.into(),
            vec![IdValue::new(event.member_selector, field)],
        );
        Ok(data.to_envelope(raw.metadata()))
    }

    fn build_del_record(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event: StoreDelRecord = CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;
        let data = DeleteRecords::new(event.selector, vec![event.entity_id.into()]);
        Ok(data.to_envelope(raw.metadata()))
    }

    async fn decode_event(&self, event: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let selector = event.keys[0];
        let selector_raw = selector.to_raw();
        match selector_raw {
            ModelRegistered::SELECTOR_RAW => self.build_event_registered(event).await,
            ModelWithSchemaRegistered::SELECTOR_RAW => {
                self.build_model_with_schema_registered(event).await
            }
            ModelUpgraded::SELECTOR_RAW => self.build_model_upgraded(event).await,
            EventRegistered::SELECTOR_RAW => self.build_event_registered(event).await,
            EventUpgraded::SELECTOR_RAW => self.build_event_upgraded(event).await,
            StoreSetRecord::SELECTOR_RAW => self.build_set_record(event),
            StoreUpdateRecord::SELECTOR_RAW => self.build_update_record(event),
            StoreUpdateMember::SELECTOR_RAW => self.build_update_member(event),
            StoreDelRecord::SELECTOR_RAW => self.build_del_record(event),
            EventEmitted::SELECTOR_RAW => self.build_emit_event(event),
            _ => Err(DojoToriiError::UnknownDojoEventSelector(selector)),
        }
    }
}
