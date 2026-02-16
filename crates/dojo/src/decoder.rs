use crate::manager::DojoTableManager;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
use dojo_introspect_types::DojoSchema;
use dojo_introspect_types::{
    events::{
        DojoEvent, DojoEventSerde, EventEmitted, EventRegistered, EventUpgraded, ModelRegistered,
        ModelUpgraded, ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord,
        StoreUpdateMember, StoreUpdateRecord,
    },
    DojoSchemaFetcher,
};
use introspect_types::{
    CairoEvent, CairoEventInfo, CairoSerde, DecodeError, DecodeResult, FeltSource, IntoFeltSource,
    PrimaryDef, PrimaryValue, SliceFeltSource, VecFeltSource,
};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use torii::etl::event::EmittedEventExt;
use torii::etl::{Envelope, TypedBody};
use torii_introspect::events::{CreateTable, DeleteRecords, InsertFields, UpdateTable};
use torii_introspect::IdValue;

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
        let event = ModelRegistered::from_emitted_event(raw)?;
        let schema = self.fetcher.schema(event.address).await?;
        let table = self
            .manager
            .register_table(&event.namespace, &event.name, schema)?;
        let id = format!("dojo_introspect.create_table.{}", table.id);
        Ok(Into::<CreateTable>::into(table).to_envelope(id, raw.metadata()))
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
        let id = format!("dojo_introspect.create_table.{}", table.id);

        Ok(Into::<CreateTable>::into(table).to_envelope(id, raw.metadata()))
    }

    async fn build_model_upgraded(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event: ModelUpgraded = CairoEvent::<CairoSerde<_>>::from_emitted_event(raw)?;
        let dojo_schema = self.fetcher.schema(event.address).await?;
        let table = self.manager.update_table(event.selector, dojo_schema)?;
        let id = format!("dojo_introspect.update_table.{}", table.id);

        Ok(Into::<UpdateTable>::into(table).to_envelope(raw, raw.metadata()))
    }

    async fn build_event_registered(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event = EventRegistered::from_emitted_event(raw)?;
        let schema = self.fetcher.schema(event.address).await?;
        let table = self
            .manager
            .register_table(&event.namespace, &event.name, schema)?;
        Ok(Into::<CreateTable>::into(table).to_envelope(raw))
    }

    async fn build_event_upgraded(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event = EventUpgraded::from_emitted_event(raw)?;
        let schema = self.fetcher.schema(event.address).await?;
        let table = self.manager.update_table(event.selector, schema)?;
        Ok(Into::<UpdateTable>::into(table).to_envelope(raw))
    }

    fn build_set_record(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event = StoreSetRecord::from_emitted_event(raw)?;
        let fields = self.with_table(event.selector, |table| {
            Ok(table.parse_key_values(event.keys.clone(), event.values.clone()))
        })??;
        let data = InsertFields::new(
            event.selector,
            PrimaryValue::Felt252(event.entity_id),
            fields,
        );
        data.to_envelope(raw, raw.metadata())
    }

    fn build_update_record(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event = StoreUpdateRecord::from_emitted_event(raw)?;
        let fields =
            self.with_table(event.selector, |table| Ok(table.parse_values(event.values)))??;
        let data = InsertFields::new(
            event.selector,
            PrimaryValue::Felt252(event.entity_id),
            fields,
        );
        data.to_envelope(raw, raw.metadata())
    }

    fn build_update_member(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event = StoreUpdateMember::from_emitted_event(raw)?;
        let field = self.with_table(event.selector, |table| {
            Ok(table.parse_field(event.member_selector, event.values))
        })??;

        let data = InsertFields::new(
            event.selector,
            PrimaryValue::Felt252(event.entity_id),
            vec![IdValue::new(event.member_selector, field)],
        );
        data.to_envelope(raw, raw.metadata())
    }

    fn build_del_record(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event = StoreDelRecord::from_emitted_event(raw)?;
        let data = DeleteRecords::new(event.selector, vec![PrimaryValue::Felt252(event.entity_id)]);
        data.to_envelope(raw, raw.metadata())
    }

    fn build_emit_event(&self, raw: &EmittedEvent) -> DojoToriiResult<Envelope> {
        let event = EventEmitted::from_emitted_event(raw)?;
        let fields = self.with_table(event.selector, |table| {
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
        let primary = make_entity_id_for_event(&event.keys);
        let data = InsertFields::new(event.selector, primary, fields);
        data.to_ok_envelope(raw, raw.metadata())
    }
    pub fn decode_event(&self, event: &EmittedEvent) -> DojoToriiResult<DojoEvent> {
        let mut keys = (&event.keys).into_source();
        let mut data: CairoSerde<_> = (&event.data).into();
        let selector = keys.next()?;
        let selector_raw = selector.to_raw();
        match selector_raw {
            ModelRegistered::SELECTOR_RAW => Ok(
                ModelRegistered::deserialize_and_verify_event_enum(&mut keys, &mut data)?,
            ),
            ModelWithSchemaRegistered::SELECTOR_RAW => Ok(
                ModelWithSchemaRegistered::deserialize_and_verify_event_enum(&mut keys, &mut data)?,
            ),
            ModelUpgraded::SELECTOR_RAW => Ok(ModelUpgraded::deserialize_and_verify_event_enum(
                &mut keys, &mut data,
            )?),
            EventRegistered::SELECTOR_RAW => Ok(
                EventRegistered::deserialize_and_verify_event_enum(&mut keys, &mut data)?,
            ),
            EventUpgraded::SELECTOR_RAW => Ok(EventUpgraded::deserialize_and_verify_event_enum(
                &mut keys, &mut data,
            )?),
            StoreSetRecord::SELECTOR_RAW => Ok(StoreSetRecord::deserialize_and_verify_event_enum(
                &mut keys, &mut data,
            )?),
            StoreUpdateRecord::SELECTOR_RAW => Ok(
                StoreUpdateRecord::deserialize_and_verify_event_enum(&mut keys, &mut data)?,
            ),
            StoreUpdateMember::SELECTOR_RAW => Ok(
                StoreUpdateMember::deserialize_and_verify_event_enum(&mut keys, &mut data)?,
            ),
            StoreDelRecord::SELECTOR_RAW => Ok(StoreDelRecord::deserialize_and_verify_event_enum(
                &mut keys, &mut data,
            )?),
            EventEmitted::SELECTOR_RAW => Ok(EventEmitted::deserialize_and_verify_event_enum(
                &mut keys, &mut data,
            )?),
            _ => Err(DojoToriiError::UnknownDojoEventSelector(selector)),
        }
    }
}
