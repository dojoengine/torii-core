use crate::store::DojoStoreTrait;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
pub use anyhow::Result as AnyResult;
use async_trait::async_trait;
use dojo_introspect::events::{
    EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use dojo_introspect::serde::dojo_primary_def;
use dojo_introspect::{DojoSchema, DojoSchemaFetcher};
use introspect_types::{
    Attributes, CairoEvent, CairoEventInfo, CairoSerde, IntoFeltSource, PrimaryDef, PrimaryTypeDef,
    ResultInto, SliceFeltSource, TableSchema,
};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::RwLock;
use torii::etl::event::EmittedEventExt;
use torii::etl::{Decoder, Envelope, EventBody};
use torii_introspect::events::IntrospectMsg;
use torii_introspect::EventId;

pub const DOJO_ID_FIELD_NAME: &str = "entity_id";

pub struct DojoDecoder<Store, F> {
    pub tables: RwLock<HashMap<Felt, DojoTable>>,
    pub store: Store,
    pub fetcher: F,
}

fn deserialize_data<'a, T>(keys: &[Felt], data: &'a [Felt]) -> DojoToriiResult<T>
where
    T: CairoEvent<CairoSerde<SliceFeltSource<'a>>> + CairoEventInfo,
{
    let mut keys = keys.into_source();
    let mut data: CairoSerde<_> = data.into();
    match T::deserialize_and_verify_event(&mut keys, &mut data) {
        Ok(event) => Ok(event),
        Err(err) => Err(DojoToriiError::EventDeserializationError(T::NAME, err)),
    }
}

// #[async_trait]
// pub trait DojoEventProcessor<Store: Sync + Send, F: Sync + Send>:
//     Sized + CairoEventInfo + Debug
// {
//     type Msg: EventId;
//     async fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg>;
//     fn deserialize_data<'a>(keys: &[Felt], data: &'a [Felt]) -> DojoToriiResult<Self>
//     where
//         Self: CairoEvent<CairoSerde<SliceFeltSource<'a>>>,
//     {
//         let mut keys = keys.into_source();
//         let mut data: CairoSerde<_> = data.into();
//         match Self::deserialize_and_verify_event(&mut keys, &mut data) {
//             Ok(event) => Ok(event),
//             Err(err) => Err(DojoToriiError::EventDeserializationError(Self::NAME, err)),
//         }
//     }
//     async fn data_to_msg<'a>(
//         decoder: &DojoDecoder<Store, F>,
//         keys: &[Felt],
//         data: &'a [Felt],
//     ) -> DojoToriiResult<Self::Msg>
//     where
//         Self: CairoEvent<CairoSerde<SliceFeltSource<'a>>>,
//         Self::Msg: Into<IntrospectMsg>,
//     {
//         Self::deserialize_data(keys, data)?
//             .event_to_msg(decoder)
//             .await
//     }
// }

#[async_trait]
pub trait DojoTableEvent<Store, F>: Sized + CairoEventInfo + Debug {
    type Msg: EventId;
    async fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg>;
}

pub trait DojoRecordEvent<Store, F>: Sized + CairoEventInfo + Debug {
    type Msg: EventId;
    fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg>;
}

#[async_trait]
impl<Store, F> DojoStoreTrait for DojoDecoder<Store, F>
where
    Store: DojoStoreTrait + Send + Sync,
    Store::Error: ToString,
    F: Send + Sync + 'static,
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

impl<Store, F> DojoDecoder<Store, F> {
    pub fn with_table<R>(
        &self,
        id: &Felt,
        f: impl FnOnce(&DojoTable) -> DojoToriiResult<R>,
    ) -> DojoToriiResult<R> {
        let tables = self.tables.read()?;
        let table = tables
            .get(id)
            .ok_or_else(|| DojoToriiError::TableNotFoundById(*id))?;
        f(table)
    }
}

impl<Store, F> DojoDecoder<Store, F>
where
    Store: DojoStoreTrait + Sync,
    F: DojoSchemaFetcher + Send + Sync + 'static,
{
    pub async fn new<S: Into<Store>>(store: S, fetcher: F) -> DojoToriiResult<Self> {
        let store = store.into();
        Ok(Self {
            tables: RwLock::new(
                store
                    .load_table_map()
                    .await
                    .map_err(DojoToriiError::store_error)?,
            ),
            store,
            fetcher,
        })
    }
    pub async fn register_table(
        &self,
        namespace: &str,
        name: &str,
        schema: DojoSchema,
    ) -> DojoToriiResult<TableSchema> {
        let table = DojoTable::from_schema(schema, namespace, name, dojo_primary_def());
        self.save_table(&table).await?;
        {
            if let Some(existing) = self.tables.read()?.get(&table.id) {
                return Err(DojoToriiError::TableAlreadyExists(
                    table.id,
                    existing.name.clone(),
                    name.to_string(),
                ));
            }
        }
        self.tables.write()?.insert(table.id, table.clone());
        Ok(table.into())
    }

    pub async fn update_table(&self, id: Felt, schema: DojoSchema) -> DojoToriiResult<TableSchema> {
        let mut table = {
            let mut tables = self.tables.write()?;
            match tables.remove(&id) {
                Some(t) => t,
                None => return Err(DojoToriiError::TableNotFoundById(id)),
            }
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

    async fn process_table_event<'a, E>(
        &self,
        keys: &'a [Felt],
        values: &'a [Felt],
    ) -> DojoToriiResult<IntrospectMsg>
    where
        E: DojoTableEvent<Store, F> + CairoEvent<CairoSerde<SliceFeltSource<'a>>> + Send,
        E::Msg: Into<IntrospectMsg>,
    {
        deserialize_data::<E>(keys, values)?
            .event_to_msg(self)
            .await
            .ok_into()
    }

    fn process_record_event<'a, E>(
        &self,
        keys: &'a [Felt],
        values: &'a [Felt],
    ) -> DojoToriiResult<IntrospectMsg>
    where
        E: DojoRecordEvent<Store, F> + CairoEvent<CairoSerde<SliceFeltSource<'a>>> + Send,
        E::Msg: Into<IntrospectMsg>,
    {
        deserialize_data::<E>(keys, values)?
            .event_to_msg(self)
            .ok_into()
    }

    pub async fn decode_event_data(
        &self,
        selector: &Felt,
        keys: &[Felt],
        values: &[Felt],
    ) -> DojoToriiResult<IntrospectMsg> {
        let selector_raw = selector.to_raw();
        match selector_raw {
            ModelRegistered::SELECTOR_RAW => {
                self.process_table_event::<ModelRegistered>(keys, values)
                    .await
            }
            ModelWithSchemaRegistered::SELECTOR_RAW => {
                self.process_table_event::<ModelWithSchemaRegistered>(keys, values)
                    .await
            }
            ModelUpgraded::SELECTOR_RAW => {
                self.process_table_event::<ModelUpgraded>(keys, values)
                    .await
            }
            EventRegistered::SELECTOR_RAW => {
                self.process_table_event::<EventRegistered>(keys, values)
                    .await
            }
            EventUpgraded::SELECTOR_RAW => {
                self.process_table_event::<EventUpgraded>(keys, values)
                    .await
            }
            StoreSetRecord::SELECTOR_RAW => {
                self.process_record_event::<StoreSetRecord>(keys, values)
            }
            StoreUpdateRecord::SELECTOR_RAW => {
                self.process_record_event::<StoreUpdateRecord>(keys, values)
            }
            StoreUpdateMember::SELECTOR_RAW => {
                self.process_record_event::<StoreUpdateMember>(keys, values)
            }
            StoreDelRecord::SELECTOR_RAW => {
                self.process_record_event::<StoreDelRecord>(keys, values)
            }
            EventEmitted::SELECTOR_RAW => self.process_record_event::<EventEmitted>(keys, values),
            _ => Err(DojoToriiError::UnknownDojoEventSelector(*selector)),
        }
    }

    pub async fn decode_raw_event(&mut self, raw: &EmittedEvent) -> DojoToriiResult<IntrospectMsg> {
        let (selector, keys, values) = raw
            .split_content()
            .ok_or(DojoToriiError::MissingEventSelector)?;
        self.decode_event_data(selector, keys, values).await
    }
}

#[async_trait]
impl<Store, F> Decoder for DojoDecoder<Store, F>
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    fn decoder_name(&self) -> &'static str {
        "dojo-introspect"
    }

    async fn decode_event(&self, event: &EmittedEvent) -> AnyResult<Vec<Envelope>> {
        let (selector, keys, data) = event
            .split_content()
            .ok_or(DojoToriiError::MissingEventSelector)?;

        self.decode_event_data(selector, keys, data)
            .await
            .map(|msg| vec![EventBody::new_envelope(msg, event)])
            .err_into()
    }
}
