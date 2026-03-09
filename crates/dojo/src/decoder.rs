use crate::store::DojoStoreTrait;
use crate::table::DojoTableInfo;
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
    ResultInto, SliceFeltSource,
};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::RwLock;
use torii::etl::event::EmittedEventExt;
use torii::etl::{Decoder, Envelope, EventBody};
use torii_introspect::events::IntrospectMsg;
use torii_introspect::schema::TableSchema;
use torii_introspect::{EventId, TableKey};

pub const DOJO_ID_FIELD_NAME: &str = "entity_id";

pub struct DojoDecoder<Store, F> {
    pub tables: RwLock<HashMap<TableKey, DojoTableInfo>>,
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

#[async_trait]
pub trait DojoTableEvent<Store, F>: Sized + CairoEventInfo + Debug {
    type Msg: EventId;
    async fn event_to_msg(
        self,
        from_address: &Felt,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg>;
}

pub trait DojoRecordEvent<Store, F>: Sized + CairoEventInfo + Debug {
    type Msg: EventId;
    fn event_to_msg(
        self,
        from_address: &Felt,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg>;
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

    async fn load_table_map<K: From<(Option<Felt>, Felt)> + Eq + Hash>(
        &self,
    ) -> Result<HashMap<K, DojoTableInfo>, Self::Error> {
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
        key: &TableKey,
        f: impl FnOnce(&DojoTableInfo) -> DojoToriiResult<R>,
    ) -> DojoToriiResult<R> {
        let tables = self.tables.read()?;
        let table = tables
            .get(key)
            .ok_or_else(|| DojoToriiError::TableNotFoundById(key.owner, key.id))?;
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
        let full_table = DojoTable::from_schema(schema, namespace, name, dojo_primary_def()).into();
        self.save_table(&full_table).await?;
        let (key, table) = full_table.clone().into();
        {
            if let Some(existing) = self.tables.read()?.get(&key) {
                return Err(DojoToriiError::TableAlreadyExists(
                    key.id,
                    existing.name.clone(),
                    name.to_string(),
                ));
            }
        }
        self.tables.write()?.insert(key, table);
        Ok(full_table.into())
    }

    pub async fn update_table(
        &self,
        owner: Option<Felt>,
        id: Felt,
        schema: DojoSchema,
    ) -> DojoToriiResult<TableSchema> {
        let key = TableKey::new(owner, id);
        let mut info = {
            let mut tables = self.tables.write()?;
            match tables.remove(&key) {
                Some(t) => t,
                None => return Err(DojoToriiError::TableNotFoundById(owner, id)),
            }
        };
        let mut key_fields = Vec::new();
        let mut value_fields = Vec::new();
        for column in schema.columns {
            match column.has_attribute("key") {
                true => key_fields.push(column.id),
                false => value_fields.push(column.id),
            }
            let (column_id, column_info) = column.into();
            info.columns.insert(column_id, column_info);
        }
        info.key_fields = key_fields;
        info.value_fields = value_fields;
        let table = (key, info).into();
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
