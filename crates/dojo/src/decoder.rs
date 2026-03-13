use crate::store::DojoStoreTrait;
use crate::table::{sort_columns, DojoTableInfo};
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
    CairoEvent, CairoEventInfo, CairoSerde, IntoFeltSource, PrimaryDef, PrimaryTypeDef, ResultInto,
    SliceFeltSource,
};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::RwLock;
use torii::etl::event::EmittedEventExt;
use torii::etl::{Decoder, Envelope, EventBody};
use torii_introspect::events::IntrospectMsg;
use torii_introspect::schema::{TableMetadata, TableSchema};
use torii_introspect::EventId;

pub const DOJO_ID_FIELD_NAME: &str = "entity_id";

pub struct DojoDecoder<Store, F> {
    pub tables: RwLock<HashMap<Felt, DojoTableInfo>>,
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
        raw: &EmittedEvent,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg>;
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

    async fn save_table(
        &self,
        owner: &Felt,
        table: &DojoTable,
        tx_hash: &Felt,
        block_number: u64,
    ) -> DojoToriiResult<()> {
        self.store
            .save_table(owner, table, tx_hash, block_number)
            .await
            .map_err(DojoToriiError::store_error)
    }

    async fn load_tables(&self, owners: &[Felt]) -> DojoToriiResult<Vec<DojoTable>> {
        self.store
            .load_tables(owners)
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
        f: impl FnOnce(&DojoTableInfo) -> DojoToriiResult<R>,
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
    pub fn new<S: Into<Store>>(store: S, fetcher: F) -> Self {
        let store = store.into();
        Self {
            tables: Default::default(),
            store,
            fetcher,
        }
    }

    pub async fn load_tables(&self, owners: &[Felt]) -> DojoToriiResult<()> {
        let new_tables = self
            .store
            .load_tables(owners)
            .await
            .map_err(DojoToriiError::store_error)?;
        let mut tables = self.tables.write()?;
        for table in new_tables {
            let (id, info) = table.into();
            tables.insert(id, info);
        }
        Ok(())
    }

    pub fn with_tables<S: Into<Store>>(store: S, fetcher: F, tables: Vec<DojoTable>) -> Self {
        let store = store.into();
        let table_map = tables.into_iter().map(Into::into).collect();
        Self::with_table_map(store, fetcher, table_map)
    }

    pub fn with_table_map(store: Store, fetcher: F, tables: HashMap<Felt, DojoTableInfo>) -> Self {
        Self {
            tables: RwLock::new(tables),
            store,
            fetcher,
        }
    }

    pub async fn register_table(
        &self,
        owner: &Felt,
        namespace: &str,
        name: &str,
        schema: DojoSchema,
        metadata: &impl TableMetadata,
    ) -> DojoToriiResult<TableSchema> {
        let full_table = DojoTable::from_schema(schema, namespace, name, dojo_primary_def());
        self.save_table(
            owner,
            &full_table,
            metadata.tx_hash(),
            metadata.block_number(),
        )
        .await?;
        let (id, table) = full_table.clone().into();
        {
            if let Some(existing) = self.tables.read()?.get(&id) {
                return Err(DojoToriiError::TableAlreadyExists(
                    id,
                    existing.name.clone(),
                    name.to_string(),
                ));
            }
        }
        self.tables.write()?.insert(id, table);
        Ok(full_table.into())
    }

    pub async fn update_table(
        &self,
        owner: &Felt,
        id: Felt,
        schema: DojoSchema,
        meta_data: &impl TableMetadata,
    ) -> DojoToriiResult<TableSchema> {
        let mut info = {
            let mut tables = self.tables.write()?;
            match tables.remove(&id) {
                Some(t) => t,
                None => return Err(DojoToriiError::TableNotFoundById(id)),
            }
        };
        let (columns, key_fields, value_fields) = sort_columns(schema.columns);
        info.columns.extend(columns);
        info.key_fields = key_fields;
        info.value_fields = value_fields;
        let table = (id, info).into();
        self.store
            .save_table(owner, &table, meta_data.tx_hash(), meta_data.block_number())
            .await
            .map_err(DojoToriiError::store_error)?;
        let (_, info) = table.clone().into();
        self.tables.write()?.insert(id, info);
        Ok(table.to_schema())
    }

    async fn process_table_event<'a, E>(
        &self,
        raw: &EmittedEvent,
        keys: &'a [Felt],
        values: &'a [Felt],
    ) -> DojoToriiResult<IntrospectMsg>
    where
        E: DojoTableEvent<Store, F> + CairoEvent<CairoSerde<SliceFeltSource<'a>>> + Send,
        E::Msg: Into<IntrospectMsg>,
    {
        deserialize_data::<E>(keys, values)?
            .event_to_msg(raw, self)
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
        raw: &EmittedEvent,
        selector: &Felt,
        keys: &[Felt],
        values: &[Felt],
    ) -> DojoToriiResult<IntrospectMsg> {
        let selector_raw = selector.to_raw();
        match selector_raw {
            ModelRegistered::SELECTOR_RAW => {
                self.process_table_event::<ModelRegistered>(raw, keys, values)
                    .await
            }
            ModelWithSchemaRegistered::SELECTOR_RAW => {
                self.process_table_event::<ModelWithSchemaRegistered>(raw, keys, values)
                    .await
            }
            ModelUpgraded::SELECTOR_RAW => {
                self.process_table_event::<ModelUpgraded>(raw, keys, values)
                    .await
            }
            EventRegistered::SELECTOR_RAW => {
                self.process_table_event::<EventRegistered>(raw, keys, values)
                    .await
            }
            EventUpgraded::SELECTOR_RAW => {
                self.process_table_event::<EventUpgraded>(raw, keys, values)
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

    pub async fn decode_raw_event(&self, raw: &EmittedEvent) -> DojoToriiResult<IntrospectMsg> {
        let (selector, keys) = raw
            .split_keys()
            .ok_or(DojoToriiError::MissingEventSelector)?;
        self.decode_event_data(raw, selector, keys, &raw.data).await
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
        self.decode_raw_event(event)
            .await
            .map(|msg| vec![EventBody::new_envelope(msg, event)])
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use dojo_introspect::DojoIntrospectError;
    use introspect_types::{Attribute, ColumnDef, TypeDef};
    use std::sync::Mutex;

    #[derive(Debug, thiserror::Error)]
    #[error("{0}")]
    struct FakeStoreError(String);

    #[derive(Default)]
    struct FakeStore {
        saved_blocks: Mutex<Vec<u64>>,
    }

    #[async_trait]
    impl DojoStoreTrait for FakeStore {
        type Error = FakeStoreError;

        async fn save_table(
            &self,
            _owner: &Felt,
            _table: &DojoTable,
            _tx_hash: &Felt,
            block_number: u64,
        ) -> Result<(), Self::Error> {
            self.saved_blocks.lock().unwrap().push(block_number);
            Ok(())
        }

        async fn load_tables(&self, _owners: &[Felt]) -> Result<Vec<DojoTable>, Self::Error> {
            Ok(Vec::new())
        }
    }

    struct PanicFetcher;

    #[async_trait]
    impl DojoSchemaFetcher for PanicFetcher {
        async fn schema(&self, _contract_address: Felt) -> Result<DojoSchema, DojoIntrospectError> {
            panic!("schema fetch should not be used in this test");
        }
    }

    fn schema(columns: &[(u64, &str, bool)]) -> DojoSchema {
        DojoSchema {
            name: "Duelist".to_string(),
            attributes: vec![Attribute::new_empty("model".to_string())],
            columns: columns
                .iter()
                .map(|(id, name, key)| ColumnDef {
                    id: Felt::from(*id),
                    name: (*name).to_string(),
                    attributes: if *key {
                        vec![Attribute::new_empty("key".to_string())]
                    } else {
                        vec![]
                    },
                    type_def: TypeDef::U32,
                })
                .collect(),
            legacy: false,
        }
    }

    #[tokio::test]
    async fn update_table_keeps_table_in_decoder_and_records_block() {
        let owner = Felt::from(0x123_u64);
        let initial = DojoTable::from_schema(
            schema(&[(1, "entity_id", true), (2, "health", false)]),
            "pistols",
            "Duelist",
            primary_field_def(),
        );
        let table_id = initial.id;
        let store = FakeStore::default();
        let decoder: DojoDecoder<FakeStore, PanicFetcher> =
            DojoDecoder::with_tables(store, PanicFetcher, vec![initial]);

        decoder
            .update_table(
                &owner,
                table_id,
                schema(&[
                    (1, "entity_id", true),
                    (2, "health", false),
                    (3, "armor", false),
                ]),
                &(42, Felt::ZERO),
            )
            .await
            .unwrap();

        let parsed = decoder
            .with_table(&table_id, |table| Ok(table.columns.len()))
            .unwrap();
        assert_eq!(parsed, 3);
        assert_eq!(*decoder.store.saved_blocks.lock().unwrap(), vec![42]);
    }
}
