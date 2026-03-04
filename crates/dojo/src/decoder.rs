use crate::manager::DojoTableManager;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
use async_trait::async_trait;
use dojo_introspect_types::events::{
    EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use dojo_introspect_types::DojoSchemaFetcher;
use introspect_types::{
    CairoEvent, CairoEventInfo, CairoSerde, FeltIds, IntoFeltSource, PrimaryDef, SliceFeltSource,
};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use torii_introspect::events::{
    CreateTable, DeleteRecords, InsertsFields, IntrospectMsg, UpdateTable,
};
use torii_introspect::IntrospectMsgTrait;

pub struct DojoDecoder<M, F> {
    pub manager: M,
    pub fetcher: F,
    pub primary_field: PrimaryDef,
}
#[async_trait]
trait DojoEventDecoder<M: Sync, F: Sync>: Sized {
    type Msg: IntrospectMsgTrait;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg>;
    fn deserialize_data<'a>(keys: &[Felt], data: &'a [Felt]) -> DojoToriiResult<Self>
    where
        Self: CairoEvent<CairoSerde<SliceFeltSource<'a>>>,
    {
        let mut keys = keys.into_source();
        let mut data: CairoSerde<_> = data.into();
        Ok(Self::deserialize_and_verify_event(&mut keys, &mut data)?)
    }
    async fn data_to_introspect_msg<'a>(
        decoder: &DojoDecoder<M, F>,
        keys: &[Felt],
        data: &'a [Felt],
    ) -> DojoToriiResult<IntrospectMsg>
    where
        Self: CairoEvent<CairoSerde<SliceFeltSource<'a>>>,
        Self::Msg: Into<IntrospectMsg>,
    {
        Self::deserialize_data(keys, data)?
            .event_to_msg(decoder)
            .await
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventDecoder<M, F>
    for ModelWithSchemaRegistered
{
    type Msg = CreateTable;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        decoder
            .manager
            .register_table(&self.namespace, &self.name, self.schema)
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventDecoder<M, F>
    for ModelRegistered
{
    type Msg = CreateTable;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .manager
            .register_table(&self.namespace, &self.name, schema)
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventDecoder<M, F>
    for EventRegistered
{
    type Msg = CreateTable;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .manager
            .register_table(&self.namespace, &self.name, schema)
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventDecoder<M, F>
    for ModelUpgraded
{
    type Msg = UpdateTable;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .manager
            .update_table(self.selector, schema)
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventDecoder<M, F>
    for EventUpgraded
{
    type Msg = UpdateTable;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .manager
            .update_table(self.selector, schema)
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventDecoder<M, F>
    for StoreSetRecord
{
    type Msg = InsertsFields;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let (columns, data) = decoder.with_table(self.selector, |table| {
            table.parse_record(self.keys, self.values)
        })?;
        Ok(InsertsFields::new_single(
            self.selector,
            columns,
            self.entity_id,
            data,
        ))
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventDecoder<M, F>
    for StoreUpdateRecord
{
    type Msg = InsertsFields;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let (columns, data) =
            decoder.with_table(self.selector, |table| table.parse_values(self.values))?;
        Ok(InsertsFields::new_single(
            self.selector,
            columns,
            self.entity_id,
            data,
        ))
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventDecoder<M, F>
    for EventEmitted
{
    type Msg = InsertsFields;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let primary = Felt::from_bytes_be(&self.keys.hash().into());
        let (columns, data) = decoder.with_table(self.selector, |table| {
            table.parse_record(self.keys, self.values)
        })?;
        Ok(InsertsFields::new_single(
            self.selector,
            columns,
            primary,
            data,
        ))
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventDecoder<M, F>
    for StoreUpdateMember
{
    type Msg = InsertsFields;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let member = self.member_selector;
        let data = decoder.with_table(self.selector, |table| {
            table.parse_field(member, self.values)
        })?;
        Ok(InsertsFields::new_single(
            self.selector,
            vec![member],
            self.entity_id,
            data,
        ))
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventDecoder<M, F>
    for StoreDelRecord
{
    type Msg = DeleteRecords;
    async fn event_to_msg(self, _decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        Ok(DeleteRecords::new(
            self.selector,
            vec![self.entity_id.into()],
        ))
    }
}

impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoDecoder<M, F> {
    async fn to_introspect_msg<'a, E>(
        &self,
        keys: &'a [Felt],
        values: &'a [Felt],
    ) -> DojoToriiResult<IntrospectMsg>
    where
        E: DojoEventDecoder<M, F> + CairoEvent<CairoSerde<SliceFeltSource<'a>>> + Send,
        E::Msg: Into<IntrospectMsg>,
    {
        E::data_to_introspect_msg(self, keys, values).await
    }
    fn with_table<Fn, R>(&self, id: Felt, f: Fn) -> DojoToriiResult<R>
    where
        Fn: FnOnce(&DojoTable) -> DojoToriiResult<R>,
    {
        self.manager.with_table(id, f)?
    }

    pub async fn decode_event(&self, event: &EmittedEvent) -> DojoToriiResult<IntrospectMsg> {
        let selector = event.keys[0];
        let selector_raw = selector.to_raw();
        let keys = &event.keys[1..];
        let values = &event.data;
        match selector_raw {
            ModelRegistered::SELECTOR_RAW => {
                self.to_introspect_msg::<ModelRegistered>(keys, values)
                    .await
            }
            ModelWithSchemaRegistered::SELECTOR_RAW => {
                self.to_introspect_msg::<ModelWithSchemaRegistered>(keys, values)
                    .await
            }
            ModelUpgraded::SELECTOR_RAW => {
                self.to_introspect_msg::<ModelUpgraded>(keys, values).await
            }
            EventRegistered::SELECTOR_RAW => {
                self.to_introspect_msg::<EventRegistered>(keys, values)
                    .await
            }
            EventUpgraded::SELECTOR_RAW => {
                self.to_introspect_msg::<EventUpgraded>(keys, values).await
            }
            StoreSetRecord::SELECTOR_RAW => {
                self.to_introspect_msg::<StoreSetRecord>(keys, values).await
            }
            StoreUpdateRecord::SELECTOR_RAW => {
                self.to_introspect_msg::<StoreUpdateRecord>(keys, values)
                    .await
            }
            StoreUpdateMember::SELECTOR_RAW => {
                self.to_introspect_msg::<StoreUpdateMember>(keys, values)
                    .await
            }
            StoreDelRecord::SELECTOR_RAW => {
                self.to_introspect_msg::<StoreDelRecord>(keys, values).await
            }
            EventEmitted::SELECTOR_RAW => {
                self.to_introspect_msg::<EventEmitted>(keys, values).await
            }
            _ => Err(DojoToriiError::UnknownDojoEventSelector(selector)),
        }
    }
}
