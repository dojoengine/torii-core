use crate::manager::DojoTableManager;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
pub use anyhow::Result as AnyResult;
use async_trait::async_trait;
use dojo_introspect_types::events::{
    EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use dojo_introspect_types::DojoSchemaFetcher;
use introspect_types::{
    CairoEvent, CairoEventInfo, CairoSerde, FeltIds, IntoFeltSource, PrimaryDef, ResultInto,
    SliceFeltSource,
};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use torii::etl::{Decoder, Envelope, EventBody};
use torii_introspect::events::{
    CreateTable, DeleteRecords, InsertsFields, IntrospectMsg, UpdateTable,
};
use torii_introspect::EventId;
pub struct DojoDecoder<M, F> {
    pub manager: M,
    pub fetcher: F,
    pub primary_field: PrimaryDef,
}
#[async_trait]
trait DojoEventProcessor<M: Sync, F: Sync>: Sized {
    type Msg: EventId;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg>;
    fn deserialize_data<'a>(keys: &[Felt], data: &'a [Felt]) -> DojoToriiResult<Self>
    where
        Self: CairoEvent<CairoSerde<SliceFeltSource<'a>>>,
    {
        let mut keys = keys.into_source();
        let mut data: CairoSerde<_> = data.into();
        Ok(Self::deserialize_and_verify_event(&mut keys, &mut data)?)
    }
    async fn data_to_msg<'a>(
        decoder: &DojoDecoder<M, F>,
        keys: &[Felt],
        data: &'a [Felt],
    ) -> DojoToriiResult<Self::Msg>
    where
        Self: CairoEvent<CairoSerde<SliceFeltSource<'a>>>,
        Self::Msg: Into<IntrospectMsg>,
    {
        Self::deserialize_data(keys, data)?
            .event_to_msg(decoder)
            .await
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
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
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
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
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
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
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
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
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
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
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
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
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
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
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
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
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
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
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
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
    async fn to_msg<'a, E>(
        &self,
        keys: &'a [Felt],
        values: &'a [Felt],
    ) -> DojoToriiResult<IntrospectMsg>
    where
        E: DojoEventProcessor<M, F> + CairoEvent<CairoSerde<SliceFeltSource<'a>>> + Send,
        E::Msg: Into<IntrospectMsg>,
    {
        E::data_to_msg(self, keys, values).await.ok_into()
    }
    fn with_table<Fn, R>(&self, id: Felt, f: Fn) -> DojoToriiResult<R>
    where
        Fn: FnOnce(&DojoTable) -> DojoToriiResult<R>,
    {
        self.manager.with_table(id, f)?
    }

    pub async fn decode_event_data(
        &self,
        selector: &Felt,
        keys: &[Felt],
        values: &[Felt],
    ) -> DojoToriiResult<IntrospectMsg> {
        let selector_raw = selector.to_raw();
        match selector_raw {
            ModelRegistered::SELECTOR_RAW => self.to_msg::<ModelRegistered>(keys, values).await,
            ModelWithSchemaRegistered::SELECTOR_RAW => {
                self.to_msg::<ModelWithSchemaRegistered>(keys, values).await
            }
            ModelUpgraded::SELECTOR_RAW => self.to_msg::<ModelUpgraded>(keys, values).await,
            EventRegistered::SELECTOR_RAW => self.to_msg::<EventRegistered>(keys, values).await,
            EventUpgraded::SELECTOR_RAW => self.to_msg::<EventUpgraded>(keys, values).await,
            StoreSetRecord::SELECTOR_RAW => self.to_msg::<StoreSetRecord>(keys, values).await,
            StoreUpdateRecord::SELECTOR_RAW => self.to_msg::<StoreUpdateRecord>(keys, values).await,
            StoreUpdateMember::SELECTOR_RAW => self.to_msg::<StoreUpdateMember>(keys, values).await,
            StoreDelRecord::SELECTOR_RAW => self.to_msg::<StoreDelRecord>(keys, values).await,
            EventEmitted::SELECTOR_RAW => self.to_msg::<EventEmitted>(keys, values).await,
            _ => Err(DojoToriiError::UnknownDojoEventSelector(*selector)),
        }
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync + Send, F: DojoSchemaFetcher + Sync + Send> Decoder
    for DojoDecoder<M, F>
{
    fn decoder_name(&self) -> &'static str {
        "dojo-introspect"
    }

    async fn decode_event(&self, event: &EmittedEvent) -> AnyResult<Vec<Envelope>> {
        let (selector, keys) = event
            .keys
            .split_first()
            .ok_or(DojoToriiError::MissingEventSelector)?;

        self.decode_event_data(selector, keys, &event.data)
            .await
            .map(|msg| vec![EventBody::new_envelope(msg, event)])
            .err_into()
    }
}
