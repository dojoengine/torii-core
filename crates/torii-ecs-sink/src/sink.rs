use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use dojo_introspect::events::{
    EventEmitted, StoreDelRecord, StoreSetRecord, StoreUpdateMember, StoreUpdateRecord,
};
use introspect_types::event::CairoEventInfo;
use starknet::core::types::Felt;
use torii::axum::Router;
use torii::etl::{
    envelope::{Envelope, TypeId},
    extractor::ExtractionBatch,
    sink::{EventBus, Sink, SinkContext, TopicInfo},
};
use torii_introspect::events::{IntrospectBody, IntrospectMsg};

use crate::grpc_service::{EcsService, TableKind};

pub struct EcsSink {
    service: Arc<EcsService>,
}

impl EcsSink {
    pub async fn new(database_url: &str, max_connections: Option<u32>) -> Result<Self> {
        Ok(Self {
            service: Arc::new(EcsService::new(database_url, max_connections).await?),
        })
    }

    pub fn get_grpc_service_impl(&self) -> Arc<EcsService> {
        self.service.clone()
    }
}

#[async_trait]
impl Sink for EcsSink {
    fn name(&self) -> &'static str {
        "ecs"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("introspect")]
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        for (ordinal, event) in batch.events.iter().enumerate() {
            let context = batch
                .get_event_context(&event.transaction_hash, event.from_address)
                .unwrap_or_default();
            self.service
                .store_event(
                    event.from_address,
                    event.transaction_hash,
                    context.transaction.block_number,
                    context.block.timestamp,
                    &event.keys,
                    &event.data,
                    ordinal,
                )
                .await?;

            self.service
                .record_contract_progress(
                    event.from_address,
                    crate::proto::types::ContractType::World,
                    context.transaction.block_number,
                    context.block.timestamp,
                    None,
                )
                .await?;

            let Some(selector) = event.keys.first() else {
                continue;
            };
            let selector_raw = selector.to_raw();
            if matches!(
                selector_raw,
                StoreSetRecord::SELECTOR_RAW
                    | StoreUpdateRecord::SELECTOR_RAW
                    | StoreUpdateMember::SELECTOR_RAW
                    | StoreDelRecord::SELECTOR_RAW
            ) {
                if event.keys.len() >= 3 {
                    self.service
                        .record_table_kind(event.from_address, event.keys[1], TableKind::Entity)
                        .await?;
                    self.service
                        .upsert_entity_meta(
                            TableKind::Entity,
                            event.from_address,
                            event.keys[1],
                            event.keys[2],
                            context.block.timestamp,
                            selector_raw == StoreDelRecord::SELECTOR_RAW,
                        )
                        .await?;
                }
            } else if selector_raw == EventEmitted::SELECTOR_RAW && event.keys.len() >= 2 {
                self.service
                    .record_table_kind(event.from_address, event.keys[1], TableKind::EventMessage)
                    .await?;
            }
        }

        for envelope in envelopes {
            if envelope.type_id != TypeId::new("introspect") {
                continue;
            }

            let Some(body) = envelope.downcast_ref::<IntrospectBody>() else {
                continue;
            };
            let context = batch
                .get_event_context(&body.transaction_hash, body.from_address)
                .unwrap_or_default();

            match &body.msg {
                IntrospectMsg::CreateTable(table) => {
                    self.service
                        .cache_created_table(body.from_address, table)
                        .await;
                    self.service
                        .record_table_kind(body.from_address, table.id, TableKind::Entity)
                        .await
                        .ok();
                }
                IntrospectMsg::UpdateTable(table) => {
                    self.service
                        .cache_updated_table(body.from_address, table)
                        .await;
                    self.service
                        .record_table_kind(body.from_address, table.id, TableKind::Entity)
                        .await
                        .ok();
                }
                IntrospectMsg::InsertsFields(insert) => {
                    let kind = if batch.events.iter().any(|event| {
                        event.keys.first().is_some_and(|selector| {
                            selector.to_raw() == EventEmitted::SELECTOR_RAW
                                && event.keys.get(1).copied() == Some(insert.table)
                        })
                    }) {
                        TableKind::EventMessage
                    } else {
                        TableKind::Entity
                    };
                    self.service
                        .record_table_kind(body.from_address, insert.table, kind)
                        .await?;
                    for record in &insert.records {
                        let entity_id = Felt::from_bytes_be(&record.id);
                        self.service
                            .upsert_entity_meta(
                                kind,
                                body.from_address,
                                insert.table,
                                entity_id,
                                context.block.timestamp,
                                false,
                            )
                            .await?;
                        self.service
                            .upsert_entity_model(
                                kind,
                                body.from_address,
                                insert.table,
                                &insert.columns,
                                record,
                                context.block.timestamp,
                            )
                            .await?;
                        self.service
                            .publish_entity_update(kind, body.from_address, entity_id)
                            .await?;
                    }
                }
                IntrospectMsg::DeleteRecords(delete) => {
                    let kind = self.service.table_kind(delete.table).await?;
                    for row in &delete.rows {
                        let entity_id = row.to_felt();
                        self.service
                            .upsert_entity_meta(
                                kind,
                                body.from_address,
                                delete.table,
                                entity_id,
                                context.block.timestamp,
                                true,
                            )
                            .await?;
                        self.service
                            .delete_entity_model(kind, body.from_address, delete.table, entity_id)
                            .await?;
                        self.service
                            .publish_entity_update(kind, body.from_address, entity_id)
                            .await?;
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        Vec::new()
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }

    async fn initialize(
        &mut self,
        _event_bus: Arc<EventBus>,
        _context: &SinkContext,
    ) -> Result<()> {
        Ok(())
    }
}
