use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use torii::axum::Router;
use torii::etl::{
    envelope::{Envelope, TypeId},
    extractor::ExtractionBatch,
    sink::{EventBus, Sink, SinkContext, TopicInfo},
};
use torii_introspect::events::{IntrospectBody, IntrospectMsg};

use crate::grpc_service::ArcadeService;

pub struct ArcadeSink {
    service: Arc<ArcadeService>,
}

impl ArcadeSink {
    pub async fn new(
        database_url: &str,
        erc721_database_url: &str,
        max_connections: Option<u32>,
    ) -> Result<Self> {
        Ok(Self {
            service: Arc::new(
                ArcadeService::new(database_url, erc721_database_url, max_connections).await?,
            ),
        })
    }

    pub fn get_grpc_service_impl(&self) -> Arc<ArcadeService> {
        self.service.clone()
    }

    async fn reload_if_tracked_table(&self, table_name: &str) -> Result<()> {
        match table_name {
            "ARCADE-Game"
            | "ARCADE-Edition"
            | "ARCADE-Collection"
            | "ARCADE-Listing"
            | "ARCADE-Sale" => self.service.bootstrap_from_source().await,
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl Sink for ArcadeSink {
    fn name(&self) -> &'static str {
        "arcade"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("introspect")]
    }

    async fn process(&self, envelopes: &[Envelope], _batch: &ExtractionBatch) -> Result<()> {
        for envelope in envelopes {
            if envelope.type_id != TypeId::new("introspect") {
                continue;
            }

            let Some(body) = envelope.downcast_ref::<IntrospectBody>() else {
                continue;
            };

            match &body.msg {
                IntrospectMsg::CreateTable(table) => {
                    self.reload_if_tracked_table(&table.name).await?;
                }
                IntrospectMsg::UpdateTable(table) => {
                    self.reload_if_tracked_table(&table.name).await?;
                }
                IntrospectMsg::InsertsFields(insert) => {
                    let table_id = format!("{:#x}", insert.table);
                    for record in &insert.records {
                        let entity_id =
                            starknet::core::types::Felt::from_bytes_be_slice(&record.id);
                        match table_id.as_str() {
                            "0x6143bc86ed1a08df992c568392c454a92ef7e7b5ba08e9bf75643cf5cfc8b14" => {
                                self.service.refresh_game(entity_id).await?;
                            }
                            "0x76002f6d86762c47f4c4004d8ca9d8c2cb82c3929b5247a19e4551c47fd0a2c" => {
                                self.service.refresh_edition(entity_id).await?;
                            }
                            "0x61ac61b99b1ae4cb2f06c53e9f5cb7c9ef7638a67d970280f138993d2ddbaa" => {
                                self.service.refresh_collection(entity_id).await?;
                            }
                            "0x2b375547a6102a593675b82dc24b7ee906423985424792436866a3a3d3b194" => {
                                self.service.refresh_listing(entity_id).await?;
                            }
                            "0x2641babd55028e5173f25aec79c7481d2625d36a6856060fa02b0818ca8a199" => {
                                self.service.refresh_sale(entity_id).await?;
                            }
                            _ => {}
                        }
                    }
                }
                IntrospectMsg::DeleteRecords(delete) => {
                    let table_id = format!("{:#x}", delete.table);
                    for row in &delete.rows {
                        let entity_id = row.to_felt();
                        match table_id.as_str() {
                            "0x6143bc86ed1a08df992c568392c454a92ef7e7b5ba08e9bf75643cf5cfc8b14" => {
                                self.service.delete_game(entity_id).await?;
                            }
                            "0x76002f6d86762c47f4c4004d8ca9d8c2cb82c3929b5247a19e4551c47fd0a2c" => {
                                self.service.delete_edition(entity_id).await?;
                            }
                            "0x61ac61b99b1ae4cb2f06c53e9f5cb7c9ef7638a67d970280f138993d2ddbaa" => {
                                self.service.delete_collection(entity_id).await?;
                            }
                            "0x2b375547a6102a593675b82dc24b7ee906423985424792436866a3a3d3b194" => {
                                self.service.delete_listing(entity_id).await?;
                            }
                            "0x2641babd55028e5173f25aec79c7481d2625d36a6856060fa02b0818ca8a199" => {
                                self.service.delete_sale(entity_id).await?;
                            }
                            _ => {}
                        }
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
