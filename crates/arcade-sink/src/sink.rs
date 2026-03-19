use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::Felt;
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
    tracked_tables: RwLock<HashMap<String, TrackedTable>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TrackedTable {
    Game,
    Edition,
    Collection,
    Listing,
    Sale,
}

impl TrackedTable {
    fn known_table_ids() -> [(&'static str, Self); 5] {
        [
            (
                "0x6143bc86ed1a08df992c568392c454a92ef7e7b5ba08e9bf75643cf5cfc8b14",
                Self::Game,
            ),
            (
                "0x76002f6d86762c47f4c4004d8ca9d8c2cb82c3929b5247a19e4551c47fd0a2c",
                Self::Edition,
            ),
            (
                "0x61ac61b99b1ae4cb2f06c53e9f5cb7c9ef7638a67d970280f138993d2ddbaa",
                Self::Collection,
            ),
            (
                "0x2b375547a6102a593675b82dc24b7ee906423985424792436866a3a3d3b194",
                Self::Listing,
            ),
            (
                "0x2641babd55028e5173f25aec79c7481d2625d36a6856060fa02b0818ca8a199",
                Self::Sale,
            ),
        ]
    }

    fn from_table_name(table_name: &str) -> Option<Self> {
        match table_name {
            "ARCADE-Game" => Some(Self::Game),
            "ARCADE-Edition" => Some(Self::Edition),
            "ARCADE-Collection" => Some(Self::Collection),
            "ARCADE-Order" | "ARCADE-Listing" => Some(Self::Listing),
            "ARCADE-Sale" => Some(Self::Sale),
            _ => None,
        }
    }

    async fn refresh(self, service: &ArcadeService, entity_id: Felt) -> Result<()> {
        match self {
            Self::Game => service.refresh_game(entity_id).await,
            Self::Edition => service.refresh_edition(entity_id).await,
            Self::Collection => service.refresh_collection(entity_id).await,
            Self::Listing => service.refresh_listing(entity_id).await,
            Self::Sale => service.refresh_sale(entity_id).await,
        }
    }

    async fn delete(self, service: &ArcadeService, entity_id: Felt) -> Result<()> {
        match self {
            Self::Game => service.delete_game(entity_id).await,
            Self::Edition => service.delete_edition(entity_id).await,
            Self::Collection => service.delete_collection(entity_id).await,
            Self::Listing => service.delete_listing(entity_id).await,
            Self::Sale => service.delete_sale(entity_id).await,
        }
    }
}

impl ArcadeSink {
    pub async fn new(
        database_url: &str,
        erc721_database_url: &str,
        max_connections: Option<u32>,
    ) -> Result<Self> {
        let service =
            Arc::new(ArcadeService::new(database_url, erc721_database_url, max_connections).await?);
        let mut tracked_tables = TrackedTable::known_table_ids()
            .into_iter()
            .map(|(table_id, tracked_table)| (table_id.to_string(), tracked_table))
            .collect::<HashMap<_, _>>();
        tracked_tables.extend(
            service
                .load_tracked_table_names_by_id()
                .await?
                .into_iter()
                .filter_map(|(table_id, table_name)| {
                    TrackedTable::from_table_name(&table_name).map(|tracked| (table_id, tracked))
                }),
        );

        Ok(Self {
            service,
            tracked_tables: RwLock::new(tracked_tables),
        })
    }

    pub fn get_grpc_service_impl(&self) -> Arc<ArcadeService> {
        self.service.clone()
    }

    async fn reload_if_tracked_table(&self, table_name: &str) -> Result<()> {
        match TrackedTable::from_table_name(table_name) {
            Some(_) => self.service.bootstrap_from_source().await,
            None => Ok(()),
        }
    }

    fn update_tracked_table(&self, table_id: Felt, table_name: &str) {
        let table_id = format!("{table_id:#x}");
        let mut tracked_tables = self
            .tracked_tables
            .write()
            .expect("tracked table map lock poisoned");

        if let Some(tracked_table) = TrackedTable::from_table_name(table_name) {
            tracked_tables.insert(table_id, tracked_table);
        } else {
            tracked_tables.remove(&table_id);
        }
    }

    fn remove_tracked_table(&self, table_id: Felt) {
        self.tracked_tables
            .write()
            .expect("tracked table map lock poisoned")
            .remove(&format!("{table_id:#x}"));
    }

    fn tracked_table_for_id(&self, table_id: Felt) -> Option<TrackedTable> {
        self.tracked_tables
            .read()
            .expect("tracked table map lock poisoned")
            .get(&format!("{table_id:#x}"))
            .copied()
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
                    self.update_tracked_table(table.id, &table.name);
                    self.reload_if_tracked_table(&table.name).await?;
                }
                IntrospectMsg::UpdateTable(table) => {
                    self.update_tracked_table(table.id, &table.name);
                    self.reload_if_tracked_table(&table.name).await?;
                }
                IntrospectMsg::RenameTable(table) => {
                    self.update_tracked_table(table.id, &table.name);
                    self.reload_if_tracked_table(&table.name).await?;
                }
                IntrospectMsg::DropTable(table) => {
                    self.remove_tracked_table(table.id);
                }
                IntrospectMsg::InsertsFields(insert) => {
                    for record in &insert.records {
                        let entity_id =
                            starknet::core::types::Felt::from_bytes_be_slice(&record.id);
                        if let Some(tracked_table) = self.tracked_table_for_id(insert.table) {
                            tracked_table
                                .refresh(self.service.as_ref(), entity_id)
                                .await?;
                        }
                    }
                }
                IntrospectMsg::DeleteRecords(delete) => {
                    for row in &delete.rows {
                        let entity_id = row.to_felt();
                        if let Some(tracked_table) = self.tracked_table_for_id(delete.table) {
                            tracked_table
                                .delete(self.service.as_ref(), entity_id)
                                .await?;
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
