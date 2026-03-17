use crate::processor::PostgresSimpleDb;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use torii::axum::Router;
use torii::etl::{
    envelope::{Envelope, TypeId},
    extractor::ExtractionBatch,
    sink::{EventBus, Sink, SinkContext, TopicInfo},
};
use torii_introspect::events::{IntrospectBody, IntrospectMsg};
use torii_postgres::PostgresConnection;

pub const LOGGING_TARGET: &str = "torii::sinks::introspect::postgres";
const INTROSPECT_TYPE: TypeId = TypeId::new("introspect");

#[async_trait]
impl<T: Send + Sync + PostgresConnection> Sink for PostgresSimpleDb<T> {
    fn name(&self) -> &'static str {
        "introspect-postgres"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![INTROSPECT_TYPE]
    }

    async fn process(&self, envelopes: &[Envelope], _batch: &ExtractionBatch) -> Result<()> {
        let mut processed = 0usize;
        let mut create_tables: usize = 0usize;
        let mut update_tables = 0usize;
        let mut inserts_fields = 0usize;
        let mut inserted_records = 0usize;
        let mut delete_records = 0usize;
        let mut msgs = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            if envelope.type_id == INTROSPECT_TYPE {
                if let Some(body) = envelope.downcast_ref::<IntrospectBody>() {
                    match &body.msg {
                        IntrospectMsg::CreateTable(_) => create_tables += 1,
                        IntrospectMsg::UpdateTable(_) => update_tables += 1,
                        IntrospectMsg::InsertsFields(event) => {
                            inserts_fields += 1;
                            inserted_records += event.records.len();
                        }
                        IntrospectMsg::DeleteRecords(event) => {
                            delete_records += event.rows.len();
                        }
                        _ => {}
                    }
                    processed += 1;
                    msgs.push(body);
                }
            }
        }
        self.process_messages(msgs).await?;
        if processed > 0 {
            tracing::info!(
                target: LOGGING_TARGET,
                processed,
                create_tables,
                update_tables,
                inserts_fields,
                inserted_records,
                delete_records,
                "Processed introspect envelopes"
            );
            ::metrics::counter!("torii_introspect_sink_messages_total", "message" => "create_table")
                .increment(create_tables as u64);
            ::metrics::counter!("torii_introspect_sink_messages_total", "message" => "update_table")
                .increment(update_tables as u64);
            ::metrics::counter!("torii_introspect_sink_messages_total", "message" => "inserts_fields")
                .increment(inserts_fields as u64);
            ::metrics::counter!("torii_introspect_sink_records_total", "message" => "inserts_fields")
                .increment(inserted_records as u64);
            ::metrics::counter!("torii_introspect_sink_records_total", "message" => "delete_records")
                .increment(delete_records as u64);
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
        self.initialize_introspect_pg_sink().await?;
        tracing::info!(
            target: LOGGING_TARGET,
            "Initialized introspect Postgres sink"
        );
        Ok(())
    }
}
