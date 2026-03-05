use crate::processor::PostgresSchema;
use anyhow::{anyhow, Result as AnyResult};
use async_trait::async_trait;
use torii::etl::{Envelope, EventContext, ExtractionBatch, Sink, TypeId};
use torii_introspect::events::{IntrospectBody, IntrospectMsg};

pub const LOGGING_TARGET: &str = "torii::sinks::introspect::postgres";

#[async_trait]
impl<T: Send + Sync> Sink for PostgresDb {
    fn name(&self) -> &'static str {
        "introspect-postgres"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("introspect")]
    }
    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> AnyResult<()> {
        let mut messages_with_context = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            if let Some(body) = envelope.downcast_ref::<IntrospectBody>() {
                let context = batch
                    .get_event_context(&body.transaction_hash, body.from_address)
                    .ok_or(anyhow!("Failed to get event context"))?;
                messages_with_context.push(MessageWithContext {
                    msg: &body.msg,
                    context,
                });
            }
        }

        Ok(())
    }
}
