use anyhow::{anyhow, Result as AnyResult};
use async_trait::async_trait;
use torii::etl::{Envelope, EventContext, ExtractionBatch, Sink, TypeId};
use torii_introspect::events::{IntrospectBody, IntrospectMsg};

pub struct PostgresSink {
    label: String,
    pool: PgPool,
    manager: TableManager,
}

struct MessageWithContext {
    msg: IntrospectMsg,
    metadata: EventContext,
}

#[async_trait]
impl Sink for PostgresSink {
    fn name(&self) -> &'static str {
        "introspect-postgres"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("introspect")]
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> AnyResult<()> {
        for envelope in envelopes {
            if let Some(body) = envelope.downcast_ref::<IntrospectBody>() {
                let context = batch
                    .get_event_context(&body.transaction_hash, body.from_address)
                    .ok_or(anyhow!("Failed to get event context"))?;
            }
        }
    }
}
