use anyhow::{anyhow, Result as AnyResult};
use async_trait::async_trait;
use torii::etl::{Envelope, EventContext, ExtractionBatch, Sink, TypeId};
use torii_introspect::events::IntrospectMsg;

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
            if let Some(msg) = envelope.downcast_ref::<IntrospectMsg>() {
                let from_address = envelope
                    .metadata
                    .get("from_address")
                    .ok_or(anyhow!("Missing from_address in metadata"))?;
                let tx_hash = envelope
                    .metadata
                    .get("transaction_hash")
                    .ok_or(anyhow!("Missing transaction_hash in metadata"))?;
                let context = batch
                    .get_event_context(tx_hash.parse()?, from_address.parse()?)
                    .ok_or(anyhow!("Failed to get event context"))?;
            }
        }
    }
}
