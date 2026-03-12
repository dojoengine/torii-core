use itertools::Itertools;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use torii::etl::EventContext;
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::store::postgres::PgStore;
use torii_dojo::DojoToriiError;
use torii_introspect_postgres_sink::processor::{PgSchema, PostgresSimpleDb};
use torii_test_utils::{resolve_path_like, EventIterator, FakeProvider};

const DB_URL: &str = "postgres://torii:torii@localhost:5432/torii";
// const CHAIN_DATA_PATH: &str = "~/tc-tests/pistols";
const CHAIN_DATA_PATH: &str = "~/tc-tests/blob-arena";
const BATCH_SIZE: usize = 1000;

#[tokio::main]
async fn main() {
    let chain_path = resolve_path_like(CHAIN_DATA_PATH);
    let events_path = chain_path.join("events");
    let contracts_path = chain_path.join("model-contracts");
    let provider = FakeProvider::new(contracts_path);
    let mut event_iterator = EventIterator::new(events_path);

    let pool = Arc::new(PgPoolOptions::new().connect(DB_URL).await.unwrap());
    let decoder = DojoDecoder::<PgStore<_>, _>::new(pool.clone(), provider);
    let db = PostgresSimpleDb::new(pool.clone(), PgSchema::Public);
    decoder.store.initialize().await.unwrap();
    db.migrate_introspect_sink().await.unwrap();
    let context = EventContext::default();
    let mut event_n = 0;
    let mut running = true;
    while running {
        let mut msgs = Vec::with_capacity(BATCH_SIZE);
        for _ in 0..BATCH_SIZE {
            let Some(event) = event_iterator.next() else {
                running = false;
                break;
            };
            event_n += 1;
            match decoder.decode_raw_event(&event).await {
                Ok(msg) => {
                    msgs.push(msg);
                }
                Err(DojoToriiError::UnknownDojoEventSelector(_)) => {
                    println!("Unknown event selector, skipping event");
                }
                Err(err) => {
                    println!("Failed to decode event: {err:?}");
                }
            };
        }
        let msgs_with_context = msgs.iter().map(|msg| (msg, &context)).collect_vec();
        db.process_messages(msgs_with_context).await.unwrap();
        println!("Processed batch of events, total events processed: {event_n}");
    }
}
