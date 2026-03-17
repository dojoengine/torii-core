use itertools::Itertools;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::sync::Arc;
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::store::postgres::PgStore;
use torii_dojo::DojoToriiError;
use torii_introspect_postgres_sink::IntrospectPgDb;
use torii_test_utils::{resolve_path_like, EventIterator, FakeProvider};

const DB_URL: &str = "postgres://torii:torii@localhost:5432/torii";
const CHAIN_DATA_PATH: &str = "~/tc-tests/pistols-2";
const SCHEMA_NAME: &str = "pistols";
// const CHAIN_DATA_PATH: &str = "~/tc-tests/blob-arena";
// const SCHEMA_NAME: &str = "blob_arena";
const BATCH_SIZE: usize = 1000;

async fn run_events(
    events: &mut EventIterator,
    provider: FakeProvider,
    pool: Arc<PgPool>,
    end: Option<u64>,
    event_n: &mut u32,
    success: &mut u32,
) -> bool {
    let decoder = DojoDecoder::<PgStore<_>, _>::new(pool.clone(), provider);
    let db = IntrospectPgDb::new(pool.clone(), SCHEMA_NAME);
    decoder.store.initialize().await.unwrap();
    decoder.load_tables(&[]).await.unwrap();
    db.initialize_introspect_pg_sink().await.unwrap();
    db.load_tables_no_commit(decoder.get_tables().unwrap())
        .unwrap();
    let mut running = true;
    let mut this_run = 0;
    while running {
        let mut msgs = Vec::with_capacity(BATCH_SIZE);
        for _ in 0..BATCH_SIZE {
            let Some(event) = events.next() else {
                running = false;
                break;
            };
            *event_n += 1;
            this_run += 1;
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
        let msgs_ref = msgs.iter().collect_vec();
        for res in db.process_messages(msgs_ref).await.unwrap() {
            match res {
                Err(err) => println!("Failed to process message: {err:?}"),
                Ok(()) => *success += 1,
            }
        }
        println!(
            "Processed batch of events, total events processed: {event_n}, successful: {success}"
        );
        if let Some(end) = end {
            if end <= this_run as u64 {
                println!("Reached end of event range, stopping");
                return true;
            }
        }
    }
    false
}

#[tokio::main]
async fn main() {
    let chain_path = resolve_path_like(CHAIN_DATA_PATH);
    let events_path = chain_path.join("events");
    let contracts_path = chain_path.join("model-contracts");
    let provider = FakeProvider::new(contracts_path);
    let mut event_iterator = EventIterator::new(events_path);
    let pool = Arc::new(PgPoolOptions::new().connect(DB_URL).await.unwrap());
    let mut event_n = 0;
    let mut success = 0;
    while run_events(
        &mut event_iterator,
        provider.clone(),
        pool.clone(),
        Some(50000),
        &mut event_n,
        &mut success,
    )
    .await
    {}
    println!("Finished processing events");
}
