use rusqlite::Connection;

use crate::{extractor::PathfinderExtractor, fetcher::EventFetcher};

const DB_PATH: &str = "/mnt/store/mainnet.sqlite";

pub fn make_connection() -> Connection {
    Connection::open(DB_PATH).expect("failed to open database")
}

#[test]
fn test_emitted_events() {
    let mut extractor =
        PathfinderExtractor::new(DB_PATH, 2000, 0, 4000000).expect("failed to create extractor");
    for n in 0..10000 {
        let (blocks, events) = extractor.next_batch().expect("failed to fetch batch");
        println!(
            "Fetched {} blocks and {} events",
            blocks.len(),
            events.len()
        );
        if blocks.is_empty() || n >= 20 {
            break;
        }
    }
}
