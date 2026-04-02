use rusqlite::Connection;

use crate::fetcher::EventFetcher;

const DB_PATH: &str = "/mnt/store/mainnet.sqlite";

pub fn make_connection() -> Connection {
    Connection::open(DB_PATH).expect("failed to open database")
}

#[test]
fn test_emitted_events() {
    let conn = make_connection();
    let events = conn
        .get_emitted_events(1000000, 1000010)
        .expect("failed to fetch events");
    for event in events {
        println!("{event:#?}");
    }
}
