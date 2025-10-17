use anyhow::Error;
use dojo_introspect_events::{
    DojoEvent, EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    StoreDelRecord, StoreSetRecord, StoreUpdateMember, StoreUpdateRecord,
};
use dojo_types_manager::{DojoManager, JsonStore};
use resolve_path::PathResolveExt;
use starknet_types_core::felt::Felt;
use std::time::Instant;
use std::{collections::HashMap, path::PathBuf};
use torii_core::{Batch, Decoder, Sink};
use torii_decoder_introspect::IntrospectDecoder;
use torii_sink_sqlite::SqliteSink;
use torii_test_utils::{EventIterator, FakeProvider};
const DATA_PATH: &str = "~/tc-tests/blob-arena";

fn get_event_type(event: &starknet::core::types::EmittedEvent) -> (String, String) {
    let selector = event.keys[0];
    if selector == ModelRegistered::SELECTOR {
        ("ModelRegistered".to_string(), "Mr".to_string())
    } else if selector == ModelUpgraded::SELECTOR {
        ("ModelUpgraded".to_string(), "Mu".to_string())
    } else if selector == EventRegistered::SELECTOR {
        ("EventRegistered".to_string(), "Er".to_string())
    } else if selector == EventUpgraded::SELECTOR {
        ("EventUpgraded".to_string(), "Eu".to_string())
    } else if selector == StoreSetRecord::SELECTOR {
        ("StoreSetRecord".to_string(), "Rs".to_string())
    } else if selector == StoreUpdateRecord::SELECTOR {
        ("StoreUpdateRecord".to_string(), "Ru".to_string())
    } else if selector == StoreUpdateMember::SELECTOR {
        ("StoreUpdateMember".to_string(), "Fu".to_string())
    } else if selector == StoreDelRecord::SELECTOR {
        ("StoreDelRecord".to_string(), "Rd".to_string())
    } else if selector == EventEmitted::SELECTOR {
        ("EventEmitted".to_string(), "E".to_string())
    } else {
        ("Unknown".to_string(), "Un".to_string())
    }
}

#[tokio::main]
async fn main() {
    let data_path = PathBuf::from(DATA_PATH).resolve().into_owned();
    let manager_path = data_path.join("manager");
    let model_contracts_path = data_path.join("model-contracts");
    let sql_sink_path = data_path.join("sql-sink");
    let events_path = data_path.join("events");
    println!("Manager Path: {manager_path:#?}");
    println!("Model Contracts Path: {model_contracts_path:#?}");
    println!("SQL Sink Path: {sql_sink_path:#?}");
    println!("Events Path: {events_path:#?}");
    let manager = DojoManager::new(JsonStore::new(&manager_path)).unwrap();
    let fetcher = FakeProvider {
        file_path: model_contracts_path,
    };
    let decoder = IntrospectDecoder {
        filter: Default::default(),
        manager,
        fetcher,
    };
    let mut contract_labels: HashMap<Felt, String> = HashMap::new();
    contract_labels.insert(
        Felt::from_hex_unchecked(
            "0x2d26295d6c541d64740e1ae56abc079b82b22c35ab83985ef8bd15dc0f9edfb",
        ),
        "BlobArena".to_string(),
    );
    let sink = SqliteSink::connect(
        "sql-sink".to_string(),
        &sql_sink_path.into_os_string().to_str().unwrap(),
        None,
        contract_labels,
    )
    .await
    .unwrap();

    let mut decoder_errors: Vec<Error> = vec![];
    let mut sink_errors: Vec<Error> = vec![];

    let events = EventIterator::new(events_path);
    let now = Instant::now();

    for event in events {
        let (name, _) = get_event_type(&event);
        if name == "Unknown" {
            continue;
        }
        match decoder.decode(&event).await {
            Ok(envelope) => {
                match sink
                    .handle_batch(Batch {
                        items: vec![envelope],
                    })
                    .await
                {
                    Err(err) => {
                        println!("\nError Handling event: {name:#?}");
                        println!("Error: {err:#?}");
                        sink_errors.push(err);
                    }
                    _ => (),
                }
            }
            Err(err) => {
                println!("\nError Decoding event: {name:#?}");
                println!("Error: {:#?}", &err);
                println!("---------------");
                println!("{event:#?}");
                println!("---------------");
                decoder_errors.push(err);
            }
        }
    }
    let elapsed = now.elapsed();
    println!(
        "\nElapsed: {elapsed:.2?} decoder errors: {} sink errors: {}",
        decoder_errors.len(),
        sink_errors.len()
    );
    println!("Decoder Errors:");
    println!("{:#?}", decoder_errors);
    println!("Sink Errors:");
    println!("{:#?}", sink_errors);
}

// fn main() {
//     let contracts_path =
//         canonicalize(PathBuf::from("./test-data/blob-arena/model-contracts")).unwrap();
//     let schema = read_model_schema(
//         &contracts_path,
//         Felt::from_hex_unchecked(
//             "0x39da8317e138fead2fe5a894ab103cd73a9173707183a8b7c404533ac4d301d",
//         ),
//     )
//     .unwrap();
//     println!("{schema:#?}");
// }
