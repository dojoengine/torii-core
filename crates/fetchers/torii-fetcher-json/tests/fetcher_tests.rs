use std::collections::HashSet;
use std::fs;

use starknet::core::types::Felt;
use torii_core::{ContractFilter, FetchOptions, FetchPlan, Fetcher};
use torii_fetcher_json::{JsonFetcher, JsonFetcherConfig};

#[tokio::test]
async fn test_json_fetcher_direct_array() {
    // Create a temporary JSON file with direct array format
    let temp_dir = std::env::temp_dir();
    let file_path = temp_dir.join("test_events_array.json");

    let events_json = r#"[
        {
            "from_address": "0x1",
            "keys": ["0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9"],
            "data": ["0x1", "0x2"],
            "block_hash": "0x123",
            "block_number": 1,
            "transaction_hash": "0x456"
        }
    ]"#;

    fs::write(&file_path, events_json).unwrap();

    let config = JsonFetcherConfig {
        file_path: file_path.to_string_lossy().to_string(),
        chunk_size: Some(10),
    };

    let fetcher = JsonFetcher::new(config).unwrap();
    assert_eq!(fetcher.all_events().len(), 1);

    // Cleanup
    fs::remove_file(file_path).ok();
}

#[tokio::test]
async fn test_json_fetcher_with_metadata() {
    // Create a temporary JSON file with metadata format
    let temp_dir = std::env::temp_dir();
    let file_path = temp_dir.join("test_events_metadata.json");

    let events_json = r#"{
        "world_address": "0x123",
        "events": [
            {
                "from_address": "0x1",
                "keys": ["0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9"],
                "data": ["0x1"],
                "block_hash": "0x123",
                "block_number": 1,
                "transaction_hash": "0x456"
            }
        ]
    }"#;

    fs::write(&file_path, events_json).unwrap();

    let config = JsonFetcherConfig {
        file_path: file_path.to_string_lossy().to_string(),
        chunk_size: Some(5),
    };

    let fetcher = JsonFetcher::new(config).unwrap();
    assert_eq!(fetcher.all_events().len(), 1);

    // Cleanup
    fs::remove_file(file_path).ok();
}

#[tokio::test]
async fn test_json_fetcher_pagination() {
    let temp_dir = std::env::temp_dir();
    let file_path = temp_dir.join("test_events_pagination.json");

    // Create multiple events for the same address
    let events_json = r#"[
        {
            "from_address": "0x1",
            "keys": ["0x1"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 1,
            "transaction_hash": "0x456"
        },
        {
            "from_address": "0x1",
            "keys": ["0x2"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 2,
            "transaction_hash": "0x457"
        },
        {
            "from_address": "0x1",
            "keys": ["0x3"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 3,
            "transaction_hash": "0x458"
        }
    ]"#;

    fs::write(&file_path, events_json).unwrap();

    let config = JsonFetcherConfig {
        file_path: file_path.to_string_lossy().to_string(),
        chunk_size: Some(2), // Fetch 2 events at a time
    };

    let fetcher = JsonFetcher::new(config).unwrap();

    let address = Felt::from_hex_unchecked("0x1");
    let mut plan = FetchPlan::default();
    plan.address_selectors.insert(
        address,
        ContractFilter {
            selectors: HashSet::new(),
            deployed_at_block: None,
        },
    );

    let options = FetchOptions {
        per_filter_event_limit: 2,
        max_concurrent_filters: 1,
    };

    // First fetch: should get 2 events
    let outcome1 = fetcher.fetch(&plan, None, &options).await.unwrap();
    assert_eq!(outcome1.events.len(), 2);
    assert!(outcome1.cursor.has_more());

    // Second fetch: should get 1 event
    let outcome2 = fetcher
        .fetch(&plan, Some(&outcome1.cursor), &options)
        .await
        .unwrap();
    assert_eq!(outcome2.events.len(), 1);
    assert!(!outcome2.cursor.has_more());

    // Third fetch: should get 0 events
    let outcome3 = fetcher
        .fetch(&plan, Some(&outcome2.cursor), &options)
        .await
        .unwrap();
    assert_eq!(outcome3.events.len(), 0);
    assert!(!outcome3.cursor.has_more());

    // Cleanup
    fs::remove_file(file_path).ok();
}

#[tokio::test]
async fn test_json_fetcher_multiple_contracts() {
    let temp_dir = std::env::temp_dir();
    let file_path = temp_dir.join("test_events_multiple_contracts.json");

    // Create events for two different contract addresses
    let events_json = r#"[
        {
            "from_address": "0x1",
            "keys": ["0xa"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 1,
            "transaction_hash": "0x456"
        },
        {
            "from_address": "0x2",
            "keys": ["0xb"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 2,
            "transaction_hash": "0x457"
        },
        {
            "from_address": "0x1",
            "keys": ["0xc"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 3,
            "transaction_hash": "0x458"
        },
        {
            "from_address": "0x2",
            "keys": ["0xd"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 4,
            "transaction_hash": "0x459"
        }
    ]"#;

    fs::write(&file_path, events_json).unwrap();

    let config = JsonFetcherConfig {
        file_path: file_path.to_string_lossy().to_string(),
        chunk_size: Some(10),
    };

    let fetcher = JsonFetcher::new(config).unwrap();

    let address1 = Felt::from_hex_unchecked("0x1");
    let address2 = Felt::from_hex_unchecked("0x2");

    let mut plan = FetchPlan::default();
    plan.address_selectors.insert(
        address1,
        ContractFilter {
            selectors: HashSet::new(),
            deployed_at_block: None,
        },
    );
    plan.address_selectors.insert(
        address2,
        ContractFilter {
            selectors: HashSet::new(),
            deployed_at_block: None,
        },
    );

    let options = FetchOptions {
        per_filter_event_limit: 10,
        max_concurrent_filters: 2,
    };

    // Fetch all events
    let outcome = fetcher.fetch(&plan, None, &options).await.unwrap();

    // Should get 4 events total (2 from each contract)
    assert_eq!(outcome.events.len(), 4);

    // Verify we got events from both addresses
    let addresses: std::collections::HashSet<_> =
        outcome.events.iter().map(|e| e.from_address).collect();
    assert_eq!(addresses.len(), 2);
    assert!(addresses.contains(&address1));
    assert!(addresses.contains(&address2));

    // Cleanup
    fs::remove_file(file_path).ok();
}

#[tokio::test]
async fn test_json_fetcher_pagination_with_selector_filtering_single_contract() {
    let temp_dir = std::env::temp_dir();
    let file_path = temp_dir.join("test_events_selector_single.json");

    // Create events with different selectors for one contract
    let events_json = r#"[
        {
            "from_address": "0x1",
            "keys": ["0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9"],
            "data": ["0x1"],
            "block_hash": "0x123",
            "block_number": 1,
            "transaction_hash": "0x456"
        },
        {
            "from_address": "0x1",
            "keys": ["0x1234567890abcdef"],
            "data": ["0x2"],
            "block_hash": "0x123",
            "block_number": 2,
            "transaction_hash": "0x457"
        },
        {
            "from_address": "0x1",
            "keys": ["0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9"],
            "data": ["0x3"],
            "block_hash": "0x123",
            "block_number": 3,
            "transaction_hash": "0x458"
        },
        {
            "from_address": "0x1",
            "keys": ["0x1234567890abcdef"],
            "data": ["0x4"],
            "block_hash": "0x123",
            "block_number": 4,
            "transaction_hash": "0x459"
        },
        {
            "from_address": "0x1",
            "keys": ["0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9"],
            "data": ["0x5"],
            "block_hash": "0x123",
            "block_number": 5,
            "transaction_hash": "0x460"
        }
    ]"#;

    fs::write(&file_path, events_json).unwrap();

    let config = JsonFetcherConfig {
        file_path: file_path.to_string_lossy().to_string(),
        chunk_size: Some(2), // Fetch 2 events at a time
    };

    let fetcher = JsonFetcher::new(config).unwrap();

    let address = Felt::from_hex_unchecked("0x1");
    let selector = Felt::from_hex_unchecked(
        "0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9",
    );

    let mut selectors = HashSet::new();
    selectors.insert(selector);

    let mut plan = FetchPlan::default();
    plan.address_selectors.insert(
        address,
        ContractFilter {
            selectors,
            deployed_at_block: None,
        },
    );

    let options = FetchOptions {
        per_filter_event_limit: 2,
        max_concurrent_filters: 1,
    };

    // First fetch: should get 2 events (filtered to matching selector)
    let outcome1 = fetcher.fetch(&plan, None, &options).await.unwrap();
    assert_eq!(outcome1.events.len(), 2);
    assert!(outcome1.cursor.has_more());

    // Verify all events have the correct selector
    for event in &outcome1.events {
        assert_eq!(event.keys[0], selector);
    }

    // Second fetch: should get 1 event (only 3 total match the selector)
    let outcome2 = fetcher
        .fetch(&plan, Some(&outcome1.cursor), &options)
        .await
        .unwrap();
    assert_eq!(outcome2.events.len(), 1);
    assert!(!outcome2.cursor.has_more());
    assert_eq!(outcome2.events[0].keys[0], selector);

    // Third fetch: should get 0 events
    let outcome3 = fetcher
        .fetch(&plan, Some(&outcome2.cursor), &options)
        .await
        .unwrap();
    assert_eq!(outcome3.events.len(), 0);
    assert!(!outcome3.cursor.has_more());

    // Cleanup
    fs::remove_file(file_path).ok();
}

#[tokio::test]
async fn test_json_fetcher_pagination_with_selector_filtering_multiple_contracts() {
    let temp_dir = std::env::temp_dir();
    let file_path = temp_dir.join("test_events_selector_multiple.json");

    // Create events for two contracts with different selectors
    let events_json = r#"[
        {
            "from_address": "0x1",
            "keys": ["0xaaa"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 1,
            "transaction_hash": "0x456"
        },
        {
            "from_address": "0x1",
            "keys": ["0xbbb"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 2,
            "transaction_hash": "0x457"
        },
        {
            "from_address": "0x2",
            "keys": ["0xccc"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 3,
            "transaction_hash": "0x458"
        },
        {
            "from_address": "0x1",
            "keys": ["0xaaa"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 4,
            "transaction_hash": "0x459"
        },
        {
            "from_address": "0x2",
            "keys": ["0xddd"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 5,
            "transaction_hash": "0x460"
        },
        {
            "from_address": "0x2",
            "keys": ["0xccc"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 6,
            "transaction_hash": "0x461"
        },
        {
            "from_address": "0x1",
            "keys": ["0xaaa"],
            "data": [],
            "block_hash": "0x123",
            "block_number": 7,
            "transaction_hash": "0x462"
        }
    ]"#;

    fs::write(&file_path, events_json).unwrap();

    let config = JsonFetcherConfig {
        file_path: file_path.to_string_lossy().to_string(),
        chunk_size: Some(2), // Fetch 2 events at a time per address
    };

    let fetcher = JsonFetcher::new(config).unwrap();

    let address1 = Felt::from_hex_unchecked("0x1");
    let address2 = Felt::from_hex_unchecked("0x2");
    let selector1 = Felt::from_hex_unchecked("0xaaa"); // Contract 1 selector
    let selector2 = Felt::from_hex_unchecked("0xccc"); // Contract 2 selector

    let mut selectors1 = HashSet::new();
    selectors1.insert(selector1);

    let mut selectors2 = HashSet::new();
    selectors2.insert(selector2);

    let mut plan = FetchPlan::default();
    plan.address_selectors.insert(
        address1,
        ContractFilter {
            selectors: selectors1,
            deployed_at_block: None,
        },
    );
    plan.address_selectors.insert(
        address2,
        ContractFilter {
            selectors: selectors2,
            deployed_at_block: None,
        },
    );

    let options = FetchOptions {
        per_filter_event_limit: 2,
        max_concurrent_filters: 2,
    };

    // First fetch: should get 4 events (2 from each contract matching selectors)
    // Contract 0x1 has 3 events with 0xaaa, Contract 0x2 has 2 events with 0xccc
    let outcome1 = fetcher.fetch(&plan, None, &options).await.unwrap();
    assert_eq!(outcome1.events.len(), 4); // 2 from each contract
    assert!(outcome1.cursor.has_more()); // Contract 1 still has 1 more event

    // Verify selectors
    for event in &outcome1.events {
        if event.from_address == address1 {
            assert_eq!(event.keys[0], selector1);
        } else {
            assert_eq!(event.keys[0], selector2);
        }
    }

    // Second fetch: should get 1 event (only contract 0x1 has 1 more matching event)
    let outcome2 = fetcher
        .fetch(&plan, Some(&outcome1.cursor), &options)
        .await
        .unwrap();
    assert_eq!(outcome2.events.len(), 1);
    assert!(!outcome2.cursor.has_more());
    assert_eq!(outcome2.events[0].from_address, address1);
    assert_eq!(outcome2.events[0].keys[0], selector1);

    // Third fetch: should get 0 events
    let outcome3 = fetcher
        .fetch(&plan, Some(&outcome2.cursor), &options)
        .await
        .unwrap();
    assert_eq!(outcome3.events.len(), 0);
    assert!(!outcome3.cursor.has_more());

    // Cleanup
    fs::remove_file(file_path).ok();
}
