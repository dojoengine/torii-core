//! JSON file-based fetcher for Torii.
//!
//! This fetcher reads events from a JSON file, useful for testing and replaying
//! captured events without requiring a live RPC connection.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use starknet::core::types::EmittedEvent;
use torii_core::{
    FetchOptions, FetchOutcome, FetchPlan, Fetcher, FetcherCursor, FieldElement,
};
use tracing::{info, trace};

/// Configuration for the JSON file fetcher.
#[derive(Debug, Clone, Deserialize)]
pub struct JsonFetcherConfig {
    /// Path to the JSON file containing events.
    pub file_path: String,
    /// Optional chunk size for simulating pagination (defaults to per_filter_event_limit).
    #[serde(default)]
    pub chunk_size: Option<usize>,
}

/// Wrapper struct to support both JSON formats.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum EventsFileFormat {
    /// Direct array of events: `[{...}, {...}]`
    DirectArray(Vec<EmittedEvent>),
    /// Object with events key: `{"world_address": "0x...", "events": [{...}]}`
    WithMetadata { events: Vec<EmittedEvent> },
}

/// Fetcher that reads events from a JSON file.
///
/// Supports two JSON formats:
/// 1. Direct array: `[{...}, {...}]`
/// 2. Object with events field: `{"world_address": "0x...", "events": [{...}]}`
///
/// The cursor is used as an index to simulate pagination through the events.
pub struct JsonFetcher {
    /// All events loaded from the file, grouped by contract address.
    events_by_address: HashMap<FieldElement, Vec<EmittedEvent>>,
    /// Optional chunk size override.
    chunk_size: Option<usize>,
}

impl JsonFetcher {
    /// Creates a new JSON fetcher from the provided configuration.
    pub fn new(config: JsonFetcherConfig) -> Result<Self> {
        let file_path = PathBuf::from(&config.file_path);
        
        info!(
            target: "torii_fetcher_json",
            path = %file_path.display(),
            "loading events from JSON file"
        );

        let contents = fs::read_to_string(&file_path)
            .with_context(|| format!("failed to read file: {}", file_path.display()))?;

        let events = match serde_json::from_str::<EventsFileFormat>(&contents)
            .context("failed to parse JSON file")? {
            EventsFileFormat::DirectArray(events) => events,
            EventsFileFormat::WithMetadata { events } => events,
        };

        info!(
            target: "torii_fetcher_json",
            total_events = events.len(),
            "loaded events from file"
        );

        // Group events by contract address
        let mut events_by_address: HashMap<FieldElement, Vec<EmittedEvent>> = HashMap::new();
        for event in events {
            events_by_address
                .entry(event.from_address)
                .or_insert_with(Vec::new)
                .push(event);
        }

        info!(
            target: "torii_fetcher_json",
            unique_addresses = events_by_address.len(),
            "grouped events by address"
        );

        Ok(Self {
            events_by_address,
            chunk_size: config.chunk_size,
        })
    }

    /// Returns all events for testing purposes.
    pub fn all_events(&self) -> Vec<EmittedEvent> {
        self.events_by_address
            .values()
            .flat_map(|events| events.iter().cloned())
            .collect()
    }

    /// Returns the number of events for a specific address.
    pub fn event_count(&self, address: &FieldElement) -> usize {
        self.events_by_address
            .get(address)
            .map(|events| events.len())
            .unwrap_or(0)
    }
}

#[async_trait]
impl Fetcher for JsonFetcher {
    async fn fetch(
        &self,
        plan: &FetchPlan,
        cursor: Option<&FetcherCursor>,
        options: &FetchOptions,
    ) -> Result<FetchOutcome> {
        if plan.address_selectors.is_empty() {
            return Ok(FetchOutcome::default());
        }

        let chunk_size = self
            .chunk_size
            .unwrap_or(options.per_filter_event_limit)
            .max(1);

        info!(
            target: "torii_fetcher_json",
            addresses = plan.address_selectors.len(),
            chunk_size,
            "starting fetch cycle from JSON"
        );

        let mut aggregated_events = Vec::new();
        let mut next_cursor = cursor.cloned().unwrap_or_default();

        for (address, contract_filter) in plan.address_selectors.iter() {
            // Check if this address has already been fully consumed
            if let Some(cursor) = cursor {
                if cursor.continuations.contains_key(address) 
                    && cursor.get_continuation(address).is_none() {
                    // Already fetched all events for this address
                    trace!(
                        target: "torii_fetcher_json",
                        address = %format!("{:#x}", address),
                        "address already fully consumed"
                    );
                    continue;
                }
            }

            // Get the current index for this address from the cursor
            let current_index: usize = cursor
                .and_then(|c| c.get_continuation(address))
                .and_then(|idx_str| idx_str.parse().ok())
                .unwrap_or(0);

            // Get events for this address
            let events = match self.events_by_address.get(address) {
                Some(events) => events,
                None => {
                    trace!(
                        target: "torii_fetcher_json",
                        address = %format!("{:#x}", address),
                        "no events found for address"
                    );
                    // Mark as complete for this address
                    next_cursor.set_continuation(address, None);
                    continue;
                }
            };

            // Filter events by selectors if specified
            let filtered_events: Vec<EmittedEvent> = if contract_filter.selectors.is_empty() {
                events.clone()
            } else {
                events
                    .iter()
                    .filter(|event| {
                        // Check if any of the event's keys match the requested selectors
                        event.keys.iter().any(|key| contract_filter.selectors.contains(key))
                    })
                    .cloned()
                    .collect()
            };

            if current_index >= filtered_events.len() {
                trace!(
                    target: "torii_fetcher_json",
                    address = %format!("{:#x}", address),
                    current_index,
                    total_events = filtered_events.len(),
                    "reached end of events for address"
                );
                // No more events for this address
                next_cursor.set_continuation(address, None);
                continue;
            }

            // Get the next chunk of events
            let end_index = (current_index + chunk_size).min(filtered_events.len());
            let chunk = &filtered_events[current_index..end_index];
            
            trace!(
                target: "torii_fetcher_json",
                address = %format!("{:#x}", address),
                current_index,
                end_index,
                chunk_size = chunk.len(),
                "fetching event chunk"
            );

            aggregated_events.extend(chunk.iter().cloned());

            // Update cursor with the new index
            if end_index < filtered_events.len() {
                // More events available
                next_cursor.set_continuation(address, Some(end_index.to_string()));
            } else {
                // No more events
                next_cursor.set_continuation(address, None);
            }
        }

        info!(
            target: "torii_fetcher_json",
            events = aggregated_events.len(),
            has_more = next_cursor.has_more(),
            "completed fetch cycle"
        );

        Ok(FetchOutcome::new(aggregated_events, next_cursor))
    }
}
