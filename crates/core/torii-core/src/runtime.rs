//! High-level runtime helpers for running the fetch/decode/sink pipeline.

use std::sync::Arc;

use anyhow::Result;
use serde::Deserialize;
use tracing::Instrument;

use crate::{
    fetcher::{FetchOptions, Fetcher},
    sink::Sink,
    types::{Batch, FetchOutcome},
    DecoderRegistry,
};

const DEFAULT_PROCESS_EVENTS_BATCH_SIZE: usize = 2_560;
const DEFAULT_FILTER_CONCURRENCY: usize = 1;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub process_events_batch_size: usize,
    pub filter_concurrency: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            process_events_batch_size: DEFAULT_PROCESS_EVENTS_BATCH_SIZE,
            filter_concurrency: DEFAULT_FILTER_CONCURRENCY,
        }
    }
}

impl RuntimeConfig {
    pub fn fetch_options(&self) -> FetchOptions {
        FetchOptions {
            per_filter_event_limit: self.process_events_batch_size.max(1),
            max_concurrent_filters: self.filter_concurrency.max(1),
        }
    }
}

/// Execute a single fetch/decode/dispatch cycle using the provided components.
/// TODO: This function is merely a place holder for now, and must be replaced
/// by an actual logic of fetching event while the chain is mining blocks.
pub async fn run_once_batch(
    fetcher: &dyn Fetcher,
    registry: &DecoderRegistry,
    sinks: &[Arc<dyn Sink>],
) -> Result<()> {
    let options = RuntimeConfig::default().fetch_options();
    run_once_batch_with_options(fetcher, registry, sinks, options).await
}

/// Same as [`run_once_batch`] but using the provided runtime configuration.
pub async fn run_once_batch_with_config(
    fetcher: &dyn Fetcher,
    registry: &DecoderRegistry,
    sinks: &[Arc<dyn Sink>],
    config: &RuntimeConfig,
) -> Result<()> {
    let options = config.fetch_options();
    run_once_batch_with_options(fetcher, registry, sinks, options).await
}

async fn run_once_batch_with_options(
    fetcher: &dyn Fetcher,
    registry: &DecoderRegistry,
    sinks: &[Arc<dyn Sink>],
    options: FetchOptions,
) -> Result<()> {
    let plan = registry.fetch_plan();
    tracing::debug!(
        target: "torii_core",
        addresses = plan.contract_addresses.len(),
        selectors = plan.selectors.len(),
        address_selectors = ?plan.address_selectors,
        per_filter_limit = options.per_filter_event_limit,
        filter_concurrency = options.max_concurrent_filters,
        "prepared fetch plan"
    );
    if tracing::enabled!(tracing::Level::TRACE) {
        let addresses: Vec<String> = plan
            .contract_addresses
            .iter()
            .map(|addr| format!("{addr:#x}"))
            .collect();
        let selectors: Vec<String> = plan
            .selectors
            .iter()
            .map(|sel| format!("{sel:#x}"))
            .collect();
        tracing::trace!(
            target: "torii_core",
            ?addresses,
            ?selectors,
            "fetch plan detail"
        );
    }
    let span = tracing::info_span!(
        "batch",
        addresses = plan.contract_addresses.len(),
        selectors = plan.selectors.len(),
        per_filter_limit = options.per_filter_event_limit,
        filter_concurrency = options.max_concurrent_filters
    );

    let mut cursor = None;

    loop {
        let outcome = fetcher
            .fetch(&plan, cursor.as_ref(), &options)
            .instrument(span.clone())
            .await?;
        let FetchOutcome {
            events: raw_events,
            cursor: next_cursor,
        } = outcome;
        let has_more = next_cursor.has_more();

        if raw_events.is_empty() {
            if has_more {
                tracing::debug!("fetcher returned no events but indicated more data; continuing");
                cursor = Some(next_cursor);
                continue;
            }
            tracing::debug!("no events fetched");
            break;
        }

        let mut envelopes = Vec::new();

        // TODO: With this naive approach, we are going through the vec of events
        // once per decoder... Which may be inefficient with a big number of decoders.
        // Creating a map of decoders by selector may be a better approach as it was done in the older
        // Torii implementation, but this must be adapted to the new architecture.
        for ev in &raw_events {
            for decoder in registry.decoders() {
                if decoder.matches(ev) {
                    envelopes.push(decoder.decode(ev).await?);
                    break;
                }
            }
        }

        if envelopes.is_empty() {
            tracing::debug!("decoders emitted no envelopes");
        } else {
            let batch = Batch { items: envelopes };
            for (idx, sink) in sinks.iter().enumerate() {
                tracing::debug!(
                    target: "torii_sinks",
                    sink_index = idx,
                    sink_label = sink.label(),
                    items = batch.items.len(),
                    "dispatching batch to sink"
                );
                sink.handle_batch(batch.clone()).await?;
                tracing::trace!(
                    target: "torii_sinks",
                    sink_index = idx,
                    sink_label = sink.label(),
                    "sink completed batch"
                );
            }
        }

        if has_more {
            cursor = Some(next_cursor);
        } else {
            cursor = None;
            break;
        }
    }

    Ok(())
}
