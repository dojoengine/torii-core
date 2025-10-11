//! High-level runtime helpers for running the fetch/decode/sink pipeline.

use std::sync::Arc;

use anyhow::Result;
use tracing::Instrument;

use crate::{fetcher::Fetcher, sink::Sink, types::Batch, DecoderRegistry};

/// Execute a single fetch/decode/dispatch cycle using the provided components.
/// TODO: This function is merely a place holder for now, and must be replaced
/// by an actual logic of fetching event while the chain is mining blocks.
pub async fn run_once_batch(
    fetcher: &dyn Fetcher,
    registry: &DecoderRegistry,
    sinks: &[Arc<dyn Sink>],
) -> Result<()> {
    let plan = registry.fetch_plan();
    tracing::debug!(
        target: "torii_core",
        addresses = plan.contract_addresses.len(),
        selectors = plan.selectors.len(),
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
        selectors = plan.selectors.len()
    );
    let raw_events = fetcher.fetch(&plan).instrument(span.clone()).await?;

    if raw_events.is_empty() {
        tracing::debug!("no events fetched");
        return Ok(());
    }

    let mut envelopes = Vec::new();

    // TODO: With this naive approach, we are going through the vec of events
    // once per decoder... Which may be inefficient with a big number of decoders.
    // Creating a map of decoders by selector may be a better approach as it was done in the older
    // Torii implementation, but this must be adapted to the new architecture.
    for ev in &raw_events {
        for decoder_arc in registry.decoders() {
            if decoder_arc.matches(ev) {
                // Clone the Arc to get owned access, then try to get mutable reference
                let mut decoder_owned = decoder_arc.clone();
                if let Some(decoder) = Arc::get_mut(&mut decoder_owned) {
                    envelopes.push(decoder.decode(&ev).await?);
                } else {
                    // If Arc has multiple references, we can't get exclusive access
                    // This shouldn't happen in normal single-threaded iteration
                    anyhow::bail!(
                        "Cannot get exclusive access to decoder: multiple references exist"
                    );
                }
                break;
            }
        }
    }

    if envelopes.is_empty() {
        tracing::debug!("decoders emitted no envelopes");
        return Ok(());
    }

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

    Ok(())
}
