use anyhow::{Context, Result};
use starknet::core::types::{BlockId, BlockTag, EmittedEvent, EventFilter, Felt};
use starknet::providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider};
use url::Url;

/// Fetches all events from the given address.
///
/// TODO: we may add a filter on the selectors. But currently all the events
/// from the contract are fetched.
pub async fn fetch_all(rpc_url: &str, filter: &EventFilter) -> Result<Vec<EmittedEvent>> {
    let rpc_url = Url::parse(rpc_url).context("invalid RPC URL")?;
    let client = JsonRpcClient::new(HttpTransport::new(rpc_url));

    let mut continuation: Option<String> = None;
    let mut collected: Vec<EmittedEvent> = Vec::new();

    loop {
        let page = client
            .get_events(filter.clone(), continuation.clone(), 512)
            .await
            .context("failed to fetch events")?;

        let last_block = page
            .events
            .last()
            .map(|event| event.block_number)
            .unwrap_or(None);

        println!(
            "page fetched with {} events (block #{})",
            page.events.len(),
            last_block.unwrap_or_default()
        );

        collected.extend(page.events.into_iter());

        match page.continuation_token {
            Some(token) => continuation = Some(token),
            None => break,
        }
    }

    Ok(collected)
}
