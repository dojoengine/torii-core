use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{stream, StreamExt};
use serde::Deserialize;
use starknet::core::types::{BlockId, BlockTag, EmittedEvent, EventFilter, EventsPage};
use starknet::providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider};
use tokio::time::sleep;
use torii_core::{FetchOptions, FetchOutcome, FetchPlan, Fetcher, FetcherCursor, FieldElement};
use tracing::{debug, info, trace, warn};
use url::Url;

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcFetcherConfig {
    pub rpc_url: String,
    #[serde(default)]
    pub chunk_size: Option<u64>,
    #[serde(default)]
    pub request_retry: Option<usize>,
    #[serde(default)]
    pub request_backoff_ms: Option<u64>,
    #[serde(default)]
    pub address_backoff_ms: Option<u64>,
}

pub struct JsonRpcFetcher {
    provider: JsonRpcClient<HttpTransport>,
    chunk_size: u64,
    request_retry: usize,
    request_backoff: Duration,
    address_backoff: Duration,
}

impl JsonRpcFetcher {
    pub fn new(config: JsonRpcFetcherConfig) -> Result<Self> {
        let url = Url::parse(&config.rpc_url)?;
        let provider = JsonRpcClient::new(HttpTransport::new(url));

        let chunk_size = config.chunk_size.unwrap_or(512).max(1);
        let request_retry = config.request_retry.unwrap_or(0);
        let request_backoff = Duration::from_millis(config.request_backoff_ms.unwrap_or(250));
        let address_backoff = Duration::from_millis(config.address_backoff_ms.unwrap_or(0));

        Ok(Self {
            provider,
            chunk_size,
            request_retry,
            request_backoff,
            address_backoff,
        })
    }

    async fn get_events_page(
        &self,
        filter: &EventFilter,
        continuation: Option<String>,
    ) -> Result<EventsPage> {
        let mut attempts = 0;
        loop {
            match self
                .provider
                .get_events(filter.clone(), continuation.clone(), self.chunk_size)
                .await
            {
                Ok(page) => return Ok(page),
                Err(err) if attempts < self.request_retry => {
                    attempts += 1;
                    warn!(
                        target: "torii_fetcher_jsonrpc",
                        attempt = attempts,
                        error = ?err,
                        "get_events failed, backing off"
                    );
                    sleep(self.request_backoff).await;
                }
                Err(err) => return Err(anyhow!(err)),
            }
        }
    }

    async fn fetch_filter(
        &self,
        mut context: FilterContext,
        per_filter_limit: usize,
    ) -> Result<FilterResult> {
        let mut collected = Vec::new();
        let mut page = 0usize;

        let mut next_request = context.continuation.clone();
        let mut next_cursor = context.continuation.take();

        let limit = per_filter_limit.max(1);

        loop {
            if collected.len() >= limit {
                debug!(
                    target: "torii_fetcher_jsonrpc",
                    address = ?context.address,
                    limit,
                    pages = page,
                    "per-filter limit reached"
                );
                break;
            }

            let page_data = self
                .get_events_page(&context.filter, next_request.clone())
                .await?;
            page += 1;

            let continuation_token = page_data.continuation_token.clone();

            debug!(
                target: "torii_fetcher_jsonrpc",
                address = ?context.address,
                page,
                events = page_data.events.len(),
                has_more = continuation_token.is_some(),
                last_block = page_data.events.last().and_then(|event| event.block_number),
                "received events page"
            );

            if page_data.events.is_empty() {
                if let Some(token) = continuation_token {
                    next_request = Some(token.clone());
                    next_cursor = Some(token);
                    continue;
                } else {
                    next_cursor = None;
                    break;
                }
            }

            collected.extend(page_data.events.into_iter());

            if let Some(token) = continuation_token {
                next_request = Some(token.clone());
                next_cursor = Some(token);
            } else {
                next_cursor = None;
                break;
            }
        }

        Ok(FilterResult {
            address: context.address,
            events: collected,
            continuation: next_cursor,
        })
    }
}

struct FilterContext {
    address: FieldElement,
    filter: EventFilter,
    continuation: Option<String>,
}

struct FilterResult {
    address: FieldElement,
    events: Vec<EmittedEvent>,
    continuation: Option<String>,
}

#[async_trait]
impl Fetcher for JsonRpcFetcher {
    async fn fetch(
        &self,
        plan: &FetchPlan,
        cursor: Option<&FetcherCursor>,
        options: &FetchOptions,
    ) -> Result<FetchOutcome> {
        if plan.address_selectors.is_empty() {
            return Ok(FetchOutcome::default());
        }

        let per_filter_limit = options.per_filter_event_limit.max(1);
        let max_concurrent_filters = options.max_concurrent_filters.max(1);

        info!(
            target: "torii_fetcher_jsonrpc",
            addresses = plan.address_selectors.len(),
            selectors = plan.selectors.len(),
            chunk_size = self.chunk_size,
            per_filter_limit,
            max_concurrent_filters,
            "starting fetch cycle"
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
            trace!(
                target: "torii_fetcher_jsonrpc",
                ?addresses,
                ?selectors,
                per_filter_limit,
                max_concurrent_filters,
                "fetch cycle detail"
            );
        }

        let mut contexts = Vec::new();
        for (address, contract_filter) in plan.address_selectors.iter() {
            let key_patterns = if contract_filter.selectors.is_empty() {
                vec![vec![]]
            } else {
                vec![contract_filter
                    .selectors
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()]
            };

            let from_block = BlockId::Number(contract_filter.deployed_at_block.unwrap_or(0));
            let event_filter = EventFilter {
                from_block: Some(from_block),
                to_block: Some(BlockId::Tag(BlockTag::Latest)),
                address: Some(*address),
                keys: Some(key_patterns),
            };

            let continuation = cursor.and_then(|c| c.get_continuation_string(address));

            contexts.push(FilterContext {
                address: *address,
                filter: event_filter,
                continuation,
            });
        }

        let mut tasks = stream::iter(
            contexts
                .into_iter()
                .map(|context| async move { self.fetch_filter(context, per_filter_limit).await }),
        )
        .buffer_unordered(max_concurrent_filters);

        let mut aggregated_events = Vec::new();
        let mut next_cursor = cursor.cloned().unwrap_or_default();

        while let Some(result) = tasks.next().await {
            let FilterResult {
                address,
                events,
                continuation,
            } = result?;

            if !events.is_empty() {
                aggregated_events.extend(events);
            }

            next_cursor.set_continuation_string(&address, continuation);

            if max_concurrent_filters == 1 && !self.address_backoff.is_zero() {
                sleep(self.address_backoff).await;
            }
        }

        info!(
            target: "torii_fetcher_jsonrpc",
            events = aggregated_events.len(),
            filters = plan.address_selectors.len(),
            "completed fetch cycle"
        );

        Ok(FetchOutcome::new(aggregated_events, next_cursor))
    }
}
