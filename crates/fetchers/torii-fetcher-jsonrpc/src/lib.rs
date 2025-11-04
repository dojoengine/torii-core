use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use starknet::core::types::requests::GetEventsRequest;
use starknet::core::types::{
    BlockId, BlockTag, EventFilter, EventFilterWithPage, EventsPage, ResultPageRequest,
};
use starknet::providers::{
    jsonrpc::HttpTransport, JsonRpcClient, Provider, ProviderRequestData, ProviderResponseData,
};
use tokio::time::sleep;
use torii_core::{FetchOptions, FetchOutcome, FetchPlan, Fetcher, FetcherCursor, FieldElement};
use tracing::{info, trace, warn};
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
    #[serde(default)]
    pub batch_requests: bool,
    #[serde(default)]
    pub max_events_per_cycle: Option<usize>,
}

pub struct JsonRpcFetcher {
    provider: JsonRpcClient<HttpTransport>,
    chunk_size: u64,
    request_retry: usize,
    request_backoff: Duration,
    address_backoff: Duration,
    use_batch_requests: bool,
    max_events_per_cycle: Option<usize>,
}

impl JsonRpcFetcher {
    pub fn new(config: JsonRpcFetcherConfig) -> Result<Self> {
        let url = Url::parse(&config.rpc_url)?;
        let provider = JsonRpcClient::new(HttpTransport::new(url));

        let chunk_size = config.chunk_size.unwrap_or(512).max(1);
        let request_retry = config.request_retry.unwrap_or(0);
        let request_backoff = Duration::from_millis(config.request_backoff_ms.unwrap_or(250));
        let address_backoff = Duration::from_millis(config.address_backoff_ms.unwrap_or(0));
        let use_batch_requests = config.batch_requests;
        let max_events_per_cycle = config.max_events_per_cycle;

        Ok(Self {
            provider,
            chunk_size,
            request_retry,
            request_backoff,
            address_backoff,
            use_batch_requests,
            max_events_per_cycle,
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

    fn build_get_events_request(
        &self,
        filter: &EventFilter,
        continuation: Option<String>,
    ) -> ProviderRequestData {
        ProviderRequestData::GetEvents(GetEventsRequest {
            filter: EventFilterWithPage {
                event_filter: filter.clone(),
                result_page_request: ResultPageRequest {
                    continuation_token: continuation,
                    chunk_size: self.chunk_size,
                },
            },
        })
    }

    async fn batch_get_events(&self, requests: &[ProviderRequestData]) -> Result<Vec<EventsPage>> {
        let mut attempts = 0;
        loop {
            match self.provider.batch_requests(requests).await {
                Ok(responses) => {
                    let mut pages = Vec::with_capacity(responses.len());
                    for response in responses {
                        match response {
                            ProviderResponseData::GetEvents(page) => pages.push(page),
                            other => {
                                return Err(anyhow!(
                                    "unexpected response type, expected get_events, got {:?}",
                                    other
                                ))
                            }
                        }
                    }
                    return Ok(pages);
                }
                Err(err) if attempts < self.request_retry => {
                    attempts += 1;
                    warn!(
                        target: "torii_fetcher_jsonrpc",
                        attempt = attempts,
                        error = ?err,
                        "batch get_events failed, backing off"
                    );
                    sleep(self.request_backoff).await;
                }
                Err(err) => return Err(anyhow!(err)),
            }
        }
    }
}

struct FilterContext {
    address: FieldElement,
    filter: EventFilter,
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

            let continuation = cursor.and_then(|c| c.get_continuation(address));

            contexts.push(FilterContext {
                address: *address,
                filter: event_filter,
                continuation,
            });
        }

        if let Some(max_total) = self.max_events_per_cycle {
            if !contexts.is_empty() {
                let chunk = self.chunk_size as usize;
                if chunk > 0 {
                    let allowed = std::cmp::max(1, max_total / chunk);
                    if contexts.len() > allowed {
                        contexts.truncate(allowed);
                    }
                }
            }
        }

        let mut aggregated_events = Vec::new();
        let mut next_cursor = cursor.cloned().unwrap_or_default();

        if self.use_batch_requests {
            let requests: Vec<_> = contexts
                .iter()
                .map(|context| {
                    self.build_get_events_request(&context.filter, context.continuation.clone())
                })
                .collect();
            let pages = self.batch_get_events(&requests).await?;

            for (context, page) in contexts.into_iter().zip(pages.into_iter()) {
                let continuation_token = page.continuation_token.clone();
                let limit = per_filter_limit.min(page.events.len());
                aggregated_events.extend(page.events.into_iter().take(limit));
                next_cursor.set_continuation(&context.address, continuation_token);
            }
        } else {
            for context in contexts.into_iter() {
                let page = self
                    .get_events_page(&context.filter, context.continuation.clone())
                    .await?;
                let continuation_token = page.continuation_token.clone();
                let limit = per_filter_limit.min(page.events.len());
                aggregated_events.extend(page.events.into_iter().take(limit));
                next_cursor.set_continuation(&context.address, continuation_token);

                if !self.address_backoff.is_zero() {
                    sleep(self.address_backoff).await;
                }
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
