use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use starknet::core::types::{EmittedEvent, EventFilter, EventsPage};
use starknet::providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider};
use tokio::time::sleep;
use torii_core::{FetchPlan, Fetcher};
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
}

#[async_trait]
impl Fetcher for JsonRpcFetcher {
    async fn fetch(&self, plan: &FetchPlan) -> Result<Vec<EmittedEvent>> {
        if plan.contract_addresses.is_empty() && plan.selectors.is_empty() {
            return Ok(Vec::new());
        }

        info!(
            target: "torii_fetcher_jsonrpc",
            addresses = plan.contract_addresses.len(),
            selectors = plan.selectors.len(),
            chunk_size = self.chunk_size,
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
                "fetch cycle detail"
            );
        }

        let mut filters = Vec::new();
        for (address, selectors) in plan.address_selectors.iter() {
            let key_patterns = if selectors.is_empty() {
                vec![vec![]]
            } else {
                vec![selectors.iter().map(|key| *key).collect::<Vec<_>>()]
            };

            filters.push((
                EventFilter {
                    //from_block: Some(BlockId::Number(0)),
                    //to_block: Some(BlockId::Tag(BlockTag::Latest)),
                    from_block: None,
                    to_block: None,
                    address: Some(*address),
                    keys: Some(key_patterns),
                },
                true,
            ));
        }

        let mut events = Vec::new();

        let filter_count = filters.len();
        for (index, (filter, throttle)) in filters.into_iter().enumerate() {
            debug!(
                target: "torii_fetcher_jsonrpc",
                filter_index = index,
                filter = ?filter,
                "fetching events for filter"
            );
            let mut continuation: Option<String> = None;
            let mut pages = 0;

            loop {
                let page = self.get_events_page(&filter, continuation.clone()).await?;
                pages += 1;

                if page.events.is_empty() {
                    // Empty pages may be returned even if the continuation token is not None.
                    // In this case, we want to break the loop since it may be a node
                    // specific behavior (like Katana does).
                    break;
                }

                let continuation_token = page.continuation_token.clone();

                debug!(
                    target: "torii_fetcher_jsonrpc",
                    filter_index = index,
                    page_number = pages,
                    events = page.events.len(),
                    has_more = continuation_token.is_some(),
                    "received events page"
                );
                events.extend(page.events.into_iter());

                if let Some(token) = continuation_token {
                    continuation = Some(token);
                } else {
                    break;
                }
            }

            if throttle && !self.address_backoff.is_zero() && index + 1 < filter_count {
                sleep(self.address_backoff).await;
            }
        }

        info!(
            target: "torii_fetcher_jsonrpc",
            events = events.len(),
            filters = filter_count,
            "completed fetch cycle"
        );
        Ok(events)
    }
}
