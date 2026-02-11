//! Async token URI fetching and caching service.
//!
//! Processes `(contract_address, token_id)` requests via a tokio channel.
//! Deduplicates in-flight tasks — if a new request arrives for the same key,
//! the previous task is cancelled so we always apply the latest value.
//!
//! The service fetches `token_uri(token_id)` (ERC721) or `uri(token_id)` (ERC1155),
//! optionally resolves the JSON metadata, and stores the result via a callback.

use starknet::core::types::{Felt, U256};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::MetadataFetcher;

/// Token standard hint for URI fetching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenStandard {
    Erc721,
    Erc1155,
}

/// A request to fetch/update a token's URI and metadata.
#[derive(Debug, Clone)]
pub struct TokenUriRequest {
    /// Contract address
    pub contract: Felt,
    /// Token ID (the low word — high is passed separately for U256)
    pub token_id: U256,
    /// Which standard to use for fetching
    pub standard: TokenStandard,
}

/// Dedupe key for in-flight tasks
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TaskKey {
    contract: Felt,
    token_id: U256,
}

/// Result of a token URI fetch
#[derive(Debug, Clone)]
pub struct TokenUriResult {
    /// Contract address
    pub contract: Felt,
    /// Token ID
    pub token_id: U256,
    /// The raw URI string (e.g. ipfs://..., https://...)
    pub uri: Option<String>,
    /// Resolved JSON metadata (if URI pointed to JSON)
    pub metadata_json: Option<String>,
}

/// Callback trait for storing fetched token URI results.
///
/// Implementors handle persisting the result to their own storage.
#[async_trait::async_trait]
pub trait TokenUriStore: Send + Sync + 'static {
    async fn store_token_uri(&self, result: &TokenUriResult) -> anyhow::Result<()>;
}

/// Handle to send requests to the token URI service.
#[derive(Clone)]
pub struct TokenUriSender {
    tx: mpsc::Sender<TokenUriRequest>,
}

impl TokenUriSender {
    /// Queue a token URI fetch request.
    ///
    /// Returns immediately. The service will deduplicate and process async.
    pub async fn request_update(&self, request: TokenUriRequest) {
        if let Err(e) = self.tx.send(request).await {
            tracing::warn!(
                target: "torii_common::token_uri",
                error = %e,
                "Failed to send token URI request (channel closed)"
            );
        }
    }

    /// Queue updates for a batch of token IDs on the same contract.
    pub async fn request_batch(
        &self,
        contract: Felt,
        token_ids: &[U256],
        standard: TokenStandard,
    ) {
        for &token_id in token_ids {
            self.request_update(TokenUriRequest {
                contract,
                token_id,
                standard,
            })
            .await;
        }
    }
}

/// The background service that processes token URI fetch requests.
pub struct TokenUriService {
    /// Join handle for the background processing loop
    handle: JoinHandle<()>,
}

impl TokenUriService {
    /// Spawn the token URI service.
    ///
    /// Returns a `(TokenUriSender, TokenUriService)` pair.
    /// The sender is cheap to clone and can be shared across sinks.
    /// The service must be kept alive (dropping it cancels the background task).
    pub fn spawn<S: TokenUriStore>(
        fetcher: Arc<MetadataFetcher>,
        store: Arc<S>,
        buffer_size: usize,
        max_concurrent: usize,
    ) -> (TokenUriSender, Self) {
        let (tx, rx) = mpsc::channel(buffer_size);

        let handle = tokio::spawn(Self::run(rx, fetcher, store, max_concurrent));

        let sender = TokenUriSender { tx };
        (sender, Self { handle })
    }

    /// Main processing loop.
    async fn run<S: TokenUriStore>(
        mut rx: mpsc::Receiver<TokenUriRequest>,
        fetcher: Arc<MetadataFetcher>,
        store: Arc<S>,
        max_concurrent: usize,
    ) {
        // Track in-flight tasks by key — new requests cancel the old one
        let in_flight: Arc<Mutex<HashMap<TaskKey, JoinHandle<()>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Semaphore to limit concurrency
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));

        while let Some(request) = rx.recv().await {
            let key = TaskKey {
                contract: request.contract,
                token_id: request.token_id,
            };

            let mut tasks = in_flight.lock().await;

            // Cancel previous task for the same key
            if let Some(old_handle) = tasks.remove(&key) {
                old_handle.abort();
                tracing::debug!(
                    target: "torii_common::token_uri",
                    contract = %format!("{:#x}", key.contract),
                    token_id = %key.token_id,
                    "Cancelled previous fetch (superseded)"
                );
            }

            let fetcher = fetcher.clone();
            let store = store.clone();
            let in_flight = in_flight.clone();
            let sem = semaphore.clone();
            let task_key = key.clone();

            let handle = tokio::spawn(async move {
                // Acquire semaphore permit (limits concurrency)
                let _permit = match sem.acquire().await {
                    Ok(p) => p,
                    Err(_) => return, // semaphore closed
                };

                let uri = match request.standard {
                    TokenStandard::Erc721 => {
                        // token_id as Felt (low word)
                        let token_id_felt = Felt::from(request.token_id.low());
                        fetcher.fetch_token_uri(request.contract, token_id_felt).await
                    }
                    TokenStandard::Erc1155 => {
                        let token_id_felt = Felt::from(request.token_id.low());
                        fetcher.fetch_uri(request.contract, token_id_felt).await
                    }
                };

                // Try to resolve the URI to JSON metadata
                let metadata_json = if let Some(ref uri_str) = uri {
                    Self::resolve_metadata(uri_str).await
                } else {
                    None
                };

                let result = TokenUriResult {
                    contract: request.contract,
                    token_id: request.token_id,
                    uri,
                    metadata_json,
                };

                if let Err(e) = store.store_token_uri(&result).await {
                    tracing::warn!(
                        target: "torii_common::token_uri",
                        contract = %format!("{:#x}", request.contract),
                        token_id = %request.token_id,
                        error = %e,
                        "Failed to store token URI result"
                    );
                } else {
                    tracing::debug!(
                        target: "torii_common::token_uri",
                        contract = %format!("{:#x}", request.contract),
                        token_id = %request.token_id,
                        uri = ?result.uri,
                        has_json = result.metadata_json.is_some(),
                        "Stored token URI"
                    );
                }

                // Remove ourselves from in-flight
                in_flight.lock().await.remove(&task_key);
            });

            tasks.insert(key, handle);
        }

        tracing::info!(
            target: "torii_common::token_uri",
            "Token URI service shutting down"
        );
    }

    /// Resolve a URI to JSON metadata.
    ///
    /// Handles:
    /// - `https://` / `http://` URLs — fetch directly
    /// - `ipfs://` URIs — convert to HTTP gateway URL
    /// - `data:application/json;base64,` — decode inline
    /// - `data:application/json,` — decode inline (URL-encoded)
    async fn resolve_metadata(uri: &str) -> Option<String> {
        let url = if uri.starts_with("ipfs://") {
            // Use a public IPFS gateway
            format!("https://ipfs.io/ipfs/{}", &uri[7..])
        } else if uri.starts_with("data:application/json;base64,") {
            let encoded = &uri["data:application/json;base64,".len()..];
            return match base64_decode(encoded) {
                Some(json) => Some(json),
                None => {
                    tracing::debug!(
                        target: "torii_common::token_uri",
                        "Failed to decode base64 data URI"
                    );
                    None
                }
            };
        } else if uri.starts_with("data:application/json,") {
            let json = &uri["data:application/json,".len()..];
            return Some(
                urlencoding::decode(json)
                    .unwrap_or_else(|_| json.into())
                    .into_owned(),
            );
        } else if uri.starts_with("http://") || uri.starts_with("https://") {
            uri.to_string()
        } else {
            tracing::debug!(
                target: "torii_common::token_uri",
                uri = uri,
                "Unsupported URI scheme"
            );
            return None;
        };

        match reqwest::get(&url).await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    tracing::debug!(
                        target: "torii_common::token_uri",
                        url = %url,
                        status = %resp.status(),
                        "Failed to fetch metadata"
                    );
                    return None;
                }
                match resp.text().await {
                    Ok(body) => Some(body),
                    Err(e) => {
                        tracing::debug!(
                            target: "torii_common::token_uri",
                            url = %url,
                            error = %e,
                            "Failed to read metadata body"
                        );
                        None
                    }
                }
            }
            Err(e) => {
                tracing::debug!(
                    target: "torii_common::token_uri",
                    url = %url,
                    error = %e,
                    "Failed to fetch metadata URL"
                );
                None
            }
        }
    }

    /// Wait for the service to finish (blocks forever unless channel is closed).
    pub async fn join(self) {
        let _ = self.handle.await;
    }

    /// Abort the background task.
    pub fn abort(self) {
        self.handle.abort();
    }
}

/// Simple base64 decode helper
fn base64_decode(input: &str) -> Option<String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(input)
        .ok()?;
    String::from_utf8(bytes).ok()
}
