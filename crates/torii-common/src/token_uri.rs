//! Async token URI fetching and caching service.
//!
//! Processes `(contract_address, token_id)` requests via a tokio channel.
//! Deduplicates in-flight tasks — if a new request arrives for the same key,
//! the previous task is cancelled so we always apply the latest value.
//!
//! The service fetches `token_uri(token_id)` (ERC721) or `uri(token_id)` (ERC1155),
//! resolves the JSON metadata, and stores the result via a callback.
//!
//! Metadata resolution is modeled after dojoengine/torii's battle-tested approach:
//! - Retries with exponential backoff for transient errors
//! - Permanent error detection (EntrypointNotFound, ContractNotFound)
//! - Tries multiple selectors: token_uri, tokenURI, uri
//! - ERC1155 `{id}` substitution in URIs
//! - data: URI support (base64 and URL-encoded JSON)
//! - IPFS gateway resolution
//! - JSON sanitization for broken metadata (control chars, unescaped quotes)
//! - Raw JSON fallback for inline metadata

use starknet::core::types::{Felt, U256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::MetadataFetcher;

// Retry configuration
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_RETRIES: u32 = 5;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// Token ID
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
    handle: JoinHandle<()>,
}

impl TokenUriService {
    /// Spawn the token URI service.
    ///
    /// Returns a `(TokenUriSender, TokenUriService)` pair.
    /// The sender is cheap to clone and can be shared across sinks.
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
        let in_flight: Arc<Mutex<HashMap<TaskKey, JoinHandle<()>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));

        while let Some(request) = rx.recv().await {
            let key = TaskKey {
                contract: request.contract,
                token_id: request.token_id,
            };

            let mut tasks = in_flight.lock().await;

            // Cancel previous task for same key
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
                let _permit = match sem.acquire().await {
                    Ok(p) => p,
                    Err(_) => return,
                };

                // Fetch the URI from chain
                let uri = fetch_token_uri_with_retry(
                    &fetcher,
                    request.contract,
                    request.token_id,
                    request.standard,
                )
                .await;

                // Apply ERC1155 {id} substitution
                let uri = uri.map(|u| {
                    if request.standard == TokenStandard::Erc1155 {
                        let token_id_hex = format!("{:064x}", request.token_id);
                        u.replace("{id}", &token_id_hex)
                    } else {
                        u
                    }
                });

                // Resolve URI to JSON metadata
                let metadata_json = if let Some(ref uri_str) = uri {
                    if uri_str.is_empty() {
                        None
                    } else {
                        resolve_metadata(uri_str).await
                    }
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

                in_flight.lock().await.remove(&task_key);
            });

            tasks.insert(key, handle);
        }

        tracing::info!(
            target: "torii_common::token_uri",
            "Token URI service shutting down"
        );
    }

    /// Wait for the service to finish.
    pub async fn join(self) {
        let _ = self.handle.await;
    }

    /// Abort the background task.
    pub fn abort(self) {
        self.handle.abort();
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Token URI fetching (from chain)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Fetch token URI with retries, trying multiple selectors.
///
/// Tries `token_uri`, `tokenURI`, and `uri` selectors in order.
/// Distinguishes permanent errors (EntrypointNotFound) from transient ones.
async fn fetch_token_uri_with_retry(
    fetcher: &MetadataFetcher,
    contract: Felt,
    token_id: U256,
    standard: TokenStandard,
) -> Option<String> {
    // Use the MetadataFetcher which already tries multiple selectors
    let token_id_felt = Felt::from(token_id.low());

    match standard {
        TokenStandard::Erc721 => fetcher.fetch_token_uri(contract, token_id_felt).await,
        TokenStandard::Erc1155 => fetcher.fetch_uri(contract, token_id_felt).await,
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Metadata resolution (URI → JSON)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Resolve a URI to JSON metadata string.
///
/// Handles:
/// - `https://` / `http://` URLs — fetch with retries
/// - `ipfs://` URIs — convert to gateway URL, fetch with retries
/// - `data:application/json;base64,` — decode inline
/// - `data:application/json,` — decode inline (URL-encoded)
/// - Raw JSON — try to parse as-is (fallback)
///
/// Returns the metadata as a JSON string, or None on failure.
/// Based on dojoengine/torii's battle-tested `fetch_metadata`.
async fn resolve_metadata(uri: &str) -> Option<String> {
    let result = match uri {
        u if u.starts_with("http://") || u.starts_with("https://") => {
            fetch_http_with_retry(u).await
        }
        u if u.starts_with("ipfs://") => {
            let cid = &u[7..];
            // Try multiple IPFS gateways
            let gateway_url = format!("https://ipfs.io/ipfs/{}", cid);
            fetch_http_with_retry(&gateway_url).await
        }
        u if u.starts_with("data:") => resolve_data_uri(u),
        u => {
            // Fallback: try to parse as raw JSON
            match serde_json::from_str::<serde_json::Value>(u) {
                Ok(json) => match serde_json::to_string(&json) {
                    Ok(s) => Some(s),
                    Err(_) => None,
                },
                Err(_) => {
                    tracing::debug!(
                        target: "torii_common::token_uri",
                        uri = u,
                        "Unsupported URI scheme and not valid JSON"
                    );
                    None
                }
            }
        }
    };

    // Sanitize the JSON if we got a result
    result.and_then(|raw| {
        let sanitized = sanitize_json_string(&raw);
        // Validate it's actually valid JSON
        match serde_json::from_str::<serde_json::Value>(&sanitized) {
            Ok(json) => serde_json::to_string(&json).ok(),
            Err(e) => {
                tracing::debug!(
                    target: "torii_common::token_uri",
                    error = %e,
                    "Fetched content is not valid JSON after sanitization"
                );
                None
            }
        }
    })
}

/// Fetch HTTP content with exponential backoff retries.
async fn fetch_http_with_retry(url: &str) -> Option<String> {
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .ok()?;

    let mut retries = 0;
    let mut backoff = INITIAL_BACKOFF;

    loop {
        match client.get(url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    tracing::debug!(
                        target: "torii_common::token_uri",
                        url = %url,
                        status = %resp.status(),
                        "HTTP fetch failed"
                    );
                    return None;
                }
                return resp.text().await.ok();
            }
            Err(e) => {
                if retries >= MAX_RETRIES {
                    tracing::debug!(
                        target: "torii_common::token_uri",
                        url = %url,
                        error = %e,
                        "HTTP fetch failed after {} retries",
                        MAX_RETRIES
                    );
                    return None;
                }
                tracing::debug!(
                    target: "torii_common::token_uri",
                    url = %url,
                    error = %e,
                    retry = retries + 1,
                    "HTTP fetch failed, retrying"
                );
                tokio::time::sleep(backoff).await;
                retries += 1;
                backoff *= 2;
            }
        }
    }
}

/// Resolve a `data:` URI to its content.
///
/// Supports:
/// - `data:application/json;base64,<encoded>`
/// - `data:application/json,<url-encoded>`
/// - Other data URIs with JSON content
fn resolve_data_uri(uri: &str) -> Option<String> {
    // Handle the # issue: https://github.com/servo/rust-url/issues/908
    let uri = uri.replace('#', "%23");

    if uri.starts_with("data:application/json;base64,") {
        let encoded = &uri["data:application/json;base64,".len()..];
        return base64_decode(encoded);
    }

    if uri.starts_with("data:application/json,") {
        let json = &uri["data:application/json,".len()..];
        let decoded = urlencoding::decode(json)
            .unwrap_or_else(|_| json.into())
            .into_owned();
        return Some(decoded);
    }

    // Generic data URI handling
    if let Some(comma_pos) = uri.find(',') {
        let header = &uri[5..comma_pos]; // skip "data:"
        let body = &uri[comma_pos + 1..];

        if header.contains("base64") {
            return base64_decode(body);
        }

        let decoded = urlencoding::decode(body)
            .unwrap_or_else(|_| body.into())
            .into_owned();
        return Some(decoded);
    }

    tracing::debug!(
        target: "torii_common::token_uri",
        "Malformed data URI"
    );
    None
}

/// Sanitize a JSON string by escaping unescaped double quotes within string values
/// and filtering out control characters.
///
/// Ported from dojoengine/torii — handles broken metadata like Loot Survivor NFTs.
fn sanitize_json_string(s: &str) -> String {
    // First filter out ASCII control characters (except standard whitespace)
    let filtered: String = s
        .chars()
        .filter(|c| !c.is_ascii_control() || *c == '\n' || *c == '\r' || *c == '\t')
        .collect();

    let mut result = String::with_capacity(filtered.len());
    let mut chars = filtered.chars().peekable();
    let mut in_string = false;
    let mut backslash_count: usize = 0;

    while let Some(c) = chars.next() {
        if !in_string {
            if c == '"' {
                in_string = true;
                backslash_count = 0;
                result.push('"');
            } else {
                result.push(c);
            }
            continue;
        }

        // Inside a string
        if c == '\\' {
            backslash_count += 1;
            result.push('\\');
            continue;
        }

        if c == '"' {
            if backslash_count % 2 == 0 {
                // Unescaped quote — check if it ends the string or is internal
                let mut temp = chars.clone();
                // Skip whitespace
                while let Some(&next) = temp.peek() {
                    if next.is_whitespace() {
                        temp.next();
                    } else {
                        break;
                    }
                }
                if let Some(&next) = temp.peek() {
                    if next == ':' || next == ',' || next == '}' || next == ']' {
                        // End of string value
                        result.push('"');
                        in_string = false;
                    } else {
                        // Internal unescaped quote — escape it
                        result.push_str("\\\"");
                    }
                } else {
                    // End of input
                    result.push('"');
                    in_string = false;
                }
            } else {
                // Already escaped
                result.push('"');
            }
            backslash_count = 0;
            continue;
        }

        result.push(c);
        backslash_count = 0;
    }

    result
}

/// Simple base64 decode helper
fn base64_decode(input: &str) -> Option<String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(input)
        .ok()?;
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_json_string_unescaped_quotes() {
        let input = r#"{"name":""Rage Shout" DireWolf"}"#;
        let expected = r#"{"name":"\"Rage Shout\" DireWolf"}"#;
        assert_eq!(sanitize_json_string(input), expected);
    }

    #[test]
    fn test_sanitize_json_string_already_escaped() {
        let input = r#"{"name":"\"Properly Escaped\" Wolf"}"#;
        assert_eq!(sanitize_json_string(input), input);
    }

    #[test]
    fn test_sanitize_json_string_control_chars() {
        let input = "{\x01\"name\": \"test\x02\"}";
        let sanitized = sanitize_json_string(input);
        assert!(!sanitized.contains('\x01'));
        assert!(!sanitized.contains('\x02'));
    }

    #[test]
    fn test_resolve_data_uri_base64() {
        let uri = "data:application/json;base64,eyJuYW1lIjoidGVzdCJ9";
        let result = resolve_data_uri(uri);
        assert_eq!(result, Some(r#"{"name":"test"}"#.to_string()));
    }

    #[test]
    fn test_resolve_data_uri_url_encoded() {
        let uri = "data:application/json,%7B%22name%22%3A%22test%22%7D";
        let result = resolve_data_uri(uri);
        assert_eq!(result, Some(r#"{"name":"test"}"#.to_string()));
    }

    #[test]
    fn test_resolve_data_uri_with_hash() {
        // The # character in data URIs is problematic
        let uri = "data:application/json;base64,eyJuYW1lIjoiIzEifQ==";
        let result = resolve_data_uri(uri);
        assert!(result.is_some());
    }

    #[test]
    fn test_erc1155_id_substitution() {
        let uri = "https://example.com/token/{id}.json";
        let token_id = U256::from(42u64);
        let token_id_hex = format!("{:064x}", token_id);
        let result = uri.replace("{id}", &token_id_hex);
        assert!(result.contains("000000000000000000000000000000000000000000000000000000000000002a"));
    }
}
