pub mod multi;

use async_trait::async_trait;
use axum::Router;
use prost_types::Any;
use std::sync::Arc;

use super::envelope::{Envelope, TypeId};
use crate::grpc::SubscriptionManager;

pub use multi::MultiSink;

// Re-export for external sink authors
pub use tonic;

/// Topic information provided by a sink
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic name (e.g., "sql", "entities")
    pub name: String,
    /// Available filter keys for this topic
    pub available_filters: Vec<String>,
    /// Description of what this topic contains
    pub description: String,
}

impl TopicInfo {
    pub fn new(name: impl Into<String>, available_filters: Vec<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            available_filters,
            description: description.into(),
        }
    }
}

/// Sink trait - processes envelopes and exposes functionality
///
/// Sinks have three ways to expose functionality:
/// 1. **EventBus** - Publish updates to central topic-based subscriptions (via `torii.Torii/Subscribe`)
/// 2. **HTTP** - Expose custom REST endpoints (via `build_routes()`)
/// 3. **gRPC** - Provide custom gRPC services (user adds to router before passing to Torii)
///
/// # gRPC Registration
///
/// Due to Rust's type system limitations, gRPC services must be registered by the user
/// when building their application. See sink implementation docs for examples.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Get the name of this sink
    fn name(&self) -> &str;

    /// Get the type IDs this sink is interested in
    fn interested_types(&self) -> Vec<TypeId>;

    /// Process a batch of envelopes with enriched context
    ///
    /// # Arguments
    /// - `envelopes`: Decoded envelopes to process (by reference for multi-sink support).
    /// - `batch`: Original extraction batch with events, blocks, and transactions.
    ///
    /// # Enriched Context
    ///
    /// The batch provides:
    /// - `batch.events`: Original raw events (Vec).
    /// - `batch.blocks`: Block context deduplicated by block_number (HashMap - O(1) lookup).
    /// - `batch.transactions`: Transaction context deduplicated by tx_hash (HashMap - O(1) lookup).
    ///
    /// This allows sinks to access transaction calldata, block timestamps, etc.
    ///
    /// # Memory Design
    ///
    /// Both envelopes and the original batch are provided because:
    /// 1. **Multiple decoders produce multiple envelopes per event** - One raw event may be decoded
    ///    into several envelopes by different decoders, each extracting different aspects.
    /// 2. **Envelopes are lightweight** - They contain only the decoded data relevant to that decoder.
    /// 3. **Context is deduplicated** - Blocks and transactions are stored once in HashMaps,
    ///    shared across all events in the batch.
    ///
    /// This design accepts the memory cost of keeping both representations to enable flexible
    /// multi-decoder architectures while optimizing context storage.
    ///
    /// # Performance Best Practices
    ///
    /// **DO:**
    /// - Use `envelope.metadata` for event-specific data extracted by the decoder.
    /// - Use `batch.blocks[&block_number]` for fast O(1) block context lookups.
    /// - Use `batch.transactions[&tx_hash]` for fast O(1) transaction context lookups.
    ///
    /// **DON'T:**
    /// - Iterate through `batch.events` to find the original event (slow O(n) operation).
    /// - If you need original event data, have the decoder put it in `envelope.metadata`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
    ///     for envelope in envelopes {
    ///         let insert = envelope.downcast_ref::<SqlInsert>()?;
    ///
    ///         // Fast: Access via metadata (set by decoder)
    ///         let block_number: u64 = envelope.metadata.get("block_number")
    ///             .unwrap().parse()?;
    ///
    ///         // Fast: O(1) HashMap lookup
    ///         let block = &batch.blocks[&block_number];
    ///         println!("Block timestamp: {}", block.timestamp);
    ///
    ///         // If you need more event data, decoder should add it to metadata
    ///         let from_address = envelope.metadata.get("from_address").unwrap();
    ///     }
    ///     Ok(())
    /// }
    /// ```
    async fn process(
        &self,
        envelopes: &[Envelope],
        batch: &crate::etl::extractor::ExtractionBatch,
    ) -> anyhow::Result<()>;

    /// Get topic information provided by this sink
    ///
    /// Returns a list of topics with their available filters and descriptions.
    /// This is used by the ListTopics gRPC endpoint to inform clients about available subscriptions.
    fn topics(&self) -> Vec<TopicInfo>;

    /// Build HTTP routes for this sink
    ///
    /// Sinks can expose custom HTTP endpoints by implementing this method.
    /// Torii will automatically merge these routes into the main HTTP router.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// fn build_routes(&self) -> Router {
    ///     Router::new()
    ///         .route("/my-sink/query", post(query_handler))
    ///         .route("/my-sink/status", get(status_handler))
    ///         .with_state(self.state.clone())
    /// }
    /// ```
    fn build_routes(&self) -> Router;

    /// Initialize the sink with access to the event bus
    ///
    /// This is called once during server startup, before the ETL pipeline starts.
    /// Sinks can use the event bus to publish updates to subscribers.
    async fn initialize(&mut self, event_bus: Arc<EventBus>) -> anyhow::Result<()>;
}

/// EventBus allows sinks to publish updates to gRPC subscribers
/// Sinks can register new topics and broadcast data
pub struct EventBus {
    subscription_manager: Arc<SubscriptionManager>,
}

impl EventBus {
    pub fn new(subscription_manager: Arc<SubscriptionManager>) -> Self {
        Self {
            subscription_manager,
        }
    }

    /// Publish protobuf data to subscribers with sink-provided filtering
    ///
    /// **Optimized version**: Accepts both encoded (Any) and decoded data to avoid
    /// repeated decoding for each client's filter check.
    ///
    /// The `filter_fn` is provided by the sink and determines if the data matches
    /// a client's subscription filters. This keeps filtering logic in the sink,
    /// not in the core.
    ///
    /// # Arguments
    /// * `topic` - Topic name (e.g., "sql", "events")
    /// * `type_id` - Type identifier (e.g., "sql.row_inserted")
    /// * `data` - Encoded protobuf Any data (for sending to clients)
    /// * `decoded` - Decoded data (for filtering, avoids N decodes)
    /// * `update_type` - Type of update (Created, Updated, Deleted)
    /// * `filter_fn` - Sink-provided function to check if data matches filters
    ///
    /// # Performance
    /// Cost: 1 encode + 0 decodes (vs 1 encode + N decodes in naive approach)
    pub fn publish_protobuf<F, T>(
        &self,
        topic: &str,
        type_id: &str,
        data: &Any,
        decoded: &T,
        update_type: crate::grpc::UpdateType,
        filter_fn: F,
    ) where
        F: Fn(&T, &std::collections::HashMap<String, String>) -> bool,
        T: ?Sized,
    {
        use crate::grpc::TopicUpdate;

        let clients = self.subscription_manager.clients().read().unwrap();
        let timestamp = chrono::Utc::now().timestamp();
        let mut sent_count = 0;

        for (client_id, client_sub) in clients.iter() {
            if let Some(filters) = client_sub.topics.get(topic) {
                // Sink decides if data matches client filters
                // Uses decoded data - no decode overhead!
                if filter_fn(decoded, filters) {
                    let update = TopicUpdate {
                        topic: topic.to_string(),
                        update_type: update_type as i32,
                        timestamp,
                        type_id: type_id.to_string(),
                        data: Some(data.clone()),
                    };

                    if let Err(e) = client_sub.tx.try_send(update) {
                        tracing::debug!(
                            target: "torii::etl::event_bus",
                            "Failed to send to client {}: {}",
                            client_id,
                            e
                        );
                    } else {
                        sent_count += 1;
                    }
                }
            }
        }

        tracing::debug!(
            target: "torii::etl::event_bus",
            "Published protobuf to topic '{}' (type: {}, sent to {} clients)",
            topic,
            type_id,
            sent_count
        );
    }

    /// Get the subscription manager for advanced use cases
    pub fn subscription_manager(&self) -> &Arc<SubscriptionManager> {
        &self.subscription_manager
    }
}
