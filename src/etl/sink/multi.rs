//! MultiSink runs multiple sinks in sequence
//!
//! Each sink processes envelopes independently.
//! Sinks can filter by TypeId to only process events they're interested in.

use async_trait::async_trait;
use axum::Router;
use std::sync::Arc;

use super::{EventBus, Sink, SinkContext};
use crate::etl::envelope::Envelope;
use crate::etl::extractor::ExtractionBatch;

/// MultiSink runs multiple sinks and merges their routes
pub struct MultiSink {
    sinks: Vec<Arc<dyn Sink>>,
}

impl MultiSink {
    /// Create a new MultiSink with a list of sinks
    pub fn new(sinks: Vec<Arc<dyn Sink>>) -> Self {
        Self { sinks }
    }

    /// Get all sinks (useful for accessing specific sinks after creation)
    pub fn sinks(&self) -> &[Arc<dyn Sink>] {
        &self.sinks
    }
}

#[async_trait]
impl Sink for MultiSink {
    fn name(&self) -> &'static str {
        "multi"
    }

    fn interested_types(&self) -> Vec<crate::etl::envelope::TypeId> {
        // MultiSink accepts all types (delegates to individual sinks)
        vec![]
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> anyhow::Result<()> {
        // TODO: to optimize the performance, we can easily parallelize the processing of the envelopes by the sinks
        // since they are totally independent of each other.
        // Run each sink in sequence
        for sink in &self.sinks {
            // Each sink processes the same envelopes
            // Sinks filter by TypeId internally
            if let Err(e) = sink.process(envelopes, batch).await {
                tracing::error!(
                    target: "torii::etl::multi_sink",
                    "Sink '{}' failed: {}",
                    sink.name(),
                    e
                );
                // TODO: Currently, if a sink fails at processing an event, it will not be retried.
                // We should see a better mechanism here, is it better to retry and stop the whole process if it fails again?
            }
        }

        tracing::debug!(
            target: "torii::etl::multi_sink",
            "Processed {} envelopes across {} sinks",
            envelopes.len(),
            self.sinks.len()
        );

        Ok(())
    }

    fn topics(&self) -> Vec<super::TopicInfo> {
        // Aggregate topics from all sinks
        let mut all_topics = Vec::new();
        for sink in &self.sinks {
            all_topics.extend(sink.topics());
        }
        all_topics
    }

    fn build_routes(&self) -> Router {
        // Merge all sink routes into a single router
        let mut router = Router::new();
        for sink in &self.sinks {
            router = router.merge(sink.build_routes());
        }
        router
    }

    async fn initialize(
        &mut self,
        _event_bus: Arc<EventBus>,
        _context: &SinkContext,
    ) -> anyhow::Result<()> {
        // Initialize all sinks with the event bus
        for _sink in &mut self.sinks {
            // We need to get mutable access, but sinks are Arc'd
            // This is a limitation - sinks should be initialized before wrapping in Arc
            tracing::warn!(
                target: "torii::etl::multi_sink",
                "MultiSink cannot initialize Arc-wrapped sinks. Initialize sinks before wrapping in MultiSink."
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etl::envelope::{Envelope, TypeId};
    use crate::etl::extractor::ExtractionBatch;
    use crate::grpc::SubscriptionManager;
    use std::collections::HashMap;

    // Mock sink for testing
    struct MockSink {
        name: String,
    }

    #[async_trait]
    impl Sink for MockSink {
        fn name(&self) -> &str {
            &self.name
        }

        fn interested_types(&self) -> Vec<TypeId> {
            vec![TypeId::new("test.event")]
        }

        async fn process(
            &self,
            _envelopes: &[Envelope],
            _batch: &ExtractionBatch,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn topics(&self) -> Vec<super::super::TopicInfo> {
            vec![super::super::TopicInfo::new("test", vec![], "Test topic")]
        }

        fn build_routes(&self) -> Router {
            Router::new()
        }

        async fn initialize(
            &mut self,
            _event_bus: Arc<EventBus>,
            _context: &SinkContext,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_multi_sink() {
        let sub_manager = Arc::new(SubscriptionManager::new());
        let _event_bus = Arc::new(EventBus::new(sub_manager));

        // Create multiple sinks
        let sinks: Vec<Arc<dyn Sink>> = vec![
            Arc::new(MockSink {
                name: "sink1".to_string(),
            }),
            Arc::new(MockSink {
                name: "sink2".to_string(),
            }),
        ];

        let multi_sink = MultiSink::new(sinks);

        // Process empty batch
        let batch = ExtractionBatch {
            events: vec![],
            blocks: HashMap::new(),
            transactions: HashMap::new(),
            declared_classes: Vec::new(),
            deployed_contracts: Vec::new(),
            cursor: None,
            chain_head: None,
        };

        multi_sink.process(&[], &batch).await.unwrap();

        // Should have 2 sinks
        assert_eq!(multi_sink.sinks().len(), 2);
    }
}
