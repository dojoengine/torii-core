//! Sink traits and registry utilities.

use std::{any::type_name_of_val, collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::types::Batch;

/// Component that consumes decoded envelopes.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Kind string referenced in configuration (`type = "…"`).
    fn label(&self) -> &str;

    /// Handle a batch of envelopes.
    async fn handle_batch(&self, batch: Batch) -> Result<()>;
}

/// Factory used to construct sinks from configuration.
#[async_trait]
pub trait SinkFactory: Send + Sync {
    /// Kind string referenced in configuration (`type = "…"`).
    fn kind(&self) -> &'static str;

    /// Build a sink instance using the provided configuration block.
    async fn create(&self, name: &str, config: Value) -> Result<Arc<dyn Sink>>;
}

/// Registry tracking available sink factories and instantiated sinks.
pub struct SinkRegistry {
    factories: HashMap<&'static str, Arc<dyn SinkFactory>>,
    sinks: Vec<Arc<dyn Sink>>,
}

impl SinkRegistry {
    /// Create an empty sink registry.
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
            sinks: Vec::new(),
        }
    }

    /// Register a sink factory that can build sinks of the given kind.
    pub fn register_factory(&mut self, factory: Arc<dyn SinkFactory>) {
        let kind = factory.kind();
        tracing::debug!(
            target: "torii_sinks",
            factory = kind,
            "registered sink factory"
        );
        self.factories.insert(kind, factory);
    }

    /// Add a pre-constructed sink instance to the registry.
    pub fn register_sink(&mut self, sink: Arc<dyn Sink>) {
        let sink_type = type_name_of_val(&*sink);
        tracing::info!(
            target: "torii_sinks",
            sink_type,
            "registered sink instance"
        );
        self.sinks.push(sink);
    }

    /// Borrow the registered sinks as a slice.
    pub fn sinks(&self) -> &[Arc<dyn Sink>] {
        &self.sinks
    }
}

impl Default for SinkRegistry {
    fn default() -> Self {
        Self::new()
    }
}
