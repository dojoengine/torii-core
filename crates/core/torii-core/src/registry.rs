//! Registry implementation used to coordinate decoders and detect collisions.

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;

use crate::{
    decoder::{Decoder, DecoderFactory},
    types::{DecoderFilter, FetchPlan},
};

/// Registry tracking registered decoders and their factories.
pub struct DecoderRegistry {
    /// Registered decoders.
    decoders: Vec<Arc<dyn Decoder>>,
    /// Fetch filters contributed by each decoder.
    filters: Vec<DecoderFilter>,
    /// Decoder factories registered with the registry.
    factories: HashMap<&'static str, Arc<dyn DecoderFactory>>,
    /// Type identifiers indexed by decoder name. This is used mainly to detect collisions.
    type_index: HashMap<u64, String>,
}

impl DecoderRegistry {
    /// Create an empty decoder registry.
    pub fn new() -> Self {
        Self {
            decoders: Vec::new(),
            filters: Vec::new(),
            factories: HashMap::new(),
            type_index: HashMap::new(),
        }
    }

    /// Initialise the registry with a predefined list of decoders.
    pub fn with_decoders(decoders: Vec<Arc<dyn Decoder>>) -> Result<Self> {
        let mut registry = Self::new();
        for decoder in decoders {
            registry.register(decoder)?;
        }
        Ok(registry)
    }

    /// Register a decoder, ensuring its type identifiers do not collide with existing entries.
    pub fn register(&mut self, decoder: Arc<dyn Decoder>) -> Result<()> {
        for &type_id in decoder.type_ids() {
            if let Some(existing) = self.type_index.get(&type_id) {
                anyhow::bail!(
                    "decoder '{}' declares colliding type_id {type_id:#x} already owned by '{}'",
                    decoder.name(),
                    existing
                );
            }
        }

        let filter = decoder.filter().clone();
        tracing::info!(
            target: "torii_decoders",
            decoder = decoder.name(),
            addresses = filter.contract_addresses.len(),
            selectors = filter.selectors.len(),
            "registered decoder"
        );

        for &type_id in decoder.type_ids() {
            self.type_index.insert(type_id, decoder.name().to_string());
        }

        self.filters.push(filter);
        self.decoders.push(decoder);
        Ok(())
    }

    /// Borrow the registered decoders.
    pub fn decoders(&self) -> &[Arc<dyn Decoder>] {
        &self.decoders
    }

    /// Compute the aggregate fetch plan derived from all registered decoders.
    pub fn fetch_plan(&self) -> FetchPlan {
        FetchPlan::union(self.filters.clone())
    }

    /// Register a decoder factory so the registry can build instances from configuration.
    pub fn register_factory(&mut self, factory: Arc<dyn DecoderFactory>) {
        let kind = factory.kind();
        tracing::debug!(
            target: "torii_decoders",
            factory = kind,
            "registered decoder factory"
        );
        self.factories.insert(kind, factory);
    }
}

impl Default for DecoderRegistry {
    fn default() -> Self {
        Self::new()
    }
}
