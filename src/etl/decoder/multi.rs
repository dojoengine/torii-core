//! MultiDecoder runs multiple decoders in sequence.
//!
//! Each decoder can examine events and decide if it's interested.
//! Multiple decoders can process the same event, creating different envelopes.
//!
//! Two decoding modes are supported:
//! - **Chain ordering (default)**: Maintains strict event ordering.
//! - **Decoder ordering**: Groups envelopes by decoder, easier to parallelize.

use async_trait::async_trait;
use starknet::core::types::EmittedEvent;
use std::sync::Arc;

use super::Decoder;
use crate::etl::envelope::Envelope;

/// Decoding mode for MultiDecoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeMode {
    /// Chain ordering (default): Iterate on events, pass each to all decoders.
    ///
    /// Maintains strict event ordering in output:
    /// ```text
    /// Event 1 → Decoder A → Envelope A1
    /// Event 1 → Decoder B → Envelope B1
    /// Event 2 → Decoder A → Envelope A2
    /// Event 2 → Decoder B → Envelope B2
    /// Result: [A1, B1, A2, B2]
    /// ```
    ///
    /// Best for: When event order matters more than decoder grouping.
    ChainOrdering,

    /// Decoder ordering: Iterate on decoders, pass all events to each.
    ///
    /// Groups envelopes by decoder:
    /// ```text
    /// Decoder A → Events [1, 2] → Envelopes [A1, A2]
    /// Decoder B → Events [1, 2] → Envelopes [B1, B2]
    /// Result: [A1, A2, B1, B2]
    /// ```
    ///
    /// Best for: When decoder grouping or parallelization is preferred.
    DecoderOrdering,
}

/// MultiDecoder runs multiple decoders and combines their results.
pub struct MultiDecoder {
    decoders: Vec<Arc<dyn Decoder>>,
    mode: DecodeMode,
}

impl MultiDecoder {
    /// Create a new MultiDecoder with default chain ordering mode
    pub fn new(decoders: Vec<Arc<dyn Decoder>>) -> Self {
        Self {
            decoders,
            mode: DecodeMode::ChainOrdering,
        }
    }

    /// Create a new MultiDecoder with explicit decode mode
    pub fn with_mode(decoders: Vec<Arc<dyn Decoder>>, mode: DecodeMode) -> Self {
        Self { decoders, mode }
    }

    /// Set the decode mode
    pub fn set_mode(&mut self, mode: DecodeMode) {
        self.mode = mode;
    }
}

#[async_trait]
impl Decoder for MultiDecoder {
    async fn decode(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        let all_envelopes = match self.mode {
            DecodeMode::ChainOrdering => self.decode_chain_ordering(events).await?,
            DecodeMode::DecoderOrdering => self.decode_decoder_ordering(events).await?,
        };

        tracing::debug!(
            target: "torii::etl::multi_decoder",
            "Decoded {} events into {} envelopes across {} decoders (mode: {:?})",
            events.len(),
            all_envelopes.len(),
            self.decoders.len(),
            self.mode
        );

        Ok(all_envelopes)
    }
}

impl MultiDecoder {
    /// Chain ordering: iterate on events, pass each to all decoders.
    /// Maintains strict event ordering in the output, even if multiple decoders are interested in the same event.
    async fn decode_chain_ordering(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();

        for event in events {
            for decoder in &self.decoders {
                // Pass single-element slice to decoder, no copy.
                let envelopes = decoder.decode(std::slice::from_ref(event)).await?;
                all_envelopes.extend(envelopes);
            }
        }

        Ok(all_envelopes)
    }

    /// Decoder ordering: iterate on decoders, pass all events to each.
    /// Groups envelopes by decoder, easier to parallelize.
    async fn decode_decoder_ordering(&self, events: &[EmittedEvent]) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();

        // TODO: add multi-threading here, to parallelize the decoding of the events by the decoders if it's a bottleneck.
        for decoder in &self.decoders {
            let envelopes = decoder.decode(events).await?;
            all_envelopes.extend(envelopes);
        }

        Ok(all_envelopes)
    }
}
