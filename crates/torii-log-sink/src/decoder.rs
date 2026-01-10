use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::EmittedEvent;
use std::any::Any;
use torii::etl::{
    envelope::{Envelope, TypeId, TypedBody},
    Decoder,
};

/// Decoded log entry
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub message: String,
    pub block_number: u64,
    pub event_key: String,
}

impl TypedBody for LogEntry {
    fn envelope_type_id(&self) -> TypeId {
        TypeId::new("log.entry")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Decoder that converts events into log entries.
pub struct LogDecoder {
    /// Optional event key filter (e.g., only decode events with specific keys).
    key_filter: Option<String>,
}

impl LogDecoder {
    /// Creates a new LogDecoder.
    ///
    /// If `key_filter` is Some, only events with keys containing this string will be decoded.
    pub fn new(key_filter: Option<String>) -> Self {
        Self { key_filter }
    }
}

#[async_trait]
impl Decoder for LogDecoder {
    fn decoder_name(&self) -> &str {
        "log"
    }

    async fn decode(&self, events: &[EmittedEvent]) -> Result<Vec<Envelope>> {
        let mut envelopes = Vec::new();

        for (idx, event) in events.iter().enumerate() {
            // Apply key filter if specified
            if let Some(ref filter) = self.key_filter {
                let matches = event.keys.iter().any(|k| {
                    let key_hex = format!("{:#x}", k);
                    key_hex.contains(filter)
                });
                if !matches {
                    continue;
                }
            }

            // Extract log message from event data.
            // For this example, we'll convert the first data field to a string representation.
            let message = if !event.data.is_empty() {
                format!("Event data: {:#x}", event.data[0])
            } else {
                "Empty event data".to_string()
            };

            let event_key = if !event.keys.is_empty() {
                format!("{:#x}", event.keys[0])
            } else {
                "no-key".to_string()
            };

            let log_entry = LogEntry {
                message,
                block_number: event.block_number.unwrap_or(0),
                event_key,
            };

            // Create envelope with unique ID
            let envelope_id = format!("log_{}_{}", event.block_number.unwrap_or(0), idx);
            let envelope = Envelope::new(
                envelope_id,
                Box::new(log_entry),
                std::collections::HashMap::new(),
            );

            envelopes.push(envelope);
        }

        Ok(envelopes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use starknet::core::types::Felt;

    #[tokio::test]
    async fn test_decode_logs() {
        let decoder = LogDecoder::new(None);

        let event = EmittedEvent {
            from_address: Felt::from(1u64),
            keys: vec![Felt::from(0x1234u64)],
            data: vec![Felt::from(0x5678u64)],
            block_hash: None,
            block_number: Some(100),
            transaction_hash: Felt::from(0xabcdu64),
        };

        let events = vec![event];
        let envelopes = decoder.decode(&events).await.unwrap();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].type_id, TypeId::new("log.entry"));
    }
}
