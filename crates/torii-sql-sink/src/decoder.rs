//! This module contains the decoder for the SQL sink.
//!
//! It demonstrate how to decode Starknet events into envelopes based on the event content.
use async_trait::async_trait;
use starknet::core::types::{EmittedEvent, Felt};
use starknet::core::utils::parse_cairo_short_string;
use starknet::macros::selector;
use std::any::Any;
use std::collections::HashMap;

use torii::etl::decoder::Decoder;
use torii::etl::envelope::{Envelope, TypeId, TypedBody};

/// SqlInsert event type - represents a SQL insert operation.
/// By deriving TypedBody, it allows the envelope to be downcast to this type.
#[derive(Debug, Clone)]
pub struct SqlInsert {
    pub table: String,
    pub value: u64,
}

impl TypedBody for SqlInsert {
    fn envelope_type_id(&self) -> TypeId {
        TypeId::new("sql.insert")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// SqlUpdate event type - represents a SQL update operation.
/// By deriving TypedBody, it allows the envelope to be downcast to this type.
#[derive(Debug, Clone)]
pub struct SqlUpdate {
    pub table: String,
    pub value: u64,
}

impl TypedBody for SqlUpdate {
    fn envelope_type_id(&self) -> TypeId {
        TypeId::new("sql.update")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Decoder that converts Starknet events to typed SQL operation envelopes.
///
/// This decoder matches on event selectors to determine the operation type:
/// - selector!("insert") -> SqlInsert envelope
/// - selector!("update") -> SqlUpdate envelope
///
/// For processing all events, use an empty contract_filters Vec.
/// This shows an example on how decoders could have custom filters
/// based on the required business logic.
pub struct SqlDecoder {
    /// Filter by specific contract addresses.
    /// If empty, processes all events.
    contract_filters: Vec<Felt>,
}

impl SqlDecoder {
    /// Creates a new SqlDecoder.
    ///
    /// # Arguments
    /// * `contract_filters` - List of contract addresses to filter by.
    ///   If empty, processes all events.
    pub fn new(contract_filters: Vec<Felt>) -> Self {
        Self { contract_filters }
    }

    /// Checks if this decoder is interested in the given event, this could be used to skip events that are not relevant to this decoder.
    fn is_interested(&self, event: &EmittedEvent) -> bool {
        if self.contract_filters.is_empty() {
            return true;
        }

        self.contract_filters.contains(&event.from_address)
    }
}

/// Implementation of the Decoder trait for generic usage (not specific to the SQL sink).
#[async_trait]
impl Decoder for SqlDecoder {
    fn decoder_name(&self) -> &'static str {
        "sql"
    }

    async fn decode_event(&self, event: &EmittedEvent) -> anyhow::Result<Vec<Envelope>> {
        if !self.is_interested(event) {
            return Ok(Vec::new());
        }

        let insert_selector = selector!("insert");
        let update_selector = selector!("update");

        // We could add additional checks for example length of keys etc..
        // In this case, we're going to assume they are present already.
        let selector = match event.keys.first() {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let table_name = match event
            .keys
            .get(1)
            .and_then(|k| parse_cairo_short_string(k).ok())
        {
            Some(name) => name,
            None => return Ok(Vec::new()),
        };

        let value: u64 = match event.data.first().and_then(|v| (*v).try_into().ok()) {
            Some(v) => v,
            None => return Ok(Vec::new()),
        };

        let (body, operation): (Box<dyn TypedBody>, &str) = if *selector == insert_selector {
            (
                Box::new(SqlInsert {
                    table: table_name.clone(),
                    value,
                }),
                "insert",
            )
        } else if *selector == update_selector {
            (
                Box::new(SqlUpdate {
                    table: table_name.clone(),
                    value,
                }),
                "update",
            )
        } else {
            tracing::debug!(
                target: "torii::sinks::sql::decoder",
                "Unknown selector: {:#x}, skipping event",
                selector
            );
            return Ok(Vec::new());
        };

        // Create metadata, they are optional, but currently they can give more context to the envelope
        // without adding this information to the envelope body.
        let mut metadata = HashMap::new();
        metadata.insert("source".to_string(), "starknet".to_string());
        metadata.insert("operation".to_string(), operation.to_string());
        metadata.insert("table".to_string(), table_name.clone());
        metadata.insert("value".to_string(), value.to_string());
        metadata.insert(
            "from_address".to_string(),
            format!("{:#x}", event.from_address),
        );

        Ok(vec![Envelope::new(
            format!("{operation}_{table_name}"),
            body,
            metadata,
        )])
    }
}
