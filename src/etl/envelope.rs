//! This module contains the envelope for the ETL pipeline.

use starknet::core::types::{EmittedEvent, Felt};
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Type identifier based on a string hash
/// This allows sinks to identify and downcast envelope bodies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EnvelopeTypeId(u64);

impl EnvelopeTypeId {
    /// Creates a TypeId from a string (e.g., "sql.row_inserted")
    pub fn new(type_name: &str) -> Self {
        let mut hasher = DefaultHasher::new();
        type_name.hash(&mut hasher);
        EnvelopeTypeId(hasher.finish())
    }

    /// Returns the TypeId as a u64.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

// Re-export as TypeId for convenience
pub type TypeId = EnvelopeTypeId;

/// Trait for typed envelope bodies
/// Sinks can downcast to concrete types using type_id
pub trait TypedBody: Send + Sync {
    fn envelope_type_id(&self) -> TypeId;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Helper macro to implement TypedBody
#[macro_export]
macro_rules! typed_body_impl {
    ($t:ty, $url:expr) => {
        impl $crate::etl::envelope::TypedBody for $t {
            fn envelope_type_id(&self) -> $crate::etl::envelope::TypeId {
                $crate::etl::envelope::TypeId::new($url)
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }
        }
    };
}

/// Envelope wraps transformed data with metadata
/// This is the core data structure that flows through the ETL pipeline
pub struct Envelope {
    /// Unique identifier for this envelope
    pub id: String,

    /// Type identifier for the body
    pub type_id: TypeId,

    /// The actual data (can be downcast by sinks)
    pub body: Box<dyn TypedBody>,

    /// Metadata that sinks can use for filtering
    pub metadata: HashMap<String, String>,

    /// Timestamp when this envelope was created
    pub timestamp: i64,
}

impl Envelope {
    /// Creates a new envelope.
    pub fn new(id: String, body: Box<dyn TypedBody>, metadata: HashMap<String, String>) -> Self {
        let type_id = body.envelope_type_id();
        Self {
            id,
            type_id,
            body,
            metadata,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }

    /// Tries to downcast the body to a concrete type.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.body.as_any().downcast_ref::<T>()
    }

    /// Tries to downcast the body to a mutable concrete type.
    pub fn downcast_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.body.as_any_mut().downcast_mut::<T>()
    }
}

/// Debug implementation for Envelope.
impl std::fmt::Debug for Envelope {
    /// Formats the envelope for debugging.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Envelope")
            .field("id", &self.id)
            .field("type_id", &self.type_id)
            .field("metadata", &self.metadata)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

pub trait EventMsg {
    fn event_id(&self) -> String;
    fn envelope_type_id(&self) -> TypeId;
}

#[derive(Debug, Clone)]
pub struct EventBody<T: EventMsg + 'static + Send + Sync> {
    pub from_address: Felt,
    pub transaction_hash: Felt,
    pub msg: T,
}

impl<T> TypedBody for EventBody<T>
where
    T: EventMsg + Send + Sync + 'static,
{
    fn envelope_type_id(&self) -> TypeId {
        self.msg.envelope_type_id()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<T: EventMsg + 'static + Send + Sync> EventBody<T> {
    pub fn new(msg: T, raw: &EmittedEvent) -> Self {
        Self {
            from_address: raw.from_address.clone(),
            transaction_hash: raw.transaction_hash.clone(),
            msg,
        }
    }

    pub fn new_envelope(msg: T, raw: &EmittedEvent) -> Envelope {
        let id = msg.event_id();
        let event_body = Self::new(msg, raw);
        Envelope::new(id, Box::new(event_body), HashMap::new())
    }
}
