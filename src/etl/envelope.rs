//! This module contains the envelope for the ETL pipeline.

use starknet::core::types::{EmittedEvent, Felt};
use std::any::Any;
use std::collections::HashMap;
use xxhash_rust::const_xxh3::xxh3_64;

/// Type identifier based on a string hash
/// This allows sinks to identify and downcast envelope bodies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EnvelopeTypeId(u64);

impl EnvelopeTypeId {
    /// Creates a TypeId from a string at compile time.
    pub const fn new(type_name: &str) -> Self {
        EnvelopeTypeId(xxh3_64(type_name.as_bytes()))
    }

    /// Returns the TypeId as a u64.
    pub const fn as_u64(&self) -> u64 {
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

        impl From<$t> for Box<dyn TypedBody> {
            fn from(value: $t) -> Self {
                Box::new(value)
            }
        }

        impl From<$t> for Vec<Box<dyn TypedBody>> {
            fn from(value: $t) -> Self {
                vec![value.into()]
            }
        }
    };
}

pub struct TransactionMsgs {
    pub block_number: u64,
    pub transaction_hash: Felt,
    pub from_address: Felt,
    pub msgs: Vec<Box<dyn TypedBody>>,
    pub timestamp: i64,
}

impl TransactionMsgs {
    pub fn new(
        block_number: u64,
        transaction_hash: Felt,
        from_address: Felt,
        msgs: Vec<Box<dyn TypedBody>>,
    ) -> Self {
        Self {
            block_number,
            transaction_hash,
            from_address,
            msgs,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }
    pub fn new_empty(block_number: u64, transaction_hash: Felt, from_address: Felt) -> Self {
        Self {
            block_number,
            transaction_hash,
            from_address,
            msgs: Vec::new(),
            timestamp: chrono::Utc::now().timestamp(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetaData {
    pub block_number: Option<u64>,
    pub transaction_hash: Felt,
    pub from_address: Felt,
}

pub trait EventMsg: Send + Sync + 'static {
    fn event_id(&self) -> String;
    fn envelope_type_id(&self) -> TypeId;
}
