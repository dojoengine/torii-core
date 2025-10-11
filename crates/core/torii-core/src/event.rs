//! Traits and macros for working with typed domain events that Torii understands.

use starknet::core::types::EmittedEvent;

use crate::type_id_from_url;

/// Canonical domain event trait implemented by typed payloads.
pub trait Event: Send + Sync + 'static {
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Trait providing static metadata for events.
pub trait StaticEvent: Event {
    const SELECTOR: &str;
    const TYPE_ID: u64 = type_id_from_url(Self::SELECTOR);
    fn to_envelope(self, raw: &EmittedEvent) -> crate::Envelope;
}

/// Helper macro to implement [`Event`] and [`StaticEvent`] for a struct with the provided metadata.
#[macro_export]
macro_rules! impl_event {
    ($t:ty, $url:expr) => {
        impl $crate::StaticEvent for $t {
            const SELECTOR: &str = $url;
            fn to_envelope(self, raw: &starknet::core::types::EmittedEvent) -> $crate::Envelope {
                $crate::Envelope {
                    type_id: Self::TYPE_ID,
                    raw: std::sync::Arc::new(raw.clone()),
                    body: $crate::Body::Typed(
                        std::sync::Arc::new(self) as std::sync::Arc<dyn $crate::Event>
                    ),
                }
            }
        }

        impl $crate::Event for $t {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }
    };
}

/// Convenience macro for computing a canonical type identifier from a literal at compile time.
#[macro_export]
macro_rules! canonical_type_id {
    ($url:literal) => {
        $crate::type_id_from_url($url)
    };
}
