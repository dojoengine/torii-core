//! Traits and macros for working with typed domain events that Torii understands.

/// Canonical domain event trait implemented by typed payloads.
pub trait Event: Send + Sync + 'static {
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Trait providing static metadata for events.
pub trait StaticEvent: Event {
    fn static_type_id() -> u64;
}

/// Helper macro to implement [`Event`] and [`StaticEvent`] for a struct with the provided metadata.
#[macro_export]
macro_rules! impl_event {
    ($t:ty, $id:expr) => {
        impl $t {
            pub fn type_id() -> u64 {
                $id
            }
        }

        impl $crate::StaticEvent for $t {
            fn static_type_id() -> u64 {
                $id
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
