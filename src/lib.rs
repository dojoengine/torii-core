//! Torii - Modular blockchain indexer.
//!
//! This library aims at providing a modular and high-performance blockchain indexer.
//! The current implementation is still WIP, but gives a good idea of the architecture and the capabilities.

pub mod command;
pub mod config;
pub mod etl;
pub mod grpc;
pub mod http;
pub mod metrics;
pub mod runner;
// Include generated protobuf code
pub mod proto {
    pub mod torii {
        tonic::include_proto!("torii");
    }
}

// Re-export commonly used types for external sink authors
pub use async_trait::async_trait;
pub use config::{EtlConcurrencyConfig, ToriiConfig, ToriiConfigBuilder};
pub use runner::run;
pub use {axum, tokio, tonic};
// Re-export UpdateType for sink implementations
pub use grpc::UpdateType;

use grpc::{create_grpc_service, GrpcState, SubscriptionManager};
use http::create_http_router;

// Include the file descriptor set generated at build time.
// This is also exported publicly so external sink authors can use it for reflection.
const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../target/descriptor.bin");

/// Torii core gRPC service descriptor set (for reflection).
///
/// External sink authors should use this when building custom reflection services
/// that include both core Torii and their sink services.
///
/// # Example
///
/// ```rust,ignore
/// use torii::TORII_DESCRIPTOR_SET;
///
/// let reflection = tonic_reflection::server::Builder::configure()
///     .register_encoded_file_descriptor_set(TORII_DESCRIPTOR_SET)
///     .register_encoded_file_descriptor_set(my_sink::FILE_DESCRIPTOR_SET)
///     .build_v1()?;
/// ```
///
/// This descriptor set is generated at build time from `proto/torii.proto`
/// by the `build.rs` script. See `build.rs` for details.
pub const TORII_DESCRIPTOR_SET: &[u8] = FILE_DESCRIPTOR_SET;
