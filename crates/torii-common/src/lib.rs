//! Common utilities for Torii token indexers
//!
//! Provides efficient conversions between Starknet types and storage/wire formats,
//! and shared helpers like token metadata fetching.

pub mod json;
pub mod metadata;
pub mod token_uri;
pub mod utils;

pub use metadata::{MetadataFetcher, TokenMetadata};
use primitive_types::U256;
pub use token_uri::{
    process_token_uri_request, TokenStandard, TokenUriRequest, TokenUriResult, TokenUriSender,
    TokenUriService, TokenUriStore,
};

// ===== U256 conversions =====

/// Convert U256 to variable-length BLOB for storage (big-endian, compact)
///
/// Compression strategy:
/// - Zero value: 1 byte (0x00)
///
/// - Values < 2^128: 1-16 bytes (minimal encoding of low word)
/// - Values >= 2^128: Full encoding (17-32 bytes)
pub fn u256_to_blob(value: U256) -> Vec<u8> {
    match value.0 {
        [0, 0, 0, 0] => vec![0u8], // zero value
        [_, _, 0, 0] => {
            let val = value.low_u128().to_be_bytes();
            let start = val.iter().position(|&b| b != 0).unwrap_or(15);
            val[start..].to_vec() // compact encoding for < 2^128
        }
        _ => value.to_big_endian().to_vec(), // full encoding for >= 2^128
    }
}

/// Convert BLOB back to U256 (big-endian)
pub fn blob_to_u256(bytes: &[u8]) -> U256 {
    U256::from_big_endian(&bytes)
}

/// Alias for u256_to_blob (same format, different context name)
pub fn u256_to_bytes(value: U256) -> Vec<u8> {
    u256_to_blob(value)
}

/// Alias for blob_to_u256 (same format, different context name)
pub fn bytes_to_u256(bytes: &[u8]) -> U256 {
    blob_to_u256(bytes)
}
