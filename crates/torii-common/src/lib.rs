//! Common utilities for Torii token indexers
//!
//! Provides efficient conversions between Starknet types and storage/wire formats,
//! and shared helpers like token metadata fetching.

pub mod metadata;

use starknet::core::types::{Felt, U256};

pub use metadata::{MetadataFetcher, TokenMetadata};

// ===== Felt conversions =====

/// Convert Felt to 32-byte BLOB for storage (big-endian)
pub fn felt_to_blob(felt: Felt) -> Vec<u8> {
    felt.to_bytes_be().to_vec()
}

/// Convert BLOB back to Felt (big-endian)
pub fn blob_to_felt(bytes: &[u8]) -> Felt {
    let mut arr = [0u8; 32];
    let len = bytes.len().min(32);
    // Right-align for big-endian (pad zeros on the left)
    arr[32 - len..].copy_from_slice(&bytes[..len]);
    Felt::from_bytes_be(&arr)
}

/// Parse bytes to Felt (returns None if > 32 bytes)
pub fn bytes_to_felt(bytes: &[u8]) -> Option<Felt> {
    if bytes.len() > 32 {
        return None;
    }
    Some(blob_to_felt(bytes))
}

// ===== U256 conversions =====

/// Convert U256 to variable-length BLOB for storage (big-endian, compact)
///
/// Compression strategy:
/// - Zero value: 1 byte (0x00)
/// - Values < 2^128: 1-16 bytes (minimal encoding of low word)
/// - Values >= 2^128: Full encoding (17-32 bytes)
pub fn u256_to_blob(value: U256) -> Vec<u8> {
    let high = value.high();
    let low = value.low();

    if high == 0 {
        if low == 0 {
            return vec![0u8];
        }
        let bytes = low.to_be_bytes();
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(15);
        return bytes[start..].to_vec();
    }

    let mut result = Vec::with_capacity(32);
    let high_bytes = high.to_be_bytes();
    let high_start = high_bytes.iter().position(|&b| b != 0).unwrap_or(15);
    result.extend_from_slice(&high_bytes[high_start..]);
    result.extend_from_slice(&low.to_be_bytes());
    result
}

/// Convert BLOB back to U256 (big-endian)
pub fn blob_to_u256(bytes: &[u8]) -> U256 {
    let len = bytes.len();

    if len == 0 {
        return U256::from(0u64);
    }

    if len <= 16 {
        let mut low_bytes = [0u8; 16];
        low_bytes[16 - len..].copy_from_slice(bytes);
        let low = u128::from_be_bytes(low_bytes);
        U256::from_words(low, 0)
    } else {
        let high_len = len - 16;
        let mut high_bytes = [0u8; 16];
        high_bytes[16 - high_len..].copy_from_slice(&bytes[..high_len]);
        let high = u128::from_be_bytes(high_bytes);

        let mut low_bytes = [0u8; 16];
        low_bytes.copy_from_slice(&bytes[high_len..]);
        let low = u128::from_be_bytes(low_bytes);

        U256::from_words(low, high)
    }
}

/// Alias for u256_to_blob (same format, different context name)
pub fn u256_to_bytes(value: U256) -> Vec<u8> {
    u256_to_blob(value)
}

/// Alias for blob_to_u256 (same format, different context name)
pub fn bytes_to_u256(bytes: &[u8]) -> U256 {
    blob_to_u256(bytes)
}
