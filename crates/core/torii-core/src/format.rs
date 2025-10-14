//! Formatting utilities for Torii and meant to be used by sinks and decoders.

use crate::FieldElement;

/// Formats a Felt into a hex string with 0x prefix and 66 hex characters.
pub fn felt_to_padded_hex(felt: &FieldElement) -> String {
    format!("{:#066x}", felt)
}
