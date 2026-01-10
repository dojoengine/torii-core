//! Example identification rules for common contract types
//!
//! These are reference implementations showing how to create identification rules
//! for standard contract interfaces like ERC20 and ERC721.

use anyhow::Result;
use starknet::core::types::Felt;

use super::contract_registry::{ContractAbi, DecoderId, IdentificationRule};

/// ERC20 token identification rule
///
/// Identifies contracts as ERC20 tokens using:
/// - SRC-5 interface ID (if supported)
/// - ABI heuristics (presence of transfer, balance_of, etc.)
///
/// # Example
///
/// ```rust,ignore
/// let mut registry = ContractRegistry::new(
///     ContractIdentificationMode::SRC5 | ContractIdentificationMode::ABI_HEURISTICS
/// );
/// registry.add_rule(Box::new(Erc20Rule));
/// ```
pub struct Erc20Rule;

impl IdentificationRule for Erc20Rule {
    fn name(&self) -> &str {
        "ERC20"
    }

    fn src5_interface(&self) -> Option<(Felt, Vec<DecoderId>)> {
        // SRC-5 ERC20 interface ID
        // This is a placeholder - use the actual interface ID from the SRC-5 standard
        let interface_id = Felt::from_hex_unchecked(
            "0x036e04ccfceac9a96c48892a20a1fe83d03301e888062f63cf60aa8daad0395a",
        );

        let decoder_ids = vec![DecoderId::new("erc20")];

        Some((interface_id, decoder_ids))
    }

    fn identify_by_abi(
        &self,
        _contract_address: Felt,
        _class_hash: Felt,
        abi: &ContractAbi,
    ) -> Result<Vec<DecoderId>> {
        // Check for key ERC20 functions
        let has_transfer = abi.has_function("transfer");
        let has_balance_of = abi.has_function("balance_of") || abi.has_function("balanceOf");
        let has_total_supply =
            abi.has_function("total_supply") || abi.has_function("totalSupply");

        // Check for Transfer event
        let has_transfer_event = abi.has_event("Transfer");

        if (has_transfer && has_balance_of) || (has_transfer_event && has_total_supply) {
            Ok(vec![DecoderId::new("erc20")])
        } else {
            Ok(vec![])
        }
    }
}

/// ERC721 NFT identification rule
///
/// Identifies contracts as ERC721 NFTs using:
/// - SRC-5 interface ID (if supported)
/// - ABI heuristics (presence of owner_of, token_uri, etc.)
///
/// Note: ERC721 also has a Transfer event, so it's important to distinguish
/// from ERC20 by checking for unique functions like owner_of.
pub struct Erc721Rule;

impl IdentificationRule for Erc721Rule {
    fn name(&self) -> &str {
        "ERC721"
    }

    fn src5_interface(&self) -> Option<(Felt, Vec<DecoderId>)> {
        // SRC-5 ERC721 interface ID
        // This is a placeholder - use the actual interface ID from the SRC-5 standard
        let interface_id = Felt::from_hex_unchecked(
            "0x033eb2f23c90e08c10b768fd0f7c3eed0b43c0f8c0baaf79e3a0c1d6a0d5d0de",
        );

        let decoder_ids = vec![DecoderId::new("erc721")];

        Some((interface_id, decoder_ids))
    }

    fn identify_by_abi(
        &self,
        _contract_address: Felt,
        _class_hash: Felt,
        abi: &ContractAbi,
    ) -> Result<Vec<DecoderId>> {
        // Check for key ERC721 functions
        let has_owner_of = abi.has_function("owner_of") || abi.has_function("ownerOf");
        let has_token_uri = abi.has_function("token_uri") || abi.has_function("tokenURI");
        let has_safe_transfer_from = abi.has_function("safe_transfer_from")
            || abi.has_function("safeTransferFrom");

        // Check for Transfer event (similar to ERC20, but combined with NFT-specific functions)
        let has_transfer_event = abi.has_event("Transfer");

        if (has_owner_of && has_token_uri) || (has_transfer_event && has_safe_transfer_from) {
            Ok(vec![DecoderId::new("erc721")])
        } else {
            Ok(vec![])
        }
    }
}

/// Multi-decoder rule for contracts that implement multiple interfaces
///
/// Example: A contract that is both an ERC20 and has custom game logic.
/// This rule would return multiple decoder IDs.
pub struct HybridTokenRule;

impl IdentificationRule for HybridTokenRule {
    fn name(&self) -> &str {
        "HybridToken"
    }

    fn identify_by_abi(
        &self,
        _contract_address: Felt,
        _class_hash: Felt,
        abi: &ContractAbi,
    ) -> Result<Vec<DecoderId>> {
        let mut decoder_ids = Vec::new();

        // Check if it's an ERC20
        if abi.has_function("transfer") && abi.has_function("balance_of") {
            decoder_ids.push(DecoderId::new("erc20"));
        }

        // Check if it has game-specific events
        if abi.has_event("GameAction") || abi.has_event("PlayerMove") {
            decoder_ids.push(DecoderId::new("game"));
        }

        Ok(decoder_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_erc20_rule_name() {
        let rule = Erc20Rule;
        assert_eq!(rule.name(), "ERC20");
    }

    #[test]
    fn test_erc721_rule_name() {
        let rule = Erc721Rule;
        assert_eq!(rule.name(), "ERC721");
    }

    #[test]
    fn test_erc20_src5_interface() {
        let rule = Erc20Rule;
        let (interface_id, decoder_ids) = rule.src5_interface().unwrap();

        assert_eq!(decoder_ids.len(), 1);
        assert_eq!(decoder_ids[0], DecoderId::new("erc20"));
        assert!(interface_id != Felt::ZERO);
    }

    #[test]
    fn test_erc721_src5_interface() {
        let rule = Erc721Rule;
        let (interface_id, decoder_ids) = rule.src5_interface().unwrap();

        assert_eq!(decoder_ids.len(), 1);
        assert_eq!(decoder_ids[0], DecoderId::new("erc721"));
        assert!(interface_id != Felt::ZERO);
    }

    #[test]
    fn test_decoder_id_consistency() {
        // DecoderId should be deterministic based on name
        let id1 = DecoderId::new("erc20");
        let id2 = DecoderId::new("erc20");
        let id3 = DecoderId::new("erc721");

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }
}
