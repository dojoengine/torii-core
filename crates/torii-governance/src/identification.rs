use anyhow::Result;
use starknet::core::types::Felt;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::ContractAbi;
use torii::etl::identification::IdentificationRule;

use crate::discovery::governance_decoder_id;

pub struct GovernanceRule;

impl GovernanceRule {
    pub fn new() -> Self {
        Self
    }
}

impl Default for GovernanceRule {
    fn default() -> Self {
        Self::new()
    }
}

impl IdentificationRule for GovernanceRule {
    fn name(&self) -> &'static str {
        "governance"
    }

    fn decoder_ids(&self) -> Vec<DecoderId> {
        vec![governance_decoder_id()]
    }

    fn identify_by_abi(
        &self,
        _contract_address: Felt,
        _class_hash: Felt,
        abi: &ContractAbi,
    ) -> Result<Vec<DecoderId>> {
        let has_supports_interface = abi.has_function("supports_interface");
        let has_token = abi.has_function("token");
        let has_state = abi.has_function("state");
        let has_snapshot = abi.has_function("proposal_snapshot");
        let has_deadline = abi.has_function("proposal_deadline");
        let has_proposal_event = abi.has_event("ProposalCreated") || abi.has_event("VoteCast");

        if has_supports_interface
            && has_token
            && has_state
            && has_snapshot
            && has_deadline
            && has_proposal_event
        {
            Ok(vec![governance_decoder_id()])
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn governance_rule_has_expected_decoder() {
        let ids = GovernanceRule::new().decoder_ids();
        assert_eq!(ids, vec![governance_decoder_id()]);
    }
}
