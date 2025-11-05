use serde::Serialize;
use starknet::core::types::{EmittedEvent, Felt};

/// Output of the fixture generation.
#[derive(Serialize)]
pub struct FixtureOutput {
    pub world_address: Felt,
    pub events: Vec<EmittedEvent>,
}

/// Information about the deployed contracts.
///
/// Currently, only one contract address is used. Can be extended in the future.
#[derive(Debug)]
pub struct DeploymentInfo {
    pub world_address: Felt,
    pub contract_address: Felt,
}
