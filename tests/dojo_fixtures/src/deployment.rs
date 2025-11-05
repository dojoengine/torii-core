use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use starknet::core::types::Felt;
use std::{path::PathBuf, process::Command};

use crate::types::DeploymentInfo;

pub fn extract_info(project_dir: &PathBuf, contract_tag: &str) -> Result<DeploymentInfo> {
    let output = Command::new("sozo")
        .current_dir(project_dir)
        .args(["inspect", "--json"])
        .output()
        .context("failed to execute `sozo inspect`")?;

    if !output.status.success() {
        return Err(anyhow!(
            "`sozo inspect` failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    #[derive(Deserialize)]
    struct WorldInfo {
        address: Felt,
    }

    #[derive(Deserialize)]
    struct ContractInfo {
        tag: String,
        address: Felt,
    }

    #[derive(Deserialize)]
    struct InspectOutput {
        world: WorldInfo,
        contracts: Vec<ContractInfo>,
    }

    let output_data: InspectOutput = serde_json::from_slice(&output.stdout)
        .context("failed to parse `sozo inspect` JSON output")?;

    let contract = output_data
        .contracts
        .iter()
        .find(|c| c.tag == contract_tag)
        .with_context(|| format!("contract '{}' not found in inspect output", contract_tag))?;

    Ok(DeploymentInfo {
        world_address: output_data.world.address,
        contract_address: contract.address,
    })
}
