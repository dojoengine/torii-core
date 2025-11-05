//! Baseline transaction sending logic.
//!
//! Uses cainome-generated bindings to send transactions that exercise all
//! baseline contract functions.

use anyhow::Result;
use katana_runner::KatanaRunner;
use starknet::core::types::Felt;

use crate::abigen::baseline::{BaselineContract, Enum1, Model, ModelLegacy};
use crate::types::DeploymentInfo;

/// Sends a series of transactions to the baseline contract to generate events.
pub async fn send_baseline(katana_runner: &KatanaRunner, info: &DeploymentInfo) -> Result<()> {
    let account = katana_runner.account(0);
    let contract = BaselineContract::new(info.contract_address, &account);

    // Test player address
    let player = Felt::from_hex_unchecked("0x1234");

    // Call: write_model
    let model = Model {
        player: player.into(),
        e: Enum1::Left,
        index: 42,
    };
    contract.write_model(&model).send().await?;

    // Call: write_models (with 2 models)
    let models = vec![
        Model {
            player: player.into(),
            e: Enum1::Right,
            index: 1,
        },
        Model {
            player: player.into(),
            e: Enum1::Up,
            index: 2,
        },
    ];
    contract.write_models(&models).send().await?;

    // Call: write_member_model (writes only the 'e' member)
    let model = Model {
        player: player.into(),
        e: Enum1::Down,
        index: 3,
    };
    contract.write_member_model(&model).send().await?;

    // Call: write_member_of_models
    let models = vec![
        Model {
            player: player.into(),
            e: Enum1::Left,
            index: 4,
        },
        Model {
            player: player.into(),
            e: Enum1::Right,
            index: 5,
        },
    ];
    contract.write_member_of_models(&models).send().await?;

    // Call: write_model_legacy
    let model_legacy = ModelLegacy {
        player: player.into(),
        e: Enum1::Left,
        index: 10,
    };
    contract.write_model_legacy(&model_legacy).send().await?;

    // Call: write_models_legacy
    let models_legacy = vec![
        ModelLegacy {
            player: player.into(),
            e: Enum1::Right,
            index: 11,
        },
        ModelLegacy {
            player: player.into(),
            e: Enum1::Up,
            index: 12,
        },
    ];
    contract.write_models_legacy(&models_legacy).send().await?;

    // Call: write_member_model_legacy
    let model_legacy = ModelLegacy {
        player: player.into(),
        e: Enum1::Down,
        index: 13,
    };
    contract.write_member_model_legacy(&model_legacy).send().await?;

    // Call: write_member_of_models_legacy
    let models_legacy = vec![
        ModelLegacy {
            player: player.into(),
            e: Enum1::Left,
            index: 14,
        },
        ModelLegacy {
            player: player.into(),
            e: Enum1::Right,
            index: 15,
        },
    ];
    contract.write_member_of_models_legacy(&models_legacy).send().await?;

    Ok(())
}
