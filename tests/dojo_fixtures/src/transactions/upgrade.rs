//! Upgrade transaction sending logic.
//!
//! Uses cainome-generated bindings to send transactions that exercise all
//! upgrade contract functions, including new schema fields.

use anyhow::Result;
use katana_runner::KatanaRunner;
use starknet::core::types::Felt;

use crate::abigen::upgrade::{Enum1, Model, ModelLegacy, UpgradeContract};
use crate::types::DeploymentInfo;

/// Sends a series of transactions to the upgraded contract to generate events
/// with the new schema that includes the 'score' field.
pub async fn send_upgrade(katana_runner: &KatanaRunner, info: &DeploymentInfo) -> Result<()> {
    let account = katana_runner.account(0);
    let contract = UpgradeContract::new(info.contract_address, &account);

    // Test player address
    let player = Felt::from_hex_unchecked("0x1234");

    // Call: write_model (now with score field)
    let model = Model {
        player: player.into(),
        e: Enum1::Left,
        index: 100,
        score: 999,
    };
    contract.write_model(&model).send().await?;

    // Call: write_models
    let models = vec![
        Model {
            player: player.into(),
            e: Enum1::Right,
            index: 101,
            score: 1000,
        },
        Model {
            player: player.into(),
            e: Enum1::Up,
            index: 102,
            score: 1001,
        },
    ];
    contract.write_models(&models).send().await?;

    // Call: write_member_model (writes only the 'score' member)
    let model = Model {
        player: player.into(),
        e: Enum1::Down,
        index: 103,
        score: 1337,
    };
    contract.write_member_model(&model).send().await?;

    // Call: write_member_of_models
    let models = vec![
        Model {
            player: player.into(),
            e: Enum1::Left,
            index: 104,
            score: 2000,
        },
        Model {
            player: player.into(),
            e: Enum1::Right,
            index: 105,
            score: 2001,
        },
    ];
    contract.write_member_of_models(&models).send().await?;

    // Call: write_member_score (new function in upgrade)
    contract
        .write_member_score(&player.into(), &1337u32.into())
        .send()
        .await?;

    // Call: write_model_legacy (now with score field)
    let model_legacy = ModelLegacy {
        player: player.into(),
        e: Enum1::Left,
        index: 200,
        score: 888,
    };
    contract.write_model_legacy(&model_legacy).send().await?;

    // Call: write_models_legacy
    let models_legacy = vec![
        ModelLegacy {
            player: player.into(),
            e: Enum1::Right,
            index: 201,
            score: 889,
        },
        ModelLegacy {
            player: player.into(),
            e: Enum1::Up,
            index: 202,
            score: 890,
        },
    ];
    contract.write_models_legacy(&models_legacy).send().await?;

    // Call: write_member_model_legacy
    let model_legacy = ModelLegacy {
        player: player.into(),
        e: Enum1::Down,
        index: 203,
        score: 891,
    };
    contract.write_member_model_legacy(&model_legacy).send().await?;

    // Call: write_member_of_models_legacy
    let models_legacy = vec![
        ModelLegacy {
            player: player.into(),
            e: Enum1::Left,
            index: 204,
            score: 892,
        },
        ModelLegacy {
            player: player.into(),
            e: Enum1::Right,
            index: 205,
            score: 893,
        },
    ];
    contract.write_member_of_models_legacy(&models_legacy).send().await?;

    // Call: write_member_score_legacy (new function in upgrade)
    contract.write_member_score_legacy(&player.into(), &7777u32.into()).send().await?;

    Ok(())
}
