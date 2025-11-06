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

    // Cycle through distinct player addresses to populate the upgraded schema with multiple rows
    let mut players = [
        Felt::from_hex_unchecked("0x200"),
        Felt::from_hex_unchecked("0x201"),
        Felt::from_hex_unchecked("0x202"),
        Felt::from_hex_unchecked("0x203"),
        Felt::from_hex_unchecked("0x204"),
        Felt::from_hex_unchecked("0x205"),
        Felt::from_hex_unchecked("0x206"),
        Felt::from_hex_unchecked("0x207"),
        Felt::from_hex_unchecked("0x208"),
        Felt::from_hex_unchecked("0x209"),
        Felt::from_hex_unchecked("0x20a"),
        Felt::from_hex_unchecked("0x20b"),
        Felt::from_hex_unchecked("0x20c"),
        Felt::from_hex_unchecked("0x20d"),
        Felt::from_hex_unchecked("0x20e"),
        Felt::from_hex_unchecked("0x20f"),
        Felt::from_hex_unchecked("0x210"),
        Felt::from_hex_unchecked("0x211"),
    ]
    .into_iter();
    let mut next_player = || -> Felt {
        players
            .next()
            .expect("insufficient upgrade player addresses configured")
    };

    // Call: write_model (now with score field)
    let model = Model {
        player: next_player().into(),
        e: Enum1::Left,
        index: 100,
        score: 999,
    };
    contract.write_model(&model).send().await?;

    // Call: write_models
    let models = vec![
        Model {
            player: next_player().into(),
            e: Enum1::Right,
            index: 101,
            score: 1000,
        },
        Model {
            player: next_player().into(),
            e: Enum1::Up,
            index: 102,
            score: 1001,
        },
    ];
    contract.write_models(&models).send().await?;

    // Call: write_member_model (writes only the 'score' member)
    let model = Model {
        player: next_player().into(),
        e: Enum1::Down,
        index: 103,
        score: 1337,
    };
    contract.write_member_model(&model).send().await?;

    // Call: write_member_of_models
    let models = vec![
        Model {
            player: next_player().into(),
            e: Enum1::Left,
            index: 104,
            score: 2000,
        },
        Model {
            player: next_player().into(),
            e: Enum1::Right,
            index: 105,
            score: 2001,
        },
    ];
    contract.write_member_of_models(&models).send().await?;

    // Call: write_member_score (new function in upgrade)
    contract
        .write_member_score(&next_player().into(), &1337u32.into())
        .send()
        .await?;

    // Call: write_model_legacy (now with score field)
    let model_legacy = ModelLegacy {
        player: next_player().into(),
        e: Enum1::Left,
        index: 200,
        score: 888,
    };
    contract.write_model_legacy(&model_legacy).send().await?;

    // Call: write_models_legacy
    let models_legacy = vec![
        ModelLegacy {
            player: next_player().into(),
            e: Enum1::Right,
            index: 201,
            score: 889,
        },
        ModelLegacy {
            player: next_player().into(),
            e: Enum1::Up,
            index: 202,
            score: 890,
        },
    ];
    contract.write_models_legacy(&models_legacy).send().await?;

    // Call: write_member_model_legacy
    let model_legacy = ModelLegacy {
        player: next_player().into(),
        e: Enum1::Down,
        index: 203,
        score: 891,
    };
    contract
        .write_member_model_legacy(&model_legacy)
        .send()
        .await?;

    // Call: write_member_of_models_legacy
    let models_legacy = vec![
        ModelLegacy {
            player: next_player().into(),
            e: Enum1::Left,
            index: 204,
            score: 892,
        },
        ModelLegacy {
            player: next_player().into(),
            e: Enum1::Right,
            index: 205,
            score: 893,
        },
    ];
    contract
        .write_member_of_models_legacy(&models_legacy)
        .send()
        .await?;

    // Call: write_member_score_legacy (new function in upgrade)
    contract
        .write_member_score_legacy(&next_player().into(), &7777u32.into())
        .send()
        .await?;

    // Additional coverage: send extra transactions with fresh players
    let extra_models = vec![
        Model {
            player: next_player().into(),
            e: Enum1::Left,
            index: 300,
            score: 4000,
        },
        Model {
            player: next_player().into(),
            e: Enum1::Right,
            index: 301,
            score: 4001,
        },
    ];
    contract.write_models(&extra_models).send().await?;

    let bonus_score_player = next_player();
    contract
        .write_member_score(&bonus_score_player.into(), &4242u32.into())
        .send()
        .await?;

    let extra_model_legacy = ModelLegacy {
        player: next_player().into(),
        e: Enum1::Up,
        index: 302,
        score: 4002,
    };
    contract
        .write_model_legacy(&extra_model_legacy)
        .send()
        .await?;

    Ok(())
}
