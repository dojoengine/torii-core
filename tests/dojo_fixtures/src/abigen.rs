//! Leverages Cainome to generate Rust bindings for the contracts.
//! This eases the generation of the transactions and transaction execution.

pub mod baseline {
    use cainome::rs::abigen;

    abigen!(
        BaselineContract,
        "tests/dojo_fixtures/cairo/baseline/target/dev/dojo_introspect_baseline_c1.contract_class.json",
        type_aliases {
            dojo::contract::components::upgradeable::upgradeable_cpt::Event as UpgradeableEvent;
            dojo::contract::components::world_provider::world_provider_cpt::Event as WorldProviderEvent;
        },

    );
}

pub mod upgrade {
    use cainome::rs::abigen;

    abigen!(
        UpgradeContract,
        "tests/dojo_fixtures/cairo/upgrade/target/dev/dojo_introspect_upgrade_c1.contract_class.json",
        type_aliases {
            dojo::contract::components::upgradeable::upgradeable_cpt::Event as UpgradeableEvent;
            dojo::contract::components::world_provider::world_provider_cpt::Event as WorldProviderEvent;
        },
    );
}
