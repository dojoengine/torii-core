use starknet::ContractAddress;

#[derive(Serde, Copy, Drop, Introspect, PartialEq, Debug, Default, DojoStore)]
pub enum Enum1 {
    #[default]
    Left,
    Right,
    Up,
    Down,
    New: u32,
}

#[derive(Copy, Drop, Serde, Debug)]
#[dojo::model]
pub struct Model {
    #[key]
    pub player: ContractAddress,
    pub e: Enum1,
    pub index: u32,
    pub score: u32,
}

#[derive(Copy, Drop, Serde, Debug, DojoLegacyStore)]
#[dojo::model]
pub struct ModelLegacy {
    #[key]
    pub player: ContractAddress,
    pub e: Enum1,
    pub index: u32,
    pub score: u32,
}

#[starknet::interface]
pub trait IDojoTest<T> {
    fn write_model(ref self: T, m: Model);
    fn write_models(ref self: T, ms: Span<Model>);
    fn write_member_model(ref self: T, m: Model);
    fn write_member_of_models(ref self: T, ms: Span<Model>);
    fn write_member_score(ref self: T, player: ContractAddress, score: u32);

    fn write_model_legacy(ref self: T, m: ModelLegacy);
    fn write_models_legacy(ref self: T, ms: Span<ModelLegacy>);
    fn write_member_model_legacy(ref self: T, m: ModelLegacy);
    fn write_member_of_models_legacy(ref self: T, ms: Span<ModelLegacy>);
    fn write_member_score_legacy(ref self: T, player: ContractAddress, score: u32);
}

#[dojo::contract]
pub mod c1 {
    use dojo::model::{ModelPtr, ModelStorage};
    use super::*;

    #[abi(embed_v0)]
    impl DojoTestImpl of IDojoTest<ContractState> {
        fn write_model(ref self: ContractState, m: Model) {
            let mut world = self.world_default();
            world.write_model(@m);
        }

        fn write_models(ref self: ContractState, ms: Span<Model>) {
            let mut world = self.world_default();

            let mut ms = ms.clone();
            let mut snapshots: Array<@Model> = array![];
            while let Option::Some(m) = ms.pop_front() {
                snapshots.append(m);
            }

            world.write_models(snapshots.span());
        }

        fn write_member_model(ref self: ContractState, m: Model) {
            let mut world = self.world_default();
            world.write_member(m.ptr(), selector!("score"), m.score);
        }

        fn write_member_of_models(ref self: ContractState, ms: Span<Model>) {
            let mut world = self.world_default();

            let mut ms = ms.clone();
            let mut ptrs: Array<ModelPtr> = array![];
            let mut values: Array<u32> = array![];

            while let Option::Some(m) = ms.pop_front() {
                ptrs.append(m.ptr());
                values.append(*m.score);
            }

            world.write_member_of_models(ptrs.span(), selector!("score"), values.span());
        }

        fn write_member_score(ref self: ContractState, player: ContractAddress, score: u32) {
            let mut world = self.world_default();
            let model = Model { player, e: Enum1::Left, index: 0, score };
            world.write_member(model.ptr(), selector!("score"), score);
        }

        fn write_model_legacy(ref self: ContractState, m: ModelLegacy) {
            let mut world = self.world_default();
            world.write_model(@m);
        }

        fn write_models_legacy(ref self: ContractState, ms: Span<ModelLegacy>) {
            let mut world = self.world_default();

            let mut ms = ms.clone();
            let mut snapshots: Array<@ModelLegacy> = array![];
            while let Option::Some(m) = ms.pop_front() {
                snapshots.append(m);
            }

            world.write_models(snapshots.span());
        }

        fn write_member_model_legacy(ref self: ContractState, m: ModelLegacy) {
            let mut world = self.world_default();
            world.write_member_legacy(m.ptr(), selector!("score"), m.score);
        }

        fn write_member_of_models_legacy(ref self: ContractState, ms: Span<ModelLegacy>) {
            let mut world = self.world_default();

            let mut ms = ms.clone();
            let mut ptrs: Array<ModelPtr> = array![];
            let mut values: Array<u32> = array![];

            while let Option::Some(m) = ms.pop_front() {
                ptrs.append(m.ptr());
                values.append(*m.score);
            }

            world.write_member_of_models_legacy(ptrs.span(), selector!("score"), values.span());
        }

        fn write_member_score_legacy(
            ref self: ContractState,
            player: ContractAddress,
            score: u32,
        ) {
            let mut world = self.world_default();
            let model = ModelLegacy { player, e: Enum1::Left, index: 0, score };
            world.write_member_legacy(model.ptr(), selector!("score"), score);
        }
    }

    #[generate_trait]
    impl InternalImpl of InternalTrait {
        fn world_default(self: @ContractState) -> dojo::world::WorldStorage {
            self.world(@"ns")
        }
    }
}
