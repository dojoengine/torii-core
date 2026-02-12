use dojo_introspect_types::events::{
    DojoEvent, DojoEventSerde, EventEmitted, EventRegistered, EventUpgraded, ModelRegistered,
    ModelUpgraded, ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use introspect_types::{
    CairoEvent, CairoSerde, DecodeError, DecodeResult, FeltSource, IntoFeltSource, VecFeltSource,
};
use starknet::core::types::EmittedEvent;

pub struct DojoDecoder;

impl DojoDecoder {
    pub fn decode_event(&self, event: EmittedEvent) -> DecodeResult<DojoEvent> {
        let mut keys = event.keys.into_source();
        let mut data = event.data.into_source();
        let selector = keys.next()?.to_raw();
        match selector {
            ModelRegistered::SELECTOR_RAW => {
                ModelRegistered::deserialize_and_verify_event_enum(&mut keys, &mut data)
            }
            ModelWithSchemaRegistered::SELECTOR_RAW => {
                ModelWithSchemaRegistered::deserialize_and_verify_event_enum(&mut keys, &mut data)
            }
            ModelUpgraded::SELECTOR_RAW => {
                ModelUpgraded::deserialize_and_verify_event_enum(&mut keys, &mut data)
            }
            EventRegistered::SELECTOR_RAW => {
                EventRegistered::deserialize_and_verify_event_enum(&mut keys, &mut data)
            }
            EventUpgraded::SELECTOR_RAW => {
                EventUpgraded::deserialize_and_verify_event_enum(&mut keys, &mut data)
            }
            StoreSetRecord::SELECTOR_RAW => {
                StoreSetRecord::deserialize_and_verify_event_enum(&mut keys, &mut data)
            }
            StoreUpdateRecord::SELECTOR_RAW => {
                StoreUpdateRecord::deserialize_and_verify_event_enum(&mut keys, &mut data)
            }
            StoreUpdateMember::SELECTOR_RAW => {
                StoreUpdateMember::deserialize_and_verify_event_enum(&mut keys, &mut data)
            }
            StoreDelRecord::SELECTOR_RAW => {
                StoreDelRecord::deserialize_and_verify_event_enum(&mut keys, &mut data)
            }
            EventEmitted::SELECTOR_RAW => {
                EventEmitted::deserialize_and_verify_event_enum(&mut keys, &mut data)
            }
            _ => Err(DecodeError::Message("Unknown Dojo Event selector")),
        }
    }
}
