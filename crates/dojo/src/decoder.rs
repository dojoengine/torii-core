use dojo_introspect_types::events::{
    DojoEvent, DojoEventSerde, EventEmitted, EventRegistered, EventUpgraded, ModelRegistered,
    ModelUpgraded, ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use introspect_types::{
    CairoEvent, CairoEventInfo, CairoSerde, DecodeError, DecodeResult, FeltSource, IntoFeltSource,
    SliceFeltSource, VecFeltSource,
};
use starknet::core::types::EmittedEvent;

use crate::error::{DojoToriiError, DojoToriiResult};

pub struct DojoDecoder;

impl DojoDecoder {
    pub fn decode_event(&self, event: &EmittedEvent) -> DojoToriiResult<DojoEvent> {
        let mut keys = (&event.keys).into_source();
        let mut data: CairoSerde<_> = (&event.data).into();
        let selector = keys.next()?;
        let selector_raw = selector.to_raw();
        match selector_raw {
            ModelRegistered::SELECTOR_RAW => Ok(
                ModelRegistered::deserialize_and_verify_event_enum(&mut keys, &mut data)?,
            ),
            ModelWithSchemaRegistered::SELECTOR_RAW => Ok(
                ModelWithSchemaRegistered::deserialize_and_verify_event_enum(&mut keys, &mut data)?,
            ),
            ModelUpgraded::SELECTOR_RAW => Ok(ModelUpgraded::deserialize_and_verify_event_enum(
                &mut keys, &mut data,
            )?),
            EventRegistered::SELECTOR_RAW => Ok(
                EventRegistered::deserialize_and_verify_event_enum(&mut keys, &mut data)?,
            ),
            EventUpgraded::SELECTOR_RAW => Ok(EventUpgraded::deserialize_and_verify_event_enum(
                &mut keys, &mut data,
            )?),
            StoreSetRecord::SELECTOR_RAW => Ok(StoreSetRecord::deserialize_and_verify_event_enum(
                &mut keys, &mut data,
            )?),
            StoreUpdateRecord::SELECTOR_RAW => Ok(
                StoreUpdateRecord::deserialize_and_verify_event_enum(&mut keys, &mut data)?,
            ),
            StoreUpdateMember::SELECTOR_RAW => Ok(
                StoreUpdateMember::deserialize_and_verify_event_enum(&mut keys, &mut data)?,
            ),
            StoreDelRecord::SELECTOR_RAW => Ok(StoreDelRecord::deserialize_and_verify_event_enum(
                &mut keys, &mut data,
            )?),
            EventEmitted::SELECTOR_RAW => Ok(EventEmitted::deserialize_and_verify_event_enum(
                &mut keys, &mut data,
            )?),
            _ => Err(DojoToriiError::UnknownDojoEventSelector(selector)),
        }
    }
}
