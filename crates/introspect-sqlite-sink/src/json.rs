use introspect_types::serialize::ToCairoDeSeFrom;
use introspect_types::serialize_def::CairoTypeSerialization;
use introspect_types::{CairoDeserializer, ResultDef, TupleDef, TypeDef};
use primitive_types::{U256, U512};
use serde::ser::SerializeMap;
use serde::Serializer;

pub struct SqliteJsonSerializer;

impl CairoTypeSerialization for SqliteJsonSerializer {
    fn serialize_byte_array<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8],
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("0x{}", hex::encode(value)))
    }

    fn serialize_felt<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8; 32],
    ) -> Result<S::Ok, S::Error> {
        self.serialize_byte_array(serializer, value)
    }

    fn serialize_eth_address<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8; 20],
    ) -> Result<S::Ok, S::Error> {
        self.serialize_byte_array(serializer, value)
    }

    fn serialize_u256<S: Serializer>(
        &self,
        serializer: S,
        value: U256,
    ) -> Result<S::Ok, S::Error> {
        let U256([a, b, c, d]) = value;
        let corrected = U256([d, c, b, a]);
        let bytes = corrected.to_big_endian();
        self.serialize_byte_array(serializer, &bytes)
    }

    fn serialize_u512<S: Serializer>(
        &self,
        serializer: S,
        value: U512,
    ) -> Result<S::Ok, S::Error> {
        let U512([a, b, c, d, e, f, g, h]) = value;
        let corrected = U512([h, g, f, e, d, c, b, a]);
        let bytes = corrected.to_big_endian();
        self.serialize_byte_array(serializer, &bytes)
    }

    fn serialize_tuple<'a, S: Serializer>(
        &'a self,
        data: &mut impl CairoDeserializer,
        serializer: S,
        tuple: &'a TupleDef,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_map(Some(tuple.elements.len()))?;
        for (index, element) in tuple.elements.iter().enumerate() {
            seq.serialize_entry(&format!("_{index}"), &element.to_de_se(data, self))?;
        }
        seq.end()
    }

    fn serialize_variant<'a, S: Serializer>(
        &'a self,
        data: &mut impl CairoDeserializer,
        serializer: S,
        name: &str,
        type_def: &'a TypeDef,
    ) -> Result<S::Ok, S::Error> {
        if type_def == &TypeDef::None {
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry("variant", name)?;
            map
        } else {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry("variant", name)?;
            map.serialize_entry(&format!("_{name}"), &type_def.to_de_se(data, self))?;
            map
        }
        .end()
    }

    fn serialize_result<'a, S: Serializer>(
        &'a self,
        data: &mut impl CairoDeserializer,
        serializer: S,
        result: &'a ResultDef,
        is_ok: bool,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("is_ok", &is_ok)?;
        if is_ok {
            map.serialize_entry("Ok", &result.ok.to_de_se(data, self))?;
        } else {
            map.serialize_entry("Err", &result.err.to_de_se(data, self))?;
        }
        map.end()
    }
}
