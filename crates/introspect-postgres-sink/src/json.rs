use introspect_types::serialize::ToCairoDeSeFrom;
use introspect_types::serialize_def::CairoTypeSerialization;
use introspect_types::{CairoDeserializer, ResultDef, TupleDef, TypeDef};
use serde::ser::SerializeMap;
use serde::Serializer;

pub struct PostgresJsonSerializer;

impl CairoTypeSerialization for PostgresJsonSerializer {
    fn serialize_byte_array<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8],
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("\\x{}", hex::encode(value)))
    }
    fn serialize_string<S: Serializer>(
        &self,
        serializer: S,
        value: &str,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.replace('\0', "\u{FFFD}"))
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
    fn serialize_tuple<'a, S: Serializer>(
        &'a self,
        data: &mut impl CairoDeserializer,
        serializer: S,
        tuple: &'a TupleDef,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_map(Some(tuple.elements.len()))?;
        for (i, element) in tuple.elements.iter().enumerate() {
            seq.serialize_entry(&format!("_{i}"), &element.to_de_se(data, self))?;
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
        match type_def {
            TypeDef::None => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("_variant", name)?;
                map
            }
            _ => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_variant", name)?;
                map.serialize_entry(name, &type_def.to_de_se(data, self))?;
                map
            }
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
        match is_ok {
            true => map.serialize_entry("Ok", &result.ok.to_de_se(data, self))?,
            false => map.serialize_entry("Err", &result.err.to_de_se(data, self))?,
        }
        map.end()
    }
}
