use introspect_types::serialize::CairoSerialization;
use serde::{ser::SerializeMap, Serialize, Serializer};

pub struct PostgresJsonSerializer;

impl CairoSerialization for PostgresJsonSerializer {
    fn serialize_byte_array<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8],
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("\\x{}", hex::encode(value)))
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
    fn serialize_enum<T, S: Serializer>(
        &self,
        serializer: S,
        variant_name: &str,
        value: &T,
    ) -> Result<S::Ok, S::Error>
    where
        T: Serialize,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("variant", variant_name)?;
        map.serialize_entry(variant_name, value)?;
        map.end()
    }
}
