use introspect_types::{Enum, PrimaryValue, Struct, Value};
use serde_json::Value::{
    Array as JsonArray, Bool as JsonBool, Number as JsonNumber, Object as JsonObject,
    String as JsonString,
};
use serde_json::{Map, Value as JsonValue};
use starknet_types_core::felt::Felt;
use torii_types_introspect::UpdateRecordFieldsV1;

use crate::types::parse_variant_name;

pub fn bytes_to_hex<T: AsRef<[u8]>>(bytes: T) -> JsonValue {
    JsonString(hex::encode(bytes))
}

pub fn to_json_number<T: Into<serde_json::Number>>(value: T) -> JsonValue {
    JsonNumber(value.into())
}

pub fn to_json_string<T: ToString>(value: T) -> JsonValue {
    JsonString(value.to_string())
}

pub fn felt_to_json_bytea(felt: Felt) -> JsonValue {
    to_json_bytea(felt.to_bytes_be())
}

pub fn to_json_bytea<T: AsRef<[u8]>>(bytes: T) -> JsonValue {
    JsonString(format!("\\x{}", hex::encode(bytes)))
}

pub fn to_pg_json_array(values: &Vec<Value>) -> JsonValue {
    JsonArray(values.into_iter().map(|v| v.to_postgres_json()).collect())
}

pub fn to_json_tuple(values: &Vec<Value>) -> JsonValue {
    let mut map = Map::new();
    for (i, value) in values.iter().enumerate() {
        map.insert(format!("_{i}"), value.to_postgres_json());
    }
    JsonObject(map)
}

pub trait ToPostgresJson {
    fn to_postgres_json(&self) -> JsonValue;
}

impl ToPostgresJson for UpdateRecordFieldsV1 {
    fn to_postgres_json(&self) -> JsonValue {
        let mut map = Map::new();
        map.insert(
            self.primary.name.clone(),
            self.primary.value.to_postgres_json(),
        );
        for field in self.fields.iter() {
            let key = field.name.clone();
            let value = field.value.to_postgres_json();
            map.insert(key, value);
        }
        JsonObject(map)
    }
}

impl ToPostgresJson for Value {
    fn to_postgres_json(&self) -> JsonValue {
        match self {
            Value::Felt252(v)
            | Value::ClassHash(v)
            | Value::ContractAddress(v)
            | Value::EthAddress(v)
            | Value::StorageAddress(v)
            | Value::StorageBaseAddress(v) => felt_to_json_bytea(*v),
            Value::Bool(v) => JsonBool(*v),
            Value::U8(v) => to_json_number(*v),
            Value::U16(v) => to_json_number(*v),
            Value::U32(v) => to_json_number(*v),
            Value::U64(v) => to_json_string(*v),
            Value::U128(v) => to_json_string(*v),
            Value::U256(v) => to_json_string(*v),
            Value::U512(v) => to_json_string(*v),
            Value::I8(v) => to_json_number(*v),
            Value::I16(v) => to_json_number(*v),
            Value::I32(v) => to_json_number(*v),
            Value::I64(v) => to_json_string(*v),
            Value::I128(v) => to_json_string(*v),
            Value::Utf8String(s) | Value::ShortUtf8(s) => to_json_string(s),
            Value::ByteArray(b) => to_json_bytea(b),
            Value::Bytes31(b) => to_json_bytea(b),
            Value::Bytes31E(b) | Value::ByteArrayE(b) => to_json_bytea(&b.bytes),
            Value::Struct(s) => s.to_postgres_json(),
            Value::Enum(e) => e.to_postgres_json(),
            Value::Array(vs) | Value::FixedArray(vs) => to_pg_json_array(vs),
            Value::Tuple(v) => to_json_tuple(v),
            _ => serde_json::Value::Null,
        }
    }
}

impl ToPostgresJson for PrimaryValue {
    fn to_postgres_json(&self) -> JsonValue {
        match self {
            PrimaryValue::Felt252(v)
            | PrimaryValue::ClassHash(v)
            | PrimaryValue::ContractAddress(v)
            | PrimaryValue::EthAddress(v)
            | PrimaryValue::StorageAddress(v)
            | PrimaryValue::StorageBaseAddress(v) => felt_to_json_bytea(*v),
            PrimaryValue::Bytes31(v) => to_json_bytea(v),
            PrimaryValue::Bytes31E(v) => to_json_bytea(&v.bytes),
            PrimaryValue::ShortUtf8(v) => to_json_string(v),
            PrimaryValue::Bool(v) => JsonBool(*v),
            PrimaryValue::U8(v) => to_json_number(*v),
            PrimaryValue::U16(v) => to_json_number(*v),
            PrimaryValue::U32(v) => to_json_number(*v),
            PrimaryValue::U64(v) => to_json_string(*v),
            PrimaryValue::U128(v) => to_json_string(*v),
            PrimaryValue::I8(v) => to_json_number(*v),
            PrimaryValue::I16(v) => to_json_number(*v),
            PrimaryValue::I32(v) => to_json_number(*v),
            PrimaryValue::I64(v) => to_json_string(*v),
            PrimaryValue::I128(v) => to_json_string(*v),
        }
    }
}

impl ToPostgresJson for Struct {
    fn to_postgres_json(&self) -> JsonValue {
        let mut map = Map::new();
        for member in self.members.iter() {
            let key = member.name.clone();
            let value = member.value.to_postgres_json();
            map.insert(key, value);
        }
        JsonObject(map)
    }
}

impl ToPostgresJson for Enum {
    fn to_postgres_json(&self) -> JsonValue {
        let mut map = Map::new();
        map.insert(
            "variant".into(),
            JsonString(parse_variant_name(&self.variant)),
        );
        if self.value != Value::None {
            map.insert(format!("_{}", self.variant), self.value.to_postgres_json());
        }

        JsonObject(map)
    }
}
