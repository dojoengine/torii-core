use introspect_types::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PostgresValueError {
    #[error("Failed to convert value to Postgres format")]
    ConversionError,
    #[error("Unsupported value type for Postgres conversion")]
    UnsupportedType,
}

type Result<T> = std::result::Result<T, PostgresValueError>;

// pub trait ToPostgresValue {
//     fn to_postgres_value(&self) -> Result<String>;
// }

// impl ToPostgresValue for Value {
//     fn to_postgres_value(&self) -> Result<String> {
//         match self {
//             Value::Felt252(v)
//             | Value::ClassHash(v)
//             | Value::ContractAddress(v)
//             | Value::EthAddress(v)
//             | Value::StorageAddress(v)
//             | Value::StorageBaseAddress(v) => Ok(v.to_string()),
//             Value::Bool(v) => Ok(v.to_string()),
//             Value::U8(v) => Ok(v.to_string()),
//             Value::U16(v) => Ok(v.to_string()),
//             Value::U32(v) => Ok(v.to_string()),
//             Value::U64(v) => Ok(v.to_string()),
//             Value::U128(v) => Ok(v.to_string()),
//             Value::U256(v) => Ok(v.to_string()),
//             Value::I8(v) => Ok(v.to_string()),
//             Value::I16(v) => Ok(v.to_string()),
//             Value::I32(v) => Ok(v.to_string()),
//             Value::I64(v) => Ok(v.to_string()),
//             Value::I128(v) => Ok(v.to_string()),
//             Value::Utf8String(s) => Ok(s.clone()),
//             _ => Err(PostgresValueError::UnsupportedType),
//         }
//     }
// }
