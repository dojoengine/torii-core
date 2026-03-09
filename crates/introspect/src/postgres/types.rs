use introspect_types::Attribute;
use starknet_types_core::felt::Felt;

#[derive(sqlx::Type)]
#[sqlx(type_name = "introspect.attribute")]
pub struct PgAttribute {
    pub name: String,
    pub data: Option<Vec<u8>>,
}

#[derive(sqlx::Type)]
#[sqlx(transparent)]
pub struct PgFelt(pub Vec<u8>);

impl From<PgAttribute> for Attribute {
    fn from(value: PgAttribute) -> Self {
        Attribute {
            name: value.name,
            data: value.data,
        }
    }
}

impl From<Attribute> for PgAttribute {
    fn from(value: Attribute) -> Self {
        PgAttribute {
            name: value.name,
            data: value.data,
        }
    }
}

impl From<PgFelt> for Felt {
    fn from(value: PgFelt) -> Self {
        Felt::from_bytes_be_slice(&value.0)
    }
}

impl From<Felt> for PgFelt {
    fn from(value: Felt) -> Self {
        PgFelt(value.to_bytes_be().to_vec())
    }
}

pub fn felt252_type(value: &Felt) -> String {
    format!("'\\x{}'::felt252", hex::encode(value.to_bytes_be()))
}

pub fn string_type(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn attribute_type(attr: &Attribute) -> String {
    let data = match &attr.data {
        Some(bytes) => format!("'\\x{}'::bytea", hex::encode(bytes)),
        None => "NULL".to_string(),
    };
    format!(
        "ROW({}, {})::introspect.attribute",
        string_type(&attr.name),
        data
    )
}
