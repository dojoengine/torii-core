use starknet_types_core::felt::Felt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SqlFelt(pub [u8; 32]);

impl From<SqlFelt> for Felt {
    fn from(value: SqlFelt) -> Self {
        Felt::from_bytes_be(&value.0)
    }
}

impl From<Felt> for SqlFelt {
    fn from(value: Felt) -> Self {
        SqlFelt(value.to_bytes_be())
    }
}

impl From<&Felt> for SqlFelt {
    fn from(value: &Felt) -> Self {
        SqlFelt(value.to_bytes_be())
    }
}
