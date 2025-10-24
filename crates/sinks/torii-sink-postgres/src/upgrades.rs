use crate::{
    types::PostgresField,
    upgrade::{new_upgrade_error, Result},
    PostgresType,
};
use introspect_types::TypeDef;

pub fn upgrade_primitive_type(
    name: &str,
    old: &TypeDef,
    new: &TypeDef,
) -> Result<Option<PostgresType>> {
    use PostgresType::{BigInt, Felt252 as PgFelt252, Int, Int128, SmallInt, Uint128, Uint64};
    use TypeDef::{
        Bool, ClassHash, ContractAddress, EthAddress, Felt252, I128, I16, I32, I64, I8, U128, U16,
        U32, U64, U8,
    };
    match (new, old) {
        (Bool, Bool)
        | (U8 | I8 | I16, U8 | I8 | I16)
        | (U16 | I32, I32 | U16)
        | (U32 | I64, U32 | I64)
        | (U64, U64)
        | (U128, U128)
        | (I128, I128)
        | (Felt252, Felt252)
        | (ClassHash, ClassHash)
        | (ContractAddress, ContractAddress)
        | (EthAddress, EthAddress) => Ok(None),
        (I8 | U8 | I16, Bool) => Ok(Some(SmallInt)),
        (U16 | I32, Bool | I8 | U8 | I16) => Ok(Some(Int)),
        (U32 | I64, Bool | I8 | U8 | I16 | U16 | I32) => Ok(Some(BigInt)),
        (U64, Bool | I8 | U8 | I16 | U16 | I32 | U32 | I64) => Ok(Some(Uint64)),
        (I128, Bool | I8 | U8 | I16 | U16 | I32 | U64 | I64) => Ok(Some(Int128)),
        (U128, Bool | I8 | U8 | I16 | U16 | I32 | U64 | I64) => Ok(Some(Uint128)),
        (
            Felt252,
            Bool | U8 | U16 | U32 | U64 | U128 | ContractAddress | ClassHash | EthAddress,
        ) => Ok(Some(PgFelt252)),
        _ => new_upgrade_error(name, old, new),
    }
}
