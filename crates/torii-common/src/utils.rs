use primitive_types::U256;
use starknet_types_raw::Felt;

pub trait ElementsInto<T> {
    fn elements_into(self) -> Vec<T>;
}

pub trait ElementsFrom<T> {
    fn elements_from(vec: Vec<T>) -> Self;
}

impl<T, U> ElementsInto<U> for Vec<T>
where
    T: Into<U>,
{
    fn elements_into(self) -> Vec<U> {
        self.into_iter().map(T::into).collect()
    }
}

impl<T, U> ElementsFrom<T> for Vec<U>
where
    U: From<T>,
{
    fn elements_from(vec: Vec<T>) -> Self {
        vec.into_iter().map(U::from).collect()
    }
}

/// Parse a U256 result from balance_of return value
///
/// ERC20 balance_of typically returns:
/// - Cairo 0: A single felt (fits in 252 bits, usually enough for balances)
/// - Cairo 1 with u256: Two felts [low, high] representing a 256-bit value
pub fn parse_u256_result(result: Vec<Felt>) -> U256 {
    match result.len() {
        0 => U256::from(0u64),
        1 => {
            // Single felt - convert to U256
            // Felt is 252 bits max, so it fits in the low part
            // Take the lower 16 bytes for u128 (fits any felt value)
            felt_to_u256(result.into_iter().next().unwrap())
        }
        _ => {
            // Two felts: [low, high] for u256
            let mut iter = result.into_iter();
            felt_pair_to_u256(iter.next().unwrap(), iter.next().unwrap())
        }
    }
}

pub fn felt_pair_to_u256(low: Felt, high: Felt) -> U256 {
    let [l0, l1, _, _] = low.to_le_words();
    let [h0, h1, _, _] = high.to_le_words();
    U256([l0, l1, h0, h1])
}

pub fn felt_to_u256(value: Felt) -> U256 {
    U256(value.to_le_words())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_u256_empty() {
        let result = parse_u256_result(vec![]);
        assert_eq!(result, U256::from(0u64));
    }

    #[test]
    fn test_parse_u256_single_felt() {
        let felt = Felt::from(1000u64);
        let result = parse_u256_result(vec![felt]);
        assert_eq!(result, U256::from(1000u64));
    }

    #[test]
    fn test_parse_u256_two_felts() {
        // low = 100, high = 0
        let low = Felt::from(100u64);
        let high = Felt::from(0u64);
        let result = parse_u256_result(vec![low, high]);
        assert_eq!(result, U256::from(100u64));

        // Test with high value
        let low = Felt::from(0u64);
        let high = Felt::from(1u64);
        let result = parse_u256_result(vec![low, high]);
        // high = 1 means value = 1 * 2^128
        let expected = U256::from(1u64) << 128;
        assert_eq!(result, expected);
    }
}
