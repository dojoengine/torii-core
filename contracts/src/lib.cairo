//! Batch SRC-5 Interface Checker
//!
//! This contract allows checking multiple contracts for SRC-5 interface support
//! in a single call. It gracefully handles failures (contracts that don't implement
//! supports_interface) by returning false instead of reverting.
//!
//! This is useful for indexers like Torii that need to identify contract types
//! for many contracts efficiently.

use starknet::ContractAddress;

/// SRC-5 supports_interface selector.
const SUPPORTS_INTERFACE_SELECTOR: felt252 =
    0xfe80f537b66d12a00b6d3c072b44afbb716e78dde5c3f0ef116ee93d3e3283;

#[starknet::interface]
pub trait IBatchSrc5Checker<T> {
    /// Check multiple contracts for interface support in a single call.
    ///
    /// # Arguments
    /// * `queries` - Array of (contract_address, interface_id) tuples to check
    ///
    /// # Returns
    /// Array of booleans, same length as queries. True if the contract supports
    /// the interface, false if it doesn't or if the call failed.
    fn batch_supports_interface(
        self: @T, queries: Array<(ContractAddress, felt252)>,
    ) -> Array<bool>;
}

#[starknet::contract]
pub mod BatchSrc5Checker {
    use starknet::ContractAddress;
    use starknet::syscalls::call_contract_syscall;
    use super::SUPPORTS_INTERFACE_SELECTOR;

    #[storage]
    struct Storage {}

    #[abi(embed_v0)]
    impl BatchSrc5CheckerImpl of super::IBatchSrc5Checker<ContractState> {
        fn batch_supports_interface(
            self: @ContractState, queries: Array<(ContractAddress, felt252)>,
        ) -> Array<bool> {
            let mut results: Array<bool> = array![];

            for (contract, interface_id) in queries {
                let supports = check_interface(contract, interface_id);
                results.append(supports);
            }

            results
        }
    }

    /// Check if a single contract supports an interface.
    /// Returns false if the call fails (contract doesn't implement SRC-5).
    fn check_interface(contract: ContractAddress, interface_id: felt252) -> bool {
        let calldata: Array<felt252> = array![interface_id];

        match call_contract_syscall(contract, SUPPORTS_INTERFACE_SELECTOR, calldata.span()) {
            Result::Ok(result) => {
                // supports_interface returns one felt252, non-zero = true.
                if result.len() > 0 {
                    *result.at(0) != 0
                } else {
                    false
                }
            },
            Result::Err(_) => {
                // Call failed (no entrypoint, reverted, etc.) = doesn't support.
                false
            },
        }
    }
}
