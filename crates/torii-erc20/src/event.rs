use primitive_types::U256;
use starknet_types_raw::Felt;
use torii::etl::EventBody;
use torii::typed_body_impl;

#[derive(Debug, Clone)]
pub enum Erc20Msg {
    Transfer(Transfer),
    Approval(Approval),
}

typed_body_impl!(Erc20Msg, "erc20");

pub type Erc20Body = EventBody<Erc20Msg>;

impl From<Transfer> for Erc20Msg {
    fn from(value: Transfer) -> Self {
        Erc20Msg::Transfer(value)
    }
}

impl From<Approval> for Erc20Msg {
    fn from(value: Approval) -> Self {
        Erc20Msg::Approval(value)
    }
}

/// Transfer event from ERC20 token
#[derive(Debug, Clone)]
pub struct Transfer {
    pub from: Felt,
    pub to: Felt,
    /// Amount as U256 (256-bit), properly representing ERC20 token amounts
    pub amount: U256,
}

/// Approval event from ERC20 token
#[derive(Debug, Clone)]
pub struct Approval {
    pub owner: Felt,
    pub spender: Felt,
    /// Amount as U256 (256-bit), properly representing ERC20 token amounts
    pub amount: U256,
}
