use serde::{Deserialize, Serialize};
use torii_core::{impl_event, FieldElement};

pub const TRANSFER_URL: &str = "torii.erc20/Transfer@1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferV1 {
    pub contract: FieldElement,
    pub from: FieldElement,
    pub to: FieldElement,
    pub amount: u128,
}

impl_event!(TransferV1, TRANSFER_URL);
