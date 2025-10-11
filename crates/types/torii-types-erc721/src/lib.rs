use serde::{Deserialize, Serialize};
use torii_core::{impl_event, type_id_from_url, FieldElement};

pub const TRANSFER_URL: &str = "torii.erc721/Transfer@1";
pub const TRANSFER_ID: u64 = type_id_from_url(TRANSFER_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferV1 {
    pub contract: FieldElement,
    pub from: FieldElement,
    pub to: FieldElement,
    pub token_id: FieldElement,
}

impl_event!(TransferV1, TRANSFER_URL);
