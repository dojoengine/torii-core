pub mod json;
pub mod postgres;

use crate::DojoTable;
use async_trait::async_trait;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;

#[async_trait]
pub trait DojoStoreTrait
where
    Self: Send + Sync + 'static + Sized,
{
    type Error: std::error::Error;

    async fn save_table(&self, table: &DojoTable) -> Result<(), Self::Error>;
    async fn load_tables(&self) -> Result<Vec<DojoTable>, Self::Error>;
    async fn load_table_map(&self) -> Result<HashMap<Felt, DojoTable>, Self::Error> {
        Ok(self
            .load_tables()
            .await?
            .into_iter()
            .map(|table| (table.id, table))
            .collect())
    }
}
