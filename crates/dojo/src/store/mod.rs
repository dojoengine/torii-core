pub mod json;
pub mod postgres;
pub mod sqlite;

use crate::table::DojoTableInfo;
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
    async fn save_table(
        &self,
        owner: &Felt,
        table: &DojoTable,
        tx_hash: &Felt,
        block_number: u64,
    ) -> Result<(), Self::Error>;
    async fn read_tables(&self, owners: &[Felt]) -> Result<Vec<DojoTable>, Self::Error>;
    async fn read_table_map(
        &self,
        owners: &[Felt],
    ) -> Result<HashMap<Felt, DojoTableInfo>, Self::Error> {
        Ok(self
            .read_tables(owners)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}
