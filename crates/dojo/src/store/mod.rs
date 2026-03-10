pub mod json;
pub mod postgres;

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
    async fn save_table(&self, owner: &Felt, table: &DojoTable) -> Result<(), Self::Error>;
    async fn load_tables(&self, owners: &[Felt]) -> Result<Vec<DojoTable>, Self::Error>;
    async fn load_table_map(
        &self,
        owners: &[Felt],
    ) -> Result<HashMap<Felt, DojoTableInfo>, Self::Error> {
        Ok(self
            .load_tables(owners)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}
