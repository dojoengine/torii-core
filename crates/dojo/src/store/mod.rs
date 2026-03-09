pub mod json;
pub mod postgres;

use crate::table::DojoTableInfo;
use crate::DojoTable;
use async_trait::async_trait;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::hash::Hash;

#[async_trait]
pub trait DojoStoreTrait
where
    Self: Send + Sync + 'static + Sized,
{
    type Error: std::error::Error;

    async fn save_table(&self, table: &DojoTable) -> Result<(), Self::Error>;
    async fn load_tables(&self) -> Result<Vec<DojoTable>, Self::Error>;
    async fn load_table_map<K: From<(Option<Felt>, Felt)> + Eq + Hash>(
        &self,
    ) -> Result<HashMap<K, DojoTableInfo>, Self::Error> {
        Ok(self
            .load_tables()
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}
