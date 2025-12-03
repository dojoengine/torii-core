use async_trait::async_trait;
use introspect_types::{Attribute, ColumnDef, PrimaryDef};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Class hash not found: {0}")]
    ClassHashNotFound(Felt),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableClass {
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

#[async_trait]
pub trait SchemaFetcherTrait: Send + Sync {
    async fn table_class(&self, class_hash: Felt) -> Result<TableClass>
    where
        Self: Send;
}
