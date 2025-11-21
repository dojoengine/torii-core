use introspect_types::{Attribute, ColumnDef, PrimaryDef};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableClass {
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

pub trait SchemaFetcherTrait {
    async fn columns_and_attributes(&self, class_hash: &Felt) -> Option<TableClass>;
}
