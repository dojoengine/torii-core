use crate::schema::TableInfo;
use async_trait::async_trait;
use introspect_types::{Attribute, ColumnDef, ColumnInfo, PrimaryTypeDef, TypeDef};
use starknet_types_core::felt::Felt;

#[async_trait]
pub trait TableStore {
    type Error;
    async fn save_table(
        &self,
        owner: &Felt,
        id: &Felt,
        table: &TableInfo,
    ) -> Result<(), Self::Error>;
    async fn load_tables(&self, owners: &[Felt]) -> Result<TableInfo, Self::Error>;
    async fn add_columns(
        &self,
        owner: &Felt,
        table: &Felt,
        columns: ColumnDef,
        order: &[Felt],
    ) -> Result<(), Self::Error>;
    async fn update_table_name(
        &self,
        owner: &Felt,
        id: &Felt,
        name: &str,
    ) -> Result<(), Self::Error>;
    async fn update_primary_name(
        &self,
        owner: &Felt,
        id: &Felt,
        name: &str,
    ) -> Result<(), Self::Error>;
    async fn update_primary_type(
        &self,
        owner: &Felt,
        id: &Felt,
        attributes: &[Attribute],
        primary: &PrimaryTypeDef,
    ) -> Result<(), Self::Error>;
    async fn update_column_name(
        &self,
        owner: &Felt,
        table: &Felt,
        column: &Felt,
        name: &str,
    ) -> Result<(), Self::Error>;
    async fn update_column_type(
        &self,
        owner: &Felt,
        table: &Felt,
        column: &Felt,
        attributes: &[Attribute],
        type_def: &TypeDef,
    ) -> Result<(), Self::Error>;
}
