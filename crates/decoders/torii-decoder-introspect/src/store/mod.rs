use crate::manager::Table;
use introspect_types::TypeDef;
use starknet_types_core::felt::Felt;

// mod json;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Store IO Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Internal Error: {0}")]
    Internal(String),
    #[error("Table not found for id: {0}")]
    TableNotFound(Felt),
    #[error("Type not found for id: {0}")]
    TypeNotFound(Felt),
    #[error("Group not found for id: {0}")]
    GroupNotFound(Felt),
}
pub type Result<T> = std::result::Result<T, Error>;

pub trait StoreTrait {
    fn dump_table(&self, table: &Table) -> Result<()>;

    fn load_table(&self, id: Felt) -> Result<Table>;
    fn load_all_tables(&self) -> Result<Vec<Table>>;

    fn remove_table(&self, id: Felt) -> Result<()>;

    fn dump_type(&self, id: Felt, type_def: &TypeDef) -> Result<()>;
    fn dump_types(&self, types: &[(Felt, TypeDef)]) -> Result<()>;

    fn load_type(&self, id: Felt) -> Result<TypeDef>;
    fn load_types(&self, ids: &[Felt]) -> Result<Vec<TypeDef>>;
    fn load_all_types(&self) -> Result<Vec<(Felt, TypeDef)>>;

    fn dump_group(&self, id: Felt, columns: &[Felt]) -> Result<()>;
    fn dump_groups(&self, groups: &[(Felt, Vec<Felt>)]) -> Result<()>;

    fn load_group(&self, id: Felt) -> Result<Vec<Felt>>;
    fn load_groups(&self, ids: &[Felt]) -> Result<Vec<Vec<Felt>>>;
    fn load_all_groups(&self) -> Result<Vec<(Felt, Vec<Felt>)>>;
}
