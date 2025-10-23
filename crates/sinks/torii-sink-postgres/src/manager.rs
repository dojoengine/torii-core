use crate::{PostgresComplexType, PostgresType};
use introspect_types::ColumnDef;
use std::{collections::HashMap, sync::RwLock};
use thiserror::Error;
pub struct SchemaManager(pub HashMap<String, RwLock<HashMap<String, PostgresType>>>);

struct TableSchema {
    columns: HashMap<String, PostgresType>,
    types: HashMap<String, PostgresComplexType>,
}

#[derive(Debug, Error)]
pub enum SchemaManagerError {
    #[error("Type extraction error: {0}")]
    TypeExtractionError(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
}

type Result<T> = std::result::Result<T, SchemaManagerError>;

impl SchemaManager {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn upgrade_schema(
        &mut self,
        table_name: &str,
        new_schema: Vec<ColumnDef>,
    ) -> Result<Vec<String>> {
        let mut queries: Vec<String> = Default::default();
        let old_schema = self
            .0
            .entry(table_name.to_string())
            .or_insert_with(|| RwLock::new(HashMap::new()));

        Ok(queries)
    }
}
