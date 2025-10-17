use sqlx::{Pool, Postgres};
use starknet_types_core::felt::Felt;
use std::{collections::HashMap, sync::RwLock};

struct TableSchema {
    pub id_column: String,
    pub columns: HashMap<String, String>,
}

pub struct PostgreSQLSink {
    pub label: String,
    pool: Pool<Postgres>,
    schemas: RwLock<HashMap<Felt, RwLock<TableSchema>>>,
}
