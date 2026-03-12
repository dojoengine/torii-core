pub mod json;
pub mod processor;
pub mod sink;
pub mod sql;
pub mod table;
pub mod types;
pub mod utils;
use sqlx::migrate::Migrator;
pub use types::{PgRustEnum, PgStructDef, PgTableStructure, PostgresField, PostgresType};
pub use utils::{truncate, HasherExt};

pub const INTROSPECT_PG_SINK_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");
