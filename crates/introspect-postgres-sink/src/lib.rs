pub mod create;
pub mod error;
pub mod json;
pub mod processor;
pub mod query;
pub mod sink;
pub mod sqlite;
pub mod table;
pub mod types;
pub mod upgrade;
pub mod utils;

pub use error::{
    PgDbError, PgDbResult, PgTableError, PgTypeError, PgTypeResult, TableResult, UpgradeError,
    UpgradeResult, UpgradeResultExt,
};
pub use processor::IntrospectPgDb;
pub use types::{
    PgSchema, PostgresArray, PostgresField, PostgresScalar, PostgresType, PrimaryKey, SchemaName,
};
pub use utils::{truncate, HasherExt};

pub const INTROSPECT_PG_SINK_MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
