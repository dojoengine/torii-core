pub mod create;
pub mod error;
pub mod insert;
pub mod json;
pub mod processor;
pub mod query;
pub mod sink;
pub mod table;
pub mod tables;
pub mod types;
pub mod upgrade;
pub mod utils;

pub use error::{
    PgDbError, PgDbResult, PgTableError, PgTypeError, PgTypeResult, TableResult, UpgradeError,
    UpgradeResult, UpgradeResultExt,
};
pub use processor::{IntrospectPgDb, SchemaMode};
pub use types::{
    PostgresArray, PostgresField, PostgresScalar, PostgresType, PrimaryKey, SchemaName,
};
pub use utils::{truncate, HasherExt};

pub const INTROSPECT_PG_SINK_MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
