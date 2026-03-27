pub mod backend;
pub mod json;
pub mod query;
pub mod record;
pub mod types;

use sqlx::migrate::Migrator;

pub use backend::{IntrospectSqliteDb, SqliteBackend};

pub const INTROSPECT_SQLITE_SINK_MIGRATIONS: Migrator = sqlx::migrate!("./migrations/sqlite");
