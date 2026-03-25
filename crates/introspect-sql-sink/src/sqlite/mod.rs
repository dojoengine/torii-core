pub mod backend;
pub mod json;
pub mod query;
use sqlx::migrate::Migrator;

pub const INTROSPECT_SQLITE_SINK_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");
