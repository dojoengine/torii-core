pub mod json;
pub mod processor;
pub mod sink;
pub mod table;

use sqlx::migrate::Migrator;

pub const INTROSPECT_SQLITE_SINK_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");
