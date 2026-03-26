pub mod migrate;
pub mod types;

pub use sqlx::sqlite::SqliteArguments;
pub use sqlx::{Sqlite, SqlitePool};

use sqlx::sqlite::SqliteConnectOptions;
use std::str::FromStr;

pub type SqliteQuery = super::FlexQuery<Sqlite>;

pub trait SqliteDbConnection: super::DbPool<Sqlite> {}
impl<T: super::DbPool<Sqlite>> SqliteDbConnection for T {}

pub fn is_sqlite_memory_path(path: &str) -> bool {
    path == ":memory:"
        || path == "sqlite::memory:"
        || path == "sqlite://:memory:"
        || path.contains("mode=memory")
}

pub fn sqlite_connect_options(path: &str) -> Result<SqliteConnectOptions, sqlx::Error> {
    if path == ":memory:" || path == "sqlite::memory:" {
        return SqliteConnectOptions::from_str("sqlite::memory:");
    }

    let options = if path.starts_with("sqlite:") {
        SqliteConnectOptions::from_str(path)?
    } else {
        SqliteConnectOptions::new().filename(path)
    };

    if path.starts_with("sqlite:") && path.contains("mode=") {
        Ok(options)
    } else {
        Ok(options.create_if_missing(true))
    }
}
