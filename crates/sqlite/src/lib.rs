pub mod db;
pub mod migration;

pub use db::{is_sqlite_memory_path, sqlite_connect_options, SqliteConnection};
pub use sqlx::Error as SqlxError;

pub type SqlxResult<T> = std::result::Result<T, SqlxError>;
