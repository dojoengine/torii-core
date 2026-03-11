pub mod db;
pub mod migration;
pub use db::PostgresConnection;
pub use sqlx::Error as SqlxError;

pub type SqlxResult<T> = std::result::Result<T, SqlxError>;
