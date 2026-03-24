pub mod connection;
pub mod migrate;
pub mod query;

pub use connection::DbConnection;
pub use migrate::{AcquiredSchema, SchemaMigrator};
pub use query::{Executable, FlexQuery, Queries};

pub use sqlx::Error as SqlxError;
pub type SqlxResult<T> = std::result::Result<T, SqlxError>;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "sqlite")]
pub mod sqlite;

// #[cfg(feature = "mysql")]
// pub mod mysql;
