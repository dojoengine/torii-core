pub mod global;
pub mod owned;
pub mod types;
pub use types::{attribute_type, felt252_type, string_type, PgAttribute, PgFelt};

pub type SqlxResult<T> = Result<T, sqlx::Error>;
