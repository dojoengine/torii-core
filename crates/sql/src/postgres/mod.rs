pub mod migrate;
pub mod types;

pub use sqlx::postgres::PgArguments;
pub use sqlx::{PgPool, Postgres};

pub type PgQuery = crate::FlexQuery<Postgres>;

pub trait PgDbConnection: crate::DbPool<Postgres> {}
impl<T: PgDbConnection> crate::PgDbConnection for T {}
