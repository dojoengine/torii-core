use crate::connection::DbType;
use crate::{DbPoolOptions, PoolConfig, SqlxError, SqlxResult};
use sqlx::{Pool, Postgres, Sqlite};

#[derive(Clone)]
pub enum DbPool {
    Postgres(Pool<Postgres>),
    Sqlite(Pool<Sqlite>),
}

impl From<&str> for DbPool {
    fn from(value: &str) -> Self {
        match DbType::try_from(value) {
            Ok(DbType::Postgres) => DbPool::Postgres(
                Pool::<Postgres>::connect_lazy(value).expect("Failed to create Postgres pool"),
            ),
            Ok(DbType::Sqlite) => DbPool::Sqlite(
                Pool::<Sqlite>::connect_lazy(value).expect("Failed to create Sqlite pool"),
            ),
            Err(err) => panic!("Error parsing database connection string: {}", err),
        }
    }
}

impl PoolConfig {
    pub async fn connect_any(&self) -> SqlxResult<DbPool> {
        match DbType::try_from(self.url.as_str()) {
            Ok(DbType::Postgres) => self.connect::<Postgres>().await.map(DbPool::Postgres),
            Ok(DbType::Sqlite) => self.connect::<Sqlite>().await.map(DbPool::Sqlite),
            Err(err) => Err(SqlxError::Configuration(err.into())),
        }
    }
}

impl DbPoolOptions {
    pub async fn connect_any(&self, url: &str) -> SqlxResult<DbPool> {
        match DbType::try_from(url) {
            Ok(DbType::Postgres) => self.connect::<Postgres>(url).await.map(DbPool::Postgres),
            Ok(DbType::Sqlite) => self.connect::<Sqlite>(url).await.map(DbPool::Sqlite),
            Err(err) => Err(SqlxError::Configuration(err.into())),
        }
    }
}
