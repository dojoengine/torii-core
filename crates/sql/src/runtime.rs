use crate::connection::DbType;
use crate::{DbPoolOptions, PoolConfig, SqlxError, SqlxResult};
use sqlx::{Pool, Postgres, Sqlite};

#[derive(Clone)]
pub enum DbPool {
    Postgres(Pool<Postgres>),
    Sqlite(Pool<Sqlite>),
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
