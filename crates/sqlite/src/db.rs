use std::ops::Deref;
use std::str::FromStr;

pub use async_trait::async_trait;
use sqlx::migrate::Migrator;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::Sqlite;
pub use sqlx::{SqlitePool, Transaction};
use torii_common::sql::SqlxResult;

use crate::migration::NamespaceMigrator;

#[async_trait]
pub trait SqliteConnection {
    fn pool(&self) -> &SqlitePool;

    async fn begin(&self) -> SqlxResult<Transaction<'_, Sqlite>> {
        Ok(self.pool().begin().await?)
    }

    async fn migrate(&self, namespace: Option<&'static str>, migrator: Migrator) -> SqlxResult<()> {
        let result = match namespace {
            Some(namespace) => {
                NamespaceMigrator::new(namespace, migrator)
                    .run(self.pool())
                    .await
            }
            None => migrator.run(self.pool()).await,
        };
        Ok(result?)
    }

    async fn execute_queries(&self, queries: &[String]) -> SqlxResult<()> {
        let mut transaction = self.begin().await?;
        for query in queries {
            sqlx::query(query).execute(&mut *transaction).await?;
        }
        transaction.commit().await
    }
}

#[allow(clippy::explicit_auto_deref)]
#[async_trait]
impl<T: Deref<Target = SqlitePool> + Send + Sync + 'static> SqliteConnection for T {
    fn pool(&self) -> &SqlitePool {
        &**self
    }
}

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
