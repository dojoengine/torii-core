use std::ops::Deref;

pub use async_trait::async_trait;
use sqlx::migrate::Migrator;
use sqlx::Postgres;
pub use sqlx::{PgPool, Transaction};

use crate::SqlxResult;

#[async_trait]
pub trait PostgresConnection {
    fn pool(&self) -> &PgPool;

    async fn new_transaction(&self) -> SqlxResult<Transaction<'_, Postgres>> {
        Ok(self.pool().begin().await?)
    }
    async fn migrate(&self, migrator: Migrator) -> SqlxResult<()> {
        Ok(migrator.run(self.pool()).await?)
    }
    async fn execute_queries(&self, queries: &[String]) -> SqlxResult<()> {
        let mut transaction = self.new_transaction().await?;
        for query in queries {
            sqlx::query(query).execute(&mut *transaction).await?;
        }
        transaction.commit().await
    }
}

// #[async_trait]
// impl PostgresConnection for PgPool {
//     fn pool(&self) -> &PgPool {
//         self
//     }
// }

// #[async_trait]
// impl PostgresConnection for Arc<PgPool> {
//     fn pool(&self) -> &PgPool {
//         &**self
//     }
// }

#[async_trait]
impl<T: Deref<Target = PgPool> + Send + Sync + 'static> PostgresConnection for T {
    fn pool(&self) -> &PgPool {
        &**self
    }
}
