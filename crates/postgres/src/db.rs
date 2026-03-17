use std::ops::Deref;

pub use async_trait::async_trait;
use sqlx::Postgres;
use sqlx::{migrate::Migrator, Executor};
pub use sqlx::{PgPool, Transaction};

use crate::{migration::SchemaMigrator, SqlxResult};

#[async_trait]
pub trait PostgresConnection {
    fn pool(&self) -> &PgPool;

    async fn begin(&self) -> SqlxResult<Transaction<'_, Postgres>> {
        Ok(self.pool().begin().await?)
    }
    async fn migrate(&self, schema: Option<&'static str>, migrator: Migrator) -> SqlxResult<()> {
        let result = match schema {
            Some(schema) => SchemaMigrator::new(schema, migrator).run(self.pool()).await,
            None => migrator.run(self.pool()).await,
        };
        Ok(result?)
    }
    async fn execute_queries(&self, queries: &[String]) -> SqlxResult<()> {
        let mut transaction = self.begin().await?;
        for query in queries {
            transaction.execute(query.as_str()).await?;
        }
        transaction.commit().await
    }
}

#[async_trait]
impl<T: Deref<Target = PgPool> + Send + Sync + 'static> PostgresConnection for T {
    fn pool(&self) -> &PgPool {
        &**self
    }
}
