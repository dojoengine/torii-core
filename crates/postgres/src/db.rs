use crate::migration::SchemaMigrator;
use async_trait::async_trait;
use sqlx::{migrate::Migrator, Postgres};
pub use sqlx::{PgPool, Transaction};
use std::ops::Deref;
use torii_common::sql::{Executable, SqlxResult};

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
    async fn execute_queries(&self, queries: impl Executable<Postgres> + Send) -> SqlxResult<()> {
        let mut transaction = self.begin().await?;
        queries.execute(&mut transaction).await?;
        transaction.commit().await
    }
}

#[allow(clippy::explicit_auto_deref)]
#[async_trait]
impl<T: Deref<Target = PgPool> + Send + Sync + 'static> PostgresConnection for T {
    fn pool(&self) -> &PgPool {
        &**self
    }
}
