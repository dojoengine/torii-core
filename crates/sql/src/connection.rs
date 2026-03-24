use crate::{query::Executable, AcquiredSchema, SqlxResult};
use async_trait::async_trait;
use sqlx::{
    migrate::{Migrate, Migrator},
    Database, Pool, Transaction,
};
use std::ops::Deref;

#[async_trait]
pub trait DbConnection<DB: Database> {
    fn pool(&self) -> &Pool<DB>;
    async fn begin(&self) -> SqlxResult<Transaction<'_, DB>> {
        Ok(self.pool().begin().await?)
    }
    async fn migrate(&self, schema: Option<&'static str>, migrator: Migrator) -> SqlxResult<()>
    where
        <DB as Database>::Connection: Migrate,
        AcquiredSchema<DB, <DB as Database>::Connection>: Migrate,
    {
        let result = match schema {
            Some(schema) => {
                let mut conn = AcquiredSchema {
                    connection: self.pool().acquire().await?.detach(),
                    schema: schema,
                };
                migrator.run_direct(&mut conn).await
            }
            None => migrator.run(self.pool()).await,
        };
        Ok(result?)
    }
    async fn execute_queries(&self, queries: impl Executable<DB> + Send) -> SqlxResult<()> {
        let mut transaction: Transaction<'_, DB> = self.begin().await?;
        queries.execute(&mut transaction).await?;
        transaction.commit().await
    }
}

#[allow(clippy::explicit_auto_deref)]
#[async_trait]
impl<DB: Database, T: Deref<Target = Pool<DB>> + Send + Sync + 'static> DbConnection<DB> for T {
    fn pool(&self) -> &Pool<DB> {
        &**self
    }
}
