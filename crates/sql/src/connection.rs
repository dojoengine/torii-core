use crate::query::Executable;
use crate::{AcquiredSchema, SqlxResult};
use async_trait::async_trait;
use sqlx::migrate::{Migrate, Migrator};
use sqlx::{Database, Pool, Transaction};

#[async_trait]
pub trait DbPool<DB: Database> {
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
                    schema,
                };
                migrator.run_direct(&mut conn).await
            }
            None => migrator.run(self.pool()).await,
        };
        Ok(result?)
    }
    async fn execute_queries<E: Executable<DB> + Send>(&self, queries: E) -> SqlxResult<()> {
        let mut transaction: Transaction<'_, DB> = self.begin().await?;
        queries.execute(&mut transaction).await?;
        transaction.commit().await
    }
}

#[async_trait]
impl<DB: Database> DbPool<DB> for Pool<DB> {
    fn pool(&self) -> &Pool<DB> {
        self
    }
}

pub enum DbConn {
    Postgres,
    Sqlite,
}
