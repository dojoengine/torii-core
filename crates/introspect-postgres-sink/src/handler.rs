use crate::query::{fetch_columns, fetch_dead_fields, fetch_tables};
use crate::PostgresBackend;
use async_trait::async_trait;
use introspect_types::ResultInto;
use torii_introspect_sql_sink::backend::IntrospectInitialize;
use torii_introspect_sql_sink::{DbColumn, DbDeadField, DbResult, DbTable};
use torii_sql::{DbConnection, Postgres};

pub const INTROSPECT_PG_SINK_MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[async_trait]
impl<T: DbConnection<Postgres> + Send + Sync> IntrospectInitialize for PostgresBackend<T> {
    async fn load_tables(&self, schemas: &Option<Vec<String>>) -> DbResult<Vec<DbTable>> {
        fetch_tables(self.pool(), schemas).await.err_into()
    }
    async fn load_columns(&self, schemas: &Option<Vec<String>>) -> DbResult<Vec<DbColumn>> {
        fetch_columns(self.pool(), schemas).await.err_into()
    }
    async fn load_dead_fields(&self, schemas: &Option<Vec<String>>) -> DbResult<Vec<DbDeadField>> {
        fetch_dead_fields(self.pool(), schemas).await.err_into()
    }
    async fn initialize(&self) -> DbResult<()> {
        self.migrate(Some("introspect"), INTROSPECT_PG_SINK_MIGRATIONS)
            .await
            .err_into()
    }
}
