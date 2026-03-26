use crate::postgres::PostgresBackend;
use crate::sqlite::backend::SqliteBackend;
use crate::tables::Tables;
use crate::{
    DbColumn, DbDeadField, DbResult, DbTable, IntrospectInitialize, IntrospectProcessor,
    NamespaceMode,
};
use async_trait::async_trait;
use torii_introspect::events::IntrospectBody;

pub enum RuntimeBackend {
    Postgres(PostgresBackend),
    Sqlite(SqliteBackend),
}

#[async_trait]
impl IntrospectInitialize for RuntimeBackend {
    async fn initialize(&self) -> DbResult<()> {
        match self {
            RuntimeBackend::Postgres(pg) => pg.initialize().await,
            RuntimeBackend::Sqlite(site) => site.initialize().await,
        }
    }
    async fn load_tables(&self, namespaces: &Option<Vec<String>>) -> DbResult<Vec<DbTable>> {
        match self {
            RuntimeBackend::Postgres(pg) => pg.load_tables(namespaces).await,
            RuntimeBackend::Sqlite(site) => site.load_tables(namespaces).await,
        }
    }
    async fn load_columns(&self, namespaces: &Option<Vec<String>>) -> DbResult<Vec<DbColumn>> {
        match self {
            RuntimeBackend::Postgres(pg) => pg.load_columns(namespaces).await,
            RuntimeBackend::Sqlite(site) => site.load_columns(namespaces).await,
        }
    }
    async fn load_dead_fields(
        &self,
        namespaces: &Option<Vec<String>>,
    ) -> DbResult<Vec<DbDeadField>> {
        match self {
            RuntimeBackend::Postgres(pg) => pg.load_dead_fields(namespaces).await,
            RuntimeBackend::Sqlite(site) => site.load_dead_fields(namespaces).await,
        }
    }
}

#[async_trait]
impl IntrospectProcessor for RuntimeBackend {
    async fn process_msgs(
        &self,
        tables: &Tables,
        namespaces: &NamespaceMode,
        msgs: Vec<&IntrospectBody>,
    ) -> DbResult<Vec<DbResult<()>>> {
        match self {
            RuntimeBackend::Postgres(pg) => pg.process_msgs(tables, namespaces, msgs).await,
            RuntimeBackend::Sqlite(site) => site.process_msgs(tables, namespaces, msgs).await,
        }
    }
}

impl From<PostgresBackend> for RuntimeBackend {
    fn from(pg: PostgresBackend) -> Self {
        RuntimeBackend::Postgres(pg)
    }
}

impl From<SqliteBackend> for RuntimeBackend {
    fn from(site: SqliteBackend) -> Self {
        RuntimeBackend::Sqlite(site)
    }
}
