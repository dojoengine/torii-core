use crate::postgres::PostgresBackend;
use crate::sqlite::backend::SqliteBackend;
use crate::tables::Tables;
use crate::{
    DbColumn, DbDeadField, DbResult, DbTable, IntrospectInitialize, IntrospectProcessor,
    NamespaceMode,
};
use async_trait::async_trait;
use sqlx::{Postgres, Sqlite};
use torii_introspect::events::IntrospectBody;
use torii_sql::DbConnection;

pub enum RuntimeBackend<Pg, Site> {
    Postgres(Pg),
    Sqlite(Site),
}

#[async_trait]
impl<Pg: IntrospectInitialize + Send + Sync, Site: IntrospectInitialize + Send + Sync>
    IntrospectInitialize for RuntimeBackend<Pg, Site>
{
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
impl<Pg: IntrospectProcessor + Send + Sync, Site: IntrospectProcessor + Send + Sync>
    IntrospectProcessor for RuntimeBackend<Pg, Site>
{
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

impl<Conn: DbConnection<Postgres>, Site> From<PostgresBackend<Conn>>
    for RuntimeBackend<PostgresBackend<Conn>, Site>
{
    fn from(pg: PostgresBackend<Conn>) -> Self {
        RuntimeBackend::Postgres(pg)
    }
}

impl<Conn: DbConnection<Sqlite>, Pg> From<SqliteBackend<Conn>>
    for RuntimeBackend<Pg, SqliteBackend<Conn>>
{
    fn from(site: SqliteBackend<Conn>) -> Self {
        RuntimeBackend::Sqlite(site)
    }
}
