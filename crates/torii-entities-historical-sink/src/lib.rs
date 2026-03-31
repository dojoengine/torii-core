use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use sqlx::any::AnyPoolOptions;
use sqlx::{Any, Pool, Row};
use starknet::core::types::Felt;
use tokio::sync::RwLock;
use torii::axum::Router;
use torii::etl::envelope::{Envelope, TypeId};
use torii::etl::extractor::ExtractionBatch;
use torii::etl::sink::{EventBus, Sink, SinkContext, TopicInfo};
use torii_introspect::events::{CreateTable, IntrospectBody, IntrospectMsg, UpdateTable};
use torii_introspect::schema::TableSchema;
use torii_sql::connection::DbOption;
use torii_sql::DbBackend;

const INTROSPECT_TYPE: TypeId = TypeId::new("introspect");
const SQLITE_FETCH_SCHEMA_STATE_QUERY: &str =
    "SELECT table_schema_json FROM introspect_sink_schema_state WHERE alive != 0";

#[derive(Clone, Debug, Default)]
pub enum HistoricalNamespace {
    #[default]
    Default,
    Custom(String),
}

impl HistoricalNamespace {
    fn sqlite_prefix(&self) -> &str {
        match self {
            Self::Default => "",
            Self::Custom(prefix) => prefix,
        }
    }

    fn postgres_schema(&self) -> &str {
        match self {
            Self::Default => "public",
            Self::Custom(schema) => schema,
        }
    }
}

impl From<()> for HistoricalNamespace {
    fn from((): ()) -> Self {
        Self::Default
    }
}

impl From<String> for HistoricalNamespace {
    fn from(value: String) -> Self {
        if value.is_empty() {
            Self::Default
        } else {
            Self::Custom(value)
        }
    }
}

impl From<&str> for HistoricalNamespace {
    fn from(value: &str) -> Self {
        if value.is_empty() {
            Self::Default
        } else {
            Self::Custom(value.to_string())
        }
    }
}

#[derive(Clone, Debug)]
struct HistoryColumn {
    name: String,
    type_sql: String,
}

#[derive(Clone, Debug)]
struct TrackedTable {
    logical_name: String,
    base_name: String,
    history_name: String,
    columns: Vec<HistoryColumn>,
    sqlite_queries: Option<TrackedTableSqliteQueries>,
}

#[derive(Clone, Debug)]
struct TrackedTableSqliteQueries {
    next_revision: String,
    copy_current_row: String,
    insert_tombstone: String,
}

#[derive(Clone)]
pub struct EntitiesHistoricalSink {
    pool: Pool<Any>,
    backend: DbBackend,
    namespace: HistoricalNamespace,
    tracked_names: HashSet<String>,
    tracked_tables: Arc<RwLock<HashMap<Felt, TrackedTable>>>,
}

impl EntitiesHistoricalSink {
    pub async fn new(
        database_url: &str,
        max_connections: Option<u32>,
        namespace: impl Into<HistoricalNamespace>,
        tracked_models: Vec<String>,
    ) -> Result<Self> {
        sqlx::any::install_default_drivers();
        let backend = DbBackend::from_str(database_url).map_err(anyhow::Error::new)?;
        let pool = AnyPoolOptions::new()
            .max_connections(max_connections.unwrap_or(DbOption::new(5, 1).value(&backend)))
            .connect(database_url)
            .await?;

        Ok(Self {
            pool,
            backend,
            namespace: namespace.into(),
            tracked_names: tracked_models
                .into_iter()
                .filter(|name| !name.is_empty())
                .collect(),
            tracked_tables: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn bootstrap(&self) -> Result<()> {
        if self.tracked_names.is_empty() {
            return Ok(());
        }

        if self.backend == DbBackend::Sqlite {
            sqlx::query("PRAGMA journal_mode=WAL")
                .execute(&self.pool)
                .await
                .ok();
        }

        match self.backend {
            DbBackend::Sqlite => {
                let rows = sqlx::query(SQLITE_FETCH_SCHEMA_STATE_QUERY)
                    .fetch_all(&self.pool)
                    .await?;
                for row in rows {
                    let schema_json: String = row.try_get("table_schema_json")?;
                    let schema: TableSchema = serde_json::from_str(&schema_json)?;
                    if self.tracked_names.contains(&schema.name) {
                        self.sync_tracked_table(schema.id, &schema.name).await?;
                    }
                }
            }
            DbBackend::Postgres => {
                let rows = sqlx::query(
                    "SELECT id, name
                     FROM introspect.db_tables
                     WHERE \"schema\" = $1",
                )
                .bind(self.namespace.postgres_schema())
                .fetch_all(&self.pool)
                .await?;
                for row in rows {
                    let table_name: String = row.try_get("name")?;
                    if !self.tracked_names.contains(&table_name) {
                        continue;
                    }
                    let table_id = felt_from_row_hex(&row, "id")?;
                    self.sync_tracked_table(table_id, &table_name).await?;
                }
            }
        }

        Ok(())
    }

    async fn resolve_tracked_table(&self, table_id: Felt) -> Result<Option<TrackedTable>> {
        let tracked = {
            let tracked_tables = self.tracked_tables.read().await;
            tracked_tables.get(&table_id).cloned()
        };

        if let Some(table) = tracked {
            return Ok(Some(table));
        }

        let resolved = self.lookup_table_name(table_id).await?;
        let Some(table_name) = resolved else {
            return Ok(None);
        };
        if !self.tracked_names.contains(&table_name) {
            return Ok(None);
        }

        let tracked = self.sync_tracked_table(table_id, &table_name).await?;
        Ok(Some(tracked))
    }

    async fn lookup_table_name(&self, table_id: Felt) -> Result<Option<String>> {
        let (canonical_table_id, compact_table_id) = felt_hex_variants(table_id);
        match self.backend {
            DbBackend::Sqlite => {
                let row = sqlx::query(
                    "SELECT table_schema_json
                     FROM introspect_sink_schema_state
                     WHERE table_id = ?1 OR table_id = ?2
                     LIMIT 1",
                )
                .bind(canonical_table_id)
                .bind(compact_table_id)
                .fetch_optional(&self.pool)
                .await?;
                row.map(|row| {
                    let schema_json: String = row.try_get("table_schema_json")?;
                    let schema: TableSchema = serde_json::from_str(&schema_json)?;
                    Ok::<_, anyhow::Error>(schema.name)
                })
                .transpose()
            }
            DbBackend::Postgres => {
                let row = sqlx::query(
                    "SELECT name
                     FROM introspect.db_tables
                     WHERE \"schema\" = $1 AND (id::text = $2 OR id::text = $3)
                     LIMIT 1",
                )
                .bind(self.namespace.postgres_schema())
                .bind(canonical_table_id)
                .bind(compact_table_id)
                .fetch_optional(&self.pool)
                .await?;
                row.map(|row| row.try_get("name").map_err(Into::into))
                    .transpose()
            }
        }
    }

    async fn sync_tracked_table(&self, table_id: Felt, logical_name: &str) -> Result<TrackedTable> {
        let base_name = match self.backend {
            DbBackend::Sqlite => sqlite_storage_name(self.namespace.sqlite_prefix(), logical_name),
            DbBackend::Postgres => logical_name.to_string(),
        };
        let history_name = match self.backend {
            DbBackend::Sqlite => sqlite_storage_name(
                self.namespace.sqlite_prefix(),
                &format!("{logical_name}_historical"),
            ),
            DbBackend::Postgres => format!("{logical_name}_historical"),
        };
        let columns = self.load_source_columns(&base_name).await?;
        if !columns.iter().any(|column| column.name == "entity_id") {
            return Err(anyhow!(
                "tracked table '{logical_name}' does not expose entity_id column"
            ));
        }
        let sqlite_queries = (self.backend == DbBackend::Sqlite)
            .then(|| build_tracked_table_sqlite_queries(&base_name, &history_name, &columns));
        let tracked = TrackedTable {
            logical_name: logical_name.to_string(),
            base_name,
            history_name,
            sqlite_queries,
            columns,
        };
        self.ensure_history_table(&tracked).await?;
        self.tracked_tables
            .write()
            .await
            .insert(table_id, tracked.clone());
        Ok(tracked)
    }

    async fn load_source_columns(&self, base_name: &str) -> Result<Vec<HistoryColumn>> {
        match self.backend {
            DbBackend::Sqlite => {
                // sqlite-dynamic-ok
                let rows = sqlx::query(&format!(
                    "PRAGMA table_info({})",
                    quote_sqlite_identifier(base_name)
                ))
                .fetch_all(&self.pool)
                .await?;
                let mut columns = Vec::with_capacity(rows.len());
                for row in rows {
                    let name: String = row.try_get("name")?;
                    let type_sql: String = row.try_get("type")?;
                    columns.push(HistoryColumn { name, type_sql });
                }
                Ok(columns)
            }
            DbBackend::Postgres => {
                let rows = sqlx::query(
                    "SELECT a.attname AS column_name,
                            pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type
                     FROM pg_catalog.pg_attribute a
                     JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = $1
                       AND c.relname = $2
                       AND a.attnum > 0
                       AND NOT a.attisdropped
                     ORDER BY a.attnum",
                )
                .bind(self.namespace.postgres_schema())
                .bind(base_name)
                .fetch_all(&self.pool)
                .await?;

                let mut columns = Vec::new();
                for row in rows {
                    let name: String = row.try_get("column_name")?;
                    if name.starts_with("__") {
                        continue;
                    }
                    let type_sql: String = row.try_get("column_type")?;
                    columns.push(HistoryColumn { name, type_sql });
                }
                Ok(columns)
            }
        }
    }

    async fn ensure_history_table(&self, tracked: &TrackedTable) -> Result<()> {
        let create_sql = match self.backend {
            DbBackend::Sqlite => self.create_history_table_sqlite(tracked),
            DbBackend::Postgres => self.create_history_table_postgres(tracked),
        };
        sqlx::query(&create_sql).execute(&self.pool).await?;

        let existing_columns = self.load_existing_history_columns(tracked).await?;
        for column in &tracked.columns {
            if existing_columns.contains(&column.name) {
                continue;
            }
            let add_sql = match self.backend {
                DbBackend::Sqlite => format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    quote_sqlite_identifier(&tracked.history_name),
                    quote_ident(&column.name),
                    column.type_sql
                ),
                DbBackend::Postgres => format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name),
                    quote_ident(&column.name),
                    column.type_sql
                ),
            };
            sqlx::query(&add_sql).execute(&self.pool).await?;
        }

        for meta_sql in self.ensure_history_meta_columns_sql(tracked, &existing_columns) {
            sqlx::query(&meta_sql).execute(&self.pool).await?;
        }

        for index_sql in self.ensure_history_indexes_sql(tracked) {
            sqlx::query(&index_sql).execute(&self.pool).await?;
        }

        Ok(())
    }

    async fn load_existing_history_columns(
        &self,
        tracked: &TrackedTable,
    ) -> Result<HashSet<String>> {
        let rows = match self.backend {
            DbBackend::Sqlite => {
                // sqlite-dynamic-ok: PRAGMA table_info requires the table identifier in SQL text.
                sqlx::query(&format!(
                    "PRAGMA table_info({})",
                    quote_sqlite_identifier(&tracked.history_name)
                ))
                .fetch_all(&self.pool)
                .await?
            }
            DbBackend::Postgres => {
                sqlx::query(
                    "SELECT a.attname AS column_name
                     FROM pg_catalog.pg_attribute a
                     JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = $1
                       AND c.relname = $2
                       AND a.attnum > 0
                       AND NOT a.attisdropped",
                )
                .bind(self.namespace.postgres_schema())
                .bind(&tracked.history_name)
                .fetch_all(&self.pool)
                .await?
            }
        };
        let mut columns = HashSet::with_capacity(rows.len());
        for row in rows {
            let name = if self.backend == DbBackend::Sqlite {
                row.try_get("name")?
            } else {
                row.try_get("column_name")?
            };
            columns.insert(name);
        }
        Ok(columns)
    }

    fn create_history_table_sqlite(&self, tracked: &TrackedTable) -> String {
        let model_columns = tracked
            .columns
            .iter()
            .map(|column| {
                if column.name == "entity_id" {
                    format!("{} {} NOT NULL", quote_ident(&column.name), column.type_sql)
                } else {
                    format!("{} {}", quote_ident(&column.name), column.type_sql)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE TABLE IF NOT EXISTS {} ({model_columns}, \
             \"revision\" INTEGER NOT NULL DEFAULT 0, \
             \"historical_deleted\" INTEGER NOT NULL DEFAULT 0, \
             \"historical_block_number\" INTEGER, \
             \"historical_tx_hash\" TEXT NOT NULL DEFAULT '', \
             \"historical_executed_at\" INTEGER NOT NULL DEFAULT 0)",
            quote_sqlite_identifier(&tracked.history_name)
        )
    }

    fn create_history_table_postgres(&self, tracked: &TrackedTable) -> String {
        let model_columns = tracked
            .columns
            .iter()
            .map(|column| {
                if column.name == "entity_id" {
                    format!("{} {} NOT NULL", quote_ident(&column.name), column.type_sql)
                } else {
                    format!("{} {}", quote_ident(&column.name), column.type_sql)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE TABLE IF NOT EXISTS {} ({model_columns}, \
             \"revision\" BIGINT NOT NULL DEFAULT 0, \
             \"historical_deleted\" BOOLEAN NOT NULL DEFAULT FALSE, \
             \"historical_block_number\" BIGINT, \
             \"historical_tx_hash\" TEXT NOT NULL DEFAULT '', \
             \"historical_executed_at\" BIGINT NOT NULL DEFAULT 0)",
            quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name)
        )
    }

    fn ensure_history_meta_columns_sql(
        &self,
        tracked: &TrackedTable,
        existing_columns: &HashSet<String>,
    ) -> Vec<String> {
        match self.backend {
            DbBackend::Sqlite => {
                let mut sql = Vec::new();
                let target = quote_sqlite_identifier(&tracked.history_name);
                if !existing_columns.contains("revision") {
                    sql.push(format!(
                        "ALTER TABLE {target} ADD COLUMN \"revision\" INTEGER NOT NULL DEFAULT 0"
                    ));
                }
                if !existing_columns.contains("historical_deleted") {
                    sql.push(format!(
                        "ALTER TABLE {target} ADD COLUMN \"historical_deleted\" INTEGER NOT NULL DEFAULT 0"
                    ));
                }
                if !existing_columns.contains("historical_block_number") {
                    sql.push(format!(
                        "ALTER TABLE {target} ADD COLUMN \"historical_block_number\" INTEGER"
                    ));
                }
                if !existing_columns.contains("historical_tx_hash") {
                    sql.push(format!(
                        "ALTER TABLE {target} ADD COLUMN \"historical_tx_hash\" TEXT NOT NULL DEFAULT ''"
                    ));
                }
                if !existing_columns.contains("historical_executed_at") {
                    sql.push(format!(
                        "ALTER TABLE {target} ADD COLUMN \"historical_executed_at\" INTEGER NOT NULL DEFAULT 0"
                    ));
                }
                sql
            }
            DbBackend::Postgres => vec![
                format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS \"revision\" BIGINT NOT NULL DEFAULT 0",
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name)
                ),
                format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS \"historical_deleted\" BOOLEAN NOT NULL DEFAULT FALSE",
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name)
                ),
                format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS \"historical_block_number\" BIGINT",
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name)
                ),
                format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS \"historical_tx_hash\" TEXT NOT NULL DEFAULT ''",
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name)
                ),
                format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS \"historical_executed_at\" BIGINT NOT NULL DEFAULT 0",
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name)
                ),
            ],
        }
    }

    fn ensure_history_indexes_sql(&self, tracked: &TrackedTable) -> Vec<String> {
        let unique_index = format!("{}_entity_revision_idx", tracked.history_name);
        let entity_index = format!("{}_entity_idx", tracked.history_name);
        match self.backend {
            DbBackend::Sqlite => vec![
                format!(
                    "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (\"entity_id\", \"revision\")",
                    quote_ident(&unique_index),
                    quote_sqlite_identifier(&tracked.history_name)
                ),
                format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {} (\"entity_id\")",
                    quote_ident(&entity_index),
                    quote_sqlite_identifier(&tracked.history_name)
                ),
            ],
            DbBackend::Postgres => vec![
                format!(
                    "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (\"entity_id\", \"revision\")",
                    quote_ident(&unique_index),
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name)
                ),
                format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {} (\"entity_id\")",
                    quote_ident(&entity_index),
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name)
                ),
            ],
        }
    }

    async fn next_revision(
        &self,
        tracked: &TrackedTable,
        canonical_entity_id_hex: &str,
        compact_entity_id_hex: &str,
    ) -> Result<i64> {
        let sql = match self.backend {
            DbBackend::Sqlite => tracked
                .sqlite_queries
                .as_ref()
                .expect("sqlite queries available for sqlite backend")
                .next_revision
                .clone(),
            DbBackend::Postgres => format!(
                "SELECT COALESCE(MAX(\"revision\"), 0) + 1 AS next_revision \
                 FROM {} WHERE \"entity_id\"::text = $1 OR \"entity_id\"::text = $2",
                quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name)
            ),
        };
        let row = sqlx::query(&sql)
            .bind(canonical_entity_id_hex)
            .bind(compact_entity_id_hex)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get::<i64, _>("next_revision")?)
    }

    async fn persist_snapshot(
        &self,
        tracked: &TrackedTable,
        entity_id: Felt,
        block_number: Option<u64>,
        tx_hash: Felt,
        executed_at: u64,
        deleted: bool,
    ) -> Result<()> {
        let (canonical_entity_id_hex, compact_entity_id_hex) = felt_hex_variants(entity_id);
        let revision = self
            .next_revision(tracked, &canonical_entity_id_hex, &compact_entity_id_hex)
            .await?;
        let copied = self
            .copy_current_row_into_history(
                tracked,
                &canonical_entity_id_hex,
                &compact_entity_id_hex,
                revision,
                block_number,
                tx_hash,
                executed_at,
                deleted,
            )
            .await?;
        if copied == 0 {
            if deleted {
                self.insert_tombstone_only(
                    tracked,
                    &canonical_entity_id_hex,
                    revision,
                    block_number,
                    tx_hash,
                    executed_at,
                )
                .await?;
                return Ok(());
            }
            return Err(anyhow!(
                "unable to load latest row for tracked model '{}' entity {}",
                tracked.logical_name,
                canonical_entity_id_hex
            ));
        }
        Ok(())
    }

    async fn copy_current_row_into_history(
        &self,
        tracked: &TrackedTable,
        canonical_entity_id_hex: &str,
        compact_entity_id_hex: &str,
        revision: i64,
        block_number: Option<u64>,
        tx_hash: Felt,
        executed_at: u64,
        deleted: bool,
    ) -> Result<u64> {
        let sql = match self.backend {
            DbBackend::Sqlite => tracked
                .sqlite_queries
                .as_ref()
                .expect("sqlite queries available for sqlite backend")
                .copy_current_row
                .clone(),
            DbBackend::Postgres => {
                let source_columns = tracked
                    .columns
                    .iter()
                    .map(|column| quote_ident(&column.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                let insert_columns = format!(
                    "{source_columns}, \"revision\", \"historical_deleted\", \
                     \"historical_block_number\", \"historical_tx_hash\", \"historical_executed_at\""
                );
                let history_target =
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name);
                let source_target =
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.base_name);
                format!(
                    "INSERT INTO {history_target} ({insert_columns}) \
                     SELECT {source_columns}, $1, $2, $3, $4, $5 \
                     FROM {source_target} WHERE (\"entity_id\"::text = $6 OR \"entity_id\"::text = $7) LIMIT 1"
                )
            }
        };
        let mut query = sqlx::query(&sql).bind(revision);
        query = match self.backend {
            DbBackend::Sqlite => query.bind(i64::from(deleted)),
            DbBackend::Postgres => query.bind(deleted),
        };
        let result = query
            .bind(block_number.map(|value| value as i64))
            .bind(felt_hex(tx_hash))
            .bind(executed_at as i64)
            .bind(canonical_entity_id_hex)
            .bind(compact_entity_id_hex)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn insert_tombstone_only(
        &self,
        tracked: &TrackedTable,
        entity_id_hex: &str,
        revision: i64,
        block_number: Option<u64>,
        tx_hash: Felt,
        executed_at: u64,
    ) -> Result<()> {
        let sql = match self.backend {
            DbBackend::Sqlite => tracked
                .sqlite_queries
                .as_ref()
                .expect("sqlite queries available for sqlite backend")
                .insert_tombstone
                .clone(),
            DbBackend::Postgres => {
                let mut columns = Vec::with_capacity(tracked.columns.len() + 5);
                let mut values_sql = Vec::with_capacity(tracked.columns.len() + 5);

                let mut bind_index = 1_usize;
                for column in &tracked.columns {
                    columns.push(quote_ident(&column.name));
                    if column.name == "entity_id" {
                        values_sql.push(self.entity_cast_placeholder(bind_index, &column.type_sql));
                        bind_index += 1;
                    } else {
                        values_sql.push("NULL".to_string());
                    }
                }

                columns.extend([
                    "\"revision\"".to_string(),
                    "\"historical_deleted\"".to_string(),
                    "\"historical_block_number\"".to_string(),
                    "\"historical_tx_hash\"".to_string(),
                    "\"historical_executed_at\"".to_string(),
                ]);
                values_sql.push(self.value_placeholder(bind_index));
                bind_index += 1;
                values_sql.push(self.value_placeholder(bind_index));
                bind_index += 1;
                values_sql.push(self.value_placeholder(bind_index));
                bind_index += 1;
                values_sql.push(self.value_placeholder(bind_index));
                bind_index += 1;
                values_sql.push(self.value_placeholder(bind_index));

                let target =
                    quote_pg_qualified(self.namespace.postgres_schema(), &tracked.history_name);
                format!(
                    "INSERT INTO {target} ({}) VALUES ({})",
                    columns.join(", "),
                    values_sql.join(", ")
                )
            }
        };

        let mut query = sqlx::query(&sql).bind(entity_id_hex).bind(revision);
        query = match self.backend {
            DbBackend::Sqlite => query.bind(1_i64),
            DbBackend::Postgres => query.bind(true),
        };
        query
            .bind(block_number.map(|value| value as i64))
            .bind(felt_hex(tx_hash))
            .bind(executed_at as i64)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    fn value_placeholder(&self, index: usize) -> String {
        match self.backend {
            DbBackend::Sqlite => "?".to_string(),
            DbBackend::Postgres => format!("${index}"),
        }
    }

    fn entity_cast_placeholder(&self, index: usize, type_sql: &str) -> String {
        match self.backend {
            DbBackend::Sqlite => "?".to_string(),
            DbBackend::Postgres => format!("CAST(${index} AS {type_sql})"),
        }
    }

    async fn process_table_schema(&self, table: &CreateTable) -> Result<()> {
        if self.tracked_names.contains(&table.name) {
            self.sync_tracked_table(table.id, &table.name).await?;
        }
        Ok(())
    }

    async fn process_table_update(&self, table: &UpdateTable) -> Result<()> {
        if self.tracked_names.contains(&table.name) {
            self.sync_tracked_table(table.id, &table.name).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for EntitiesHistoricalSink {
    fn name(&self) -> &'static str {
        "entities-historical"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![INTROSPECT_TYPE]
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        for envelope in envelopes {
            if envelope.type_id != INTROSPECT_TYPE {
                continue;
            }
            let Some(body) = envelope.downcast_ref::<IntrospectBody>() else {
                continue;
            };
            let context = batch
                .get_event_context(&body.metadata.transaction_hash, body.metadata.from_address)
                .unwrap_or_default();
            let block_number = body
                .metadata
                .block_number
                .or(Some(context.transaction.block_number));
            let executed_at = context.block.timestamp;

            match &body.msg {
                IntrospectMsg::CreateTable(table) => {
                    self.process_table_schema(table).await?;
                }
                IntrospectMsg::UpdateTable(table) => {
                    self.process_table_update(table).await?;
                }
                IntrospectMsg::InsertsFields(insert) => {
                    let Some(tracked) = self.resolve_tracked_table(insert.table).await? else {
                        continue;
                    };
                    for record in &insert.records {
                        self.persist_snapshot(
                            &tracked,
                            Felt::from_bytes_be(&record.id),
                            block_number,
                            body.metadata.transaction_hash,
                            executed_at,
                            false,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "failed to persist historical snapshot for '{}' insert",
                                tracked.logical_name
                            )
                        })?;
                    }
                }
                IntrospectMsg::DeleteRecords(delete) => {
                    let Some(tracked) = self.resolve_tracked_table(delete.table).await? else {
                        continue;
                    };
                    for row in &delete.rows {
                        self.persist_snapshot(
                            &tracked,
                            row.to_felt(),
                            block_number,
                            body.metadata.transaction_hash,
                            executed_at,
                            true,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "failed to persist historical tombstone for '{}'",
                                tracked.logical_name
                            )
                        })?;
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        Vec::new()
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }

    async fn initialize(
        &mut self,
        _event_bus: Arc<EventBus>,
        _context: &SinkContext,
    ) -> Result<()> {
        self.bootstrap().await?;
        Ok(())
    }
}

fn sqlite_storage_name(prefix: &str, logical_name: &str) -> String {
    if prefix.is_empty() {
        logical_name.to_string()
    } else {
        format!("{prefix}__{logical_name}")
    }
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_sqlite_identifier(table: &str) -> String {
    quote_ident(table)
}

fn quote_pg_qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

fn build_tracked_table_sqlite_queries(
    base_name: &str,
    history_name: &str,
    columns: &[HistoryColumn],
) -> TrackedTableSqliteQueries {
    let history_target = quote_sqlite_identifier(history_name);
    let source_target = quote_sqlite_identifier(base_name);
    let source_columns = columns
        .iter()
        .map(|column| quote_ident(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_columns = format!(
        "{source_columns}, \"revision\", \"historical_deleted\", \
         \"historical_block_number\", \"historical_tx_hash\", \"historical_executed_at\""
    );

    let mut tombstone_columns = Vec::with_capacity(columns.len() + 5);
    let mut tombstone_values = Vec::with_capacity(columns.len() + 5);
    for column in columns {
        tombstone_columns.push(quote_ident(&column.name));
        if column.name == "entity_id" {
            tombstone_values.push("?".to_string());
        } else {
            tombstone_values.push("NULL".to_string());
        }
    }
    tombstone_columns.extend([
        "\"revision\"".to_string(),
        "\"historical_deleted\"".to_string(),
        "\"historical_block_number\"".to_string(),
        "\"historical_tx_hash\"".to_string(),
        "\"historical_executed_at\"".to_string(),
    ]);
    tombstone_values.extend([
        "?".to_string(),
        "?".to_string(),
        "?".to_string(),
        "?".to_string(),
        "?".to_string(),
    ]);

    TrackedTableSqliteQueries {
        next_revision: format!(
            "SELECT COALESCE(MAX(\"revision\"), 0) + 1 AS next_revision \
             FROM {history_target} WHERE \"entity_id\" = ?1 OR \"entity_id\" = ?2"
        ),
        copy_current_row: format!(
            "INSERT INTO {history_target} ({insert_columns}) \
             SELECT {source_columns}, ?1, ?2, ?3, ?4, ?5 \
             FROM {source_target} \
             WHERE (\"entity_id\" = ?6 OR \"entity_id\" = ?7) \
             LIMIT 1"
        ),
        insert_tombstone: format!(
            "INSERT INTO {history_target} ({}) VALUES ({})",
            tombstone_columns.join(", "),
            tombstone_values.join(", ")
        ),
    }
}

fn felt_hex(value: Felt) -> String {
    format!("{value:#x}")
}

fn canonical_felt_hex(value: Felt) -> String {
    format!("{value:#066x}")
}

fn compact_hex_str(value: &str) -> String {
    let value = value.trim();
    let Some(hex) = value.strip_prefix("0x") else {
        return value.to_string();
    };
    let trimmed = hex.trim_start_matches('0');
    if trimmed.is_empty() {
        "0x0".to_string()
    } else {
        format!("0x{trimmed}")
    }
}

fn felt_hex_variants(value: Felt) -> (String, String) {
    let canonical = canonical_felt_hex(value);
    let compact = compact_hex_str(&canonical);
    (canonical, compact)
}

fn felt_from_row_hex(row: &sqlx::any::AnyRow, column: &str) -> Result<Felt> {
    let value: String = row.try_get(column)?;
    Felt::from_hex(&value).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use introspect_types::{ColumnDef, PrimaryDef, PrimaryTypeDef, TypeDef};
    use torii::etl::envelope::{EventBody, MetaData};
    use torii::etl::extractor::ExtractionBatch;
    use torii_introspect::events::{DeleteRecords, InsertsFields, IntrospectMsg, Record};

    fn sqlite_table_schema(name: &str, id: Felt) -> TableSchema {
        TableSchema {
            id,
            name: name.to_string(),
            attributes: Vec::new(),
            primary: PrimaryDef {
                name: "entity_id".to_string(),
                attributes: Vec::new(),
                type_def: PrimaryTypeDef::Felt252,
            },
            columns: vec![
                ColumnDef {
                    id: Felt::from(1_u8),
                    name: "name".to_string(),
                    attributes: Vec::new(),
                    type_def: TypeDef::ByteArray,
                },
                ColumnDef {
                    id: Felt::from(2_u8),
                    name: "score".to_string(),
                    attributes: Vec::new(),
                    type_def: TypeDef::U32,
                },
            ],
        }
    }

    async fn sqlite_sink() -> Result<EntitiesHistoricalSink> {
        let sink = EntitiesHistoricalSink::new(
            "sqlite::memory:",
            Some(1),
            (),
            vec!["NUMS-Game".to_string()],
        )
        .await?;
        sqlx::query(
            "CREATE TABLE introspect_sink_schema_state (
                table_id TEXT PRIMARY KEY,
                table_schema_json TEXT NOT NULL,
                alive INTEGER NOT NULL DEFAULT 1,
                updated_at INTEGER NOT NULL DEFAULT (unixepoch())
            )",
        )
        .execute(&sink.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE \"NUMS-Game\" (
                \"entity_id\" TEXT PRIMARY KEY,
                \"name\" TEXT,
                \"score\" INTEGER
            )",
        )
        .execute(&sink.pool)
        .await?;
        let schema = sqlite_table_schema("NUMS-Game", Felt::from(9_u8));
        sqlx::query(
            "INSERT INTO introspect_sink_schema_state (table_id, table_schema_json, alive, updated_at)
             VALUES (?1, ?2, 1, unixepoch())",
        )
        .bind(canonical_felt_hex(schema.id))
        .bind(serde_json::to_string(&schema)?)
        .execute(&sink.pool)
        .await?;
        Ok(sink)
    }

    #[tokio::test]
    async fn initializes_history_table_from_sqlite_schema_state() -> Result<()> {
        let sink = sqlite_sink().await?;
        sink.bootstrap().await?;

        let row = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'NUMS-Game_historical'",
        )
        .fetch_one(&sink.pool)
        .await?;
        let name: String = row.try_get("name")?;
        assert_eq!(name, "NUMS-Game_historical");
        Ok(())
    }

    #[tokio::test]
    async fn appends_revisions_and_delete_tombstones_in_sqlite() -> Result<()> {
        let sink = sqlite_sink().await?;
        sink.bootstrap().await?;

        let entity_id = Felt::from(77_u8);
        sqlx::query(
            "INSERT INTO \"NUMS-Game\" (\"entity_id\", \"name\", \"score\") VALUES (?1, ?2, ?3)",
        )
        .bind(canonical_felt_hex(entity_id))
        .bind("first")
        .bind(10_i64)
        .execute(&sink.pool)
        .await?;

        let insert = EventBody {
            metadata: MetaData {
                block_number: Some(1),
                transaction_hash: Felt::from(100_u16),
                from_address: Felt::ZERO,
            },
            msg: IntrospectMsg::InsertsFields(InsertsFields::new(
                Felt::from(9_u8),
                Vec::new(),
                vec![Record::new(entity_id, Vec::new())],
            )),
        };
        sink.process(&[Envelope::from(insert)], &ExtractionBatch::empty())
            .await?;

        sqlx::query("UPDATE \"NUMS-Game\" SET \"score\" = 25 WHERE \"entity_id\" = ?1")
            .bind(canonical_felt_hex(entity_id))
            .execute(&sink.pool)
            .await?;
        let update = EventBody {
            metadata: MetaData {
                block_number: Some(2),
                transaction_hash: Felt::from(101_u16),
                from_address: Felt::ZERO,
            },
            msg: IntrospectMsg::InsertsFields(InsertsFields::new(
                Felt::from(9_u8),
                Vec::new(),
                vec![Record::new(entity_id, Vec::new())],
            )),
        };
        sink.process(&[Envelope::from(update)], &ExtractionBatch::empty())
            .await?;

        let delete = EventBody {
            metadata: MetaData {
                block_number: Some(3),
                transaction_hash: Felt::from(102_u16),
                from_address: Felt::ZERO,
            },
            msg: IntrospectMsg::DeleteRecords(DeleteRecords::new(
                Felt::from(9_u8),
                vec![entity_id.into()],
            )),
        };
        sink.process(&[Envelope::from(delete)], &ExtractionBatch::empty())
            .await?;

        let rows = sqlx::query(
            "SELECT revision, historical_deleted, score FROM \"NUMS-Game_historical\" ORDER BY revision",
        )
        .fetch_all(&sink.pool)
        .await?;

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].try_get::<i64, _>("revision")?, 1);
        assert_eq!(rows[0].try_get::<i64, _>("historical_deleted")?, 0);
        assert_eq!(rows[0].try_get::<i64, _>("score")?, 10);
        assert_eq!(rows[1].try_get::<i64, _>("revision")?, 2);
        assert_eq!(rows[1].try_get::<i64, _>("score")?, 25);
        assert_eq!(rows[2].try_get::<i64, _>("revision")?, 3);
        assert_eq!(rows[2].try_get::<i64, _>("historical_deleted")?, 1);
        assert_eq!(rows[2].try_get::<i64, _>("score")?, 25);
        Ok(())
    }
}
