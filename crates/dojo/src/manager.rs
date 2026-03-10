pub use crate::store::json::JsonStore;
use crate::store::postgres::initialize_dojo_schema;
use crate::store::DojoStoreTrait;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
use async_trait::async_trait;
use itertools::Itertools;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Sqlite, SqlitePool};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use thiserror::Error;
use torii_introspect::events::CreateTable;

pub struct DojoTableStore<Store> {
    pub tables: HashMap<Felt, DojoTable>,
    pub store: Store,
}

impl<Store> DojoTableStore<Store>
where
    Store: DojoStoreTrait + Send + Sync,
    Store::Error: ToString,
{
    pub async fn with_store(store: Store) -> DojoToriiResult<Self> {
        let tables = store
            .load_tables(&[])
            .await
            .map_err(DojoToriiError::store_error)?;
        Ok(Self::from_loaded_tables(store, tables))
    }

    pub fn from_loaded_tables(store: Store, tables: Vec<DojoTable>) -> Self {
        Self {
            tables: tables.into_iter().map(|table| (table.id, table)).collect(),
            store,
        }
    }

    pub fn create_table_messages(&self) -> DojoToriiResult<Vec<CreateTable>> {
        Ok(self
            .tables
            .values()
            .map(|table| CreateTable::from(table.clone().to_schema()))
            .collect_vec())
    }
}

#[async_trait]
impl<Store> DojoStoreTrait for DojoTableStore<Store>
where
    Store: DojoStoreTrait + Send + Sync,
    Store::Error: ToString,
{
    type Error = DojoToriiError;

    async fn save_table_at_block(
        &self,
        owner: &Felt,
        table: &DojoTable,
        block_number: Option<u64>,
    ) -> DojoToriiResult<()> {
        self.store
            .save_table_at_block(owner, table, block_number)
            .await
            .map_err(DojoToriiError::store_error)
    }

    async fn load_tables(&self, owners: &[Felt]) -> DojoToriiResult<Vec<DojoTable>> {
        self.store
            .load_tables(owners)
            .await
            .map_err(DojoToriiError::store_error)
    }

    async fn load_tables_at_blocks(
        &self,
        owner_blocks: &[(Felt, u64)],
    ) -> DojoToriiResult<Vec<DojoTable>> {
        self.store
            .load_tables_at_blocks(owner_blocks)
            .await
            .map_err(DojoToriiError::store_error)
    }
}

pub struct PostgresStore {
    pool: PgPool,
}

pub struct SqliteStore {
    pool: SqlitePool,
}

#[derive(Debug, Clone)]
pub struct SchemaBootstrapPoint {
    pub owner: Felt,
    pub block_number: u64,
    pub had_saved_state: bool,
}

#[derive(Debug, Clone)]
pub struct HistoricalBootstrapDiagnostics {
    pub tables: Vec<DojoTable>,
    pub latest_table_count: usize,
    pub historical_table_count: usize,
    pub unsafe_owners: Vec<Felt>,
}

impl PostgresStore {
    pub async fn new(database_url: &str) -> DojoToriiResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(DojoToriiError::store_error)?;

        let store = Self { pool };
        store
            .initialize()
            .await
            .map_err(DojoToriiError::store_error)?;

        Ok(store)
    }

    async fn initialize(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS torii_dojo_manager_state (
                owner BYTEA NOT NULL,
                table_id BYTEA NOT NULL,
                table_json TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (owner, table_id)
            )
            ",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS torii_dojo_manager_state_history (
                owner BYTEA NOT NULL,
                table_id BYTEA NOT NULL,
                applied_at_block BIGINT NOT NULL,
                table_json TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (owner, table_id, applied_at_block)
            )
            ",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn load_historical_bootstrap(
        &self,
        bootstrap_points: &[SchemaBootstrapPoint],
    ) -> Result<HistoricalBootstrapDiagnostics, sqlx::Error> {
        let latest_tables = self
            .load_tables(
                &bootstrap_points
                    .iter()
                    .map(|point| point.owner)
                    .collect::<Vec<_>>(),
            )
            .await?;
        let historical_tables = self
            .load_tables_at_blocks(
                &bootstrap_points
                    .iter()
                    .map(|point| (point.owner, point.block_number))
                    .collect::<Vec<_>>(),
            )
            .await?;

        let latest_counts = self
            .latest_table_counts(
                &bootstrap_points
                    .iter()
                    .map(|point| point.owner)
                    .collect::<Vec<_>>(),
            )
            .await?;
        let history_presence = self
            .history_presence(
                &bootstrap_points
                    .iter()
                    .map(|point| point.owner)
                    .collect::<Vec<_>>(),
            )
            .await?;

        let unsafe_owners = bootstrap_points
            .iter()
            .filter(|point| point.block_number > 0)
            .filter(|point| latest_counts.get(&point.owner).copied().unwrap_or_default() > 0)
            .filter(|point| !history_presence.get(&point.owner).copied().unwrap_or(false))
            .map(|point| point.owner)
            .collect_vec();

        Ok(HistoricalBootstrapDiagnostics {
            latest_table_count: latest_tables.len(),
            historical_table_count: historical_tables.len(),
            tables: historical_tables,
            unsafe_owners,
        })
    }

    async fn latest_table_counts(
        &self,
        owners: &[Felt],
    ) -> Result<HashMap<Felt, usize>, sqlx::Error> {
        if owners.is_empty() {
            return Ok(HashMap::new());
        }

        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT owner, COUNT(*) AS table_count FROM torii_dojo_manager_state WHERE owner IN (",
        );
        {
            let mut separated = query.separated(", ");
            for owner in owners {
                separated.push_bind(owner.to_bytes_be().to_vec());
            }
        }
        query.push(") GROUP BY owner");

        let rows = query.build().fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let owner: Vec<u8> = row.get("owner");
                let table_count: i64 = row.get("table_count");
                (Felt::from_bytes_be_slice(&owner), table_count as usize)
            })
            .collect())
    }

    async fn history_presence(&self, owners: &[Felt]) -> Result<HashMap<Felt, bool>, sqlx::Error> {
        if owners.is_empty() {
            return Ok(HashMap::new());
        }

        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT owner, TRUE AS has_history FROM torii_dojo_manager_state_history WHERE owner IN (",
        );
        {
            let mut separated = query.separated(", ");
            for owner in owners {
                separated.push_bind(owner.to_bytes_be().to_vec());
            }
        }
        query.push(") GROUP BY owner");

        let rows = query.build().fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let owner: Vec<u8> = row.get("owner");
                (Felt::from_bytes_be_slice(&owner), true)
            })
            .collect())
    }
}

impl SqliteStore {
    pub async fn new(database_url: &str) -> DojoToriiResult<Self> {
        let options = sqlite_connect_options(database_url).map_err(DojoToriiError::store_error)?;
        if let Some(parent) = sqlite_parent_dir(&options) {
            std::fs::create_dir_all(parent).map_err(DojoToriiError::store_error)?;
        }

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(DojoToriiError::store_error)?;

        let store = Self { pool };
        store
            .initialize()
            .await
            .map_err(DojoToriiError::store_error)?;

        Ok(store)
    }

    async fn initialize(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS torii_dojo_manager_state (
                owner BLOB NOT NULL,
                table_id BLOB NOT NULL,
                table_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
                PRIMARY KEY (owner, table_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS torii_dojo_manager_state_history (
                owner BLOB NOT NULL,
                table_id BLOB NOT NULL,
                applied_at_block INTEGER NOT NULL,
                table_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
                PRIMARY KEY (owner, table_id, applied_at_block)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn load_historical_bootstrap(
        &self,
        bootstrap_points: &[SchemaBootstrapPoint],
    ) -> Result<HistoricalBootstrapDiagnostics, sqlx::Error> {
        let owners = bootstrap_points
            .iter()
            .map(|point| point.owner)
            .collect::<Vec<_>>();

        let latest_tables = self.load_tables(&owners).await?;
        let historical_tables = self
            .load_tables_at_blocks(
                &bootstrap_points
                    .iter()
                    .map(|point| (point.owner, point.block_number))
                    .collect::<Vec<_>>(),
            )
            .await?;

        let latest_counts = self.latest_table_counts(&owners).await?;
        let history_presence = self.history_presence(&owners).await?;

        let unsafe_owners = bootstrap_points
            .iter()
            .filter(|point| point.block_number > 0)
            .filter(|point| latest_counts.get(&point.owner).copied().unwrap_or_default() > 0)
            .filter(|point| !history_presence.get(&point.owner).copied().unwrap_or(false))
            .map(|point| point.owner)
            .collect_vec();

        Ok(HistoricalBootstrapDiagnostics {
            latest_table_count: latest_tables.len(),
            historical_table_count: historical_tables.len(),
            tables: historical_tables,
            unsafe_owners,
        })
    }

    async fn latest_table_counts(
        &self,
        owners: &[Felt],
    ) -> Result<HashMap<Felt, usize>, sqlx::Error> {
        if owners.is_empty() {
            return Ok(HashMap::new());
        }

        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT owner, COUNT(*) AS table_count FROM torii_dojo_manager_state WHERE owner IN (",
        );
        {
            let mut separated = query.separated(", ");
            for owner in owners {
                separated.push_bind(owner.to_bytes_be().to_vec());
            }
        }
        query.push(") GROUP BY owner");

        let rows = query.build().fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let owner: Vec<u8> = row.get("owner");
                let table_count: i64 = row.get("table_count");
                (Felt::from_bytes_be_slice(&owner), table_count as usize)
            })
            .collect())
    }

    async fn history_presence(&self, owners: &[Felt]) -> Result<HashMap<Felt, bool>, sqlx::Error> {
        if owners.is_empty() {
            return Ok(HashMap::new());
        }

        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT owner, 1 AS has_history FROM torii_dojo_manager_state_history WHERE owner IN (",
        );
        {
            let mut separated = query.separated(", ");
            for owner in owners {
                separated.push_bind(owner.to_bytes_be().to_vec());
            }
        }
        query.push(") GROUP BY owner");

        let rows = query.build().fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let owner: Vec<u8> = row.get("owner");
                (Felt::from_bytes_be_slice(&owner), true)
            })
            .collect())
    }
}

#[async_trait]
impl DojoStoreTrait for PostgresStore {
    type Error = sqlx::Error;

    async fn save_table_at_block(
        &self,
        owner: &Felt,
        table: &DojoTable,
        block_number: Option<u64>,
    ) -> Result<(), Self::Error> {
        let json =
            serde_json::to_string(table).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r"
            INSERT INTO torii_dojo_manager_state (owner, table_id, table_json, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (owner, table_id)
            DO UPDATE SET
                table_json = EXCLUDED.table_json,
                updated_at = NOW()
            ",
        )
        .bind(owner.to_bytes_be().to_vec())
        .bind(table.id.to_bytes_be().to_vec())
        .bind(json)
        .execute(&mut *tx)
        .await?;

        if let Some(block_number) = block_number {
            let history_json =
                serde_json::to_string(table).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            sqlx::query(
                r"
                INSERT INTO torii_dojo_manager_state_history
                    (owner, table_id, applied_at_block, table_json, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (owner, table_id, applied_at_block)
                DO UPDATE SET
                    table_json = EXCLUDED.table_json,
                    updated_at = NOW()
                ",
            )
            .bind(owner.to_bytes_be().to_vec())
            .bind(table.id.to_bytes_be().to_vec())
            .bind(block_number as i64)
            .bind(history_json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn load_tables(&self, owners: &[Felt]) -> Result<Vec<DojoTable>, Self::Error> {
        let rows = if owners.is_empty() {
            sqlx::query(
                r"
                SELECT table_json
                FROM torii_dojo_manager_state
                ORDER BY updated_at ASC
                ",
            )
            .fetch_all(&self.pool)
            .await?
        } else {
            let placeholders = (1..=owners.len())
                .map(|index| format!("${index}"))
                .join(", ");
            let query = format!(
                "
                SELECT table_json
                FROM torii_dojo_manager_state
                WHERE owner IN ({placeholders})
                ORDER BY updated_at ASC
                "
            );
            let mut query = sqlx::query(&query);
            for owner in owners {
                query = query.bind(owner.to_bytes_be().to_vec());
            }
            query.fetch_all(&self.pool).await?
        };

        rows.into_iter()
            .map(|row| {
                let json: String = row.try_get("table_json")?;
                serde_json::from_str(&json).map_err(|e| sqlx::Error::Protocol(e.to_string()))
            })
            .collect()
    }

    async fn load_tables_at_blocks(
        &self,
        owner_blocks: &[(Felt, u64)],
    ) -> Result<Vec<DojoTable>, Self::Error> {
        if owner_blocks.is_empty() {
            return self.load_tables(&[]).await;
        }

        let mut query = QueryBuilder::<Postgres>::new("WITH requested(owner, resume_block) AS (");
        query.push_values(owner_blocks, |mut builder, (owner, block_number)| {
            builder
                .push_bind(owner.to_bytes_be().to_vec())
                .push_bind(*block_number as i64);
        });
        query.push(
            ") SELECT DISTINCT ON (requested.owner, history.table_id) history.table_json \
             FROM requested \
             JOIN torii_dojo_manager_state_history AS history \
               ON history.owner = requested.owner \
              AND history.applied_at_block <= requested.resume_block \
             ORDER BY requested.owner, history.table_id, history.applied_at_block DESC",
        );

        let rows = query.build().fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                let json: String = row.try_get("table_json")?;
                serde_json::from_str(&json).map_err(|e| sqlx::Error::Protocol(e.to_string()))
            })
            .collect()
    }
}

#[async_trait]
impl DojoStoreTrait for SqliteStore {
    type Error = sqlx::Error;

    async fn save_table_at_block(
        &self,
        owner: &Felt,
        table: &DojoTable,
        block_number: Option<u64>,
    ) -> Result<(), Self::Error> {
        let json =
            serde_json::to_string(table).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO torii_dojo_manager_state (owner, table_id, table_json, updated_at)
            VALUES (?1, ?2, ?3, unixepoch())
            ON CONFLICT (owner, table_id)
            DO UPDATE SET
                table_json = excluded.table_json,
                updated_at = unixepoch()
            "#,
        )
        .bind(owner.to_bytes_be().to_vec())
        .bind(table.id.to_bytes_be().to_vec())
        .bind(json)
        .execute(&mut *tx)
        .await?;

        if let Some(block_number) = block_number {
            let history_json =
                serde_json::to_string(table).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            sqlx::query(
                r#"
                INSERT INTO torii_dojo_manager_state_history
                    (owner, table_id, applied_at_block, table_json, updated_at)
                VALUES (?1, ?2, ?3, ?4, unixepoch())
                ON CONFLICT (owner, table_id, applied_at_block)
                DO UPDATE SET
                    table_json = excluded.table_json,
                    updated_at = unixepoch()
                "#,
            )
            .bind(owner.to_bytes_be().to_vec())
            .bind(table.id.to_bytes_be().to_vec())
            .bind(block_number as i64)
            .bind(history_json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn load_tables(&self, owners: &[Felt]) -> Result<Vec<DojoTable>, Self::Error> {
        let rows = if owners.is_empty() {
            sqlx::query(
                r#"
                SELECT table_json
                FROM torii_dojo_manager_state
                ORDER BY updated_at ASC
                "#,
            )
            .fetch_all(&self.pool)
            .await?
        } else {
            let placeholders = std::iter::repeat_n("?", owners.len()).join(", ");
            let query = format!(
                "
                SELECT table_json
                FROM torii_dojo_manager_state
                WHERE owner IN ({placeholders})
                ORDER BY updated_at ASC
                "
            );
            let mut query = sqlx::query(&query);
            for owner in owners {
                query = query.bind(owner.to_bytes_be().to_vec());
            }
            query.fetch_all(&self.pool).await?
        };

        rows.into_iter()
            .map(|row| {
                let json: String = row.try_get("table_json")?;
                serde_json::from_str(&json).map_err(|e| sqlx::Error::Protocol(e.to_string()))
            })
            .collect()
    }

    async fn load_tables_at_blocks(
        &self,
        owner_blocks: &[(Felt, u64)],
    ) -> Result<Vec<DojoTable>, Self::Error> {
        if owner_blocks.is_empty() {
            return self.load_tables(&[]).await;
        }

        let mut query = QueryBuilder::<Sqlite>::new("WITH requested(owner, resume_block) AS (");
        query.push_values(owner_blocks, |mut builder, (owner, block_number)| {
            builder
                .push_bind(owner.to_bytes_be().to_vec())
                .push_bind(*block_number as i64);
        });
        query.push(
            ") SELECT history.table_json \
             FROM torii_dojo_manager_state_history AS history \
             JOIN requested \
               ON history.owner = requested.owner \
              AND history.applied_at_block = ( \
                    SELECT MAX(h2.applied_at_block) \
                    FROM torii_dojo_manager_state_history AS h2 \
                    WHERE h2.owner = requested.owner \
                      AND h2.table_id = history.table_id \
                      AND h2.applied_at_block <= requested.resume_block \
                ) \
             ORDER BY history.updated_at ASC",
        );

        let rows = query.build().fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                let json: String = row.try_get("table_json")?;
                serde_json::from_str(&json).map_err(|e| sqlx::Error::Protocol(e.to_string()))
            })
            .collect()
    }
}

impl DojoTableStore<JsonStore> {
    pub async fn new<P: Into<PathBuf>>(path: P) -> DojoToriiResult<Self> {
        Self::with_store(JsonStore::from(path.into())).await
    }
}

impl DojoTableStore<PostgresStore> {
    pub async fn new_postgres(database_url: &str) -> DojoToriiResult<Self> {
        Self::with_store(PostgresStore::new(database_url).await?).await
    }
}

impl DojoTableStore<SqliteStore> {
    pub async fn new_sqlite(database_url: &str) -> DojoToriiResult<Self> {
        Self::with_store(SqliteStore::new(database_url).await?).await
    }
}

fn is_sqlite_memory_path(path: &str) -> bool {
    path == ":memory:"
        || path == "sqlite::memory:"
        || path == "sqlite://:memory:"
        || path.contains("mode=memory")
}

fn sqlite_connect_options(path: &str) -> Result<SqliteConnectOptions, sqlx::Error> {
    if path == ":memory:" || path == "sqlite::memory:" {
        return SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|err| sqlx::Error::Configuration(Box::new(err)));
    }

    let options = if path.starts_with("sqlite:") {
        SqliteConnectOptions::from_str(path)
            .map_err(|err| sqlx::Error::Configuration(Box::new(err)))?
    } else {
        SqliteConnectOptions::new().filename(path)
    };

    if path.starts_with("sqlite:") && path.contains("mode=") {
        Ok(options)
    } else {
        Ok(options.create_if_missing(true))
    }
}

fn sqlite_parent_dir(options: &SqliteConnectOptions) -> Option<PathBuf> {
    let filename = options.get_filename();
    if is_sqlite_memory_path(filename.to_string_lossy().as_ref()) {
        return None;
    }
    filename
        .parent()
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

#[derive(Debug, Error)]
pub enum MergedStoreError {
    #[error("failed to load primary metadata source: {0}")]
    PrimaryLoad(String),
    #[error("failed to load secondary metadata source: {0}")]
    SecondaryLoad(String),
    #[error("failed to load primary metadata source: {primary}; failed to load secondary metadata source: {secondary}")]
    BothLoad { primary: String, secondary: String },
    #[error("failed to save primary metadata source: {0}")]
    PrimarySave(String),
    #[error("failed to save secondary metadata source: {0}")]
    SecondarySave(String),
    #[error("failed to save primary metadata source: {primary}; failed to save secondary metadata source: {secondary}")]
    BothSave { primary: String, secondary: String },
}

pub struct MergedStore<Primary, Secondary> {
    primary: Primary,
    secondary: Secondary,
}

pub struct MergedTableDrift {
    pub table_id: Felt,
    pub primary_name: String,
    pub secondary_name: String,
}

pub struct MergedLoadResult {
    pub tables: Vec<DojoTable>,
    pub primary_count: usize,
    pub secondary_count: usize,
    pub merged_count: usize,
    pub primary_only: usize,
    pub secondary_only: usize,
    pub overlapping: usize,
    pub drifted: Vec<MergedTableDrift>,
}

impl<Primary, Secondary> MergedStore<Primary, Secondary> {
    pub fn new(primary: Primary, secondary: Secondary) -> Self {
        Self { primary, secondary }
    }
}

impl<Primary, Secondary> MergedStore<Primary, Secondary>
where
    Primary: DojoStoreTrait + Send + Sync,
    Secondary: DojoStoreTrait + Send + Sync,
    Primary::Error: ToString,
    Secondary::Error: ToString,
{
    async fn load_tables_diagnostics(
        &self,
        owners: &[Felt],
    ) -> Result<MergedLoadResult, MergedStoreError> {
        let primary = self
            .primary
            .load_tables(owners)
            .await
            .map_err(|err| err.to_string());
        let secondary = self
            .secondary
            .load_tables(owners)
            .await
            .map_err(|err| err.to_string());

        match (primary, secondary) {
            (Ok(primary_tables), Ok(secondary_tables)) => {
                Ok(merge_tables(primary_tables, secondary_tables))
            }
            (Ok(primary_tables), Err(err)) => {
                tracing::warn!(
                    target: "torii::dojo::metadata",
                    error = %err,
                    "Secondary Dojo metadata source unavailable; using primary source only"
                );
                Ok(merge_tables(primary_tables, Vec::new()))
            }
            (Err(err), Ok(secondary_tables)) => {
                tracing::warn!(
                    target: "torii::dojo::metadata",
                    error = %err,
                    "Primary Dojo metadata source unavailable; using secondary source only"
                );
                Ok(merge_tables(Vec::new(), secondary_tables))
            }
            (Err(primary), Err(secondary)) => {
                Err(MergedStoreError::BothLoad { primary, secondary })
            }
        }
    }
}

fn tables_differ(primary: &DojoTable, secondary: &DojoTable) -> bool {
    match (
        serde_json::to_string(primary),
        serde_json::to_string(secondary),
    ) {
        (Ok(primary), Ok(secondary)) => primary != secondary,
        _ => primary.name != secondary.name || primary.columns.len() != secondary.columns.len(),
    }
}

fn merge_tables(
    primary_tables: Vec<DojoTable>,
    secondary_tables: Vec<DojoTable>,
) -> MergedLoadResult {
    let primary_count = primary_tables.len();
    let secondary_count = secondary_tables.len();
    let mut secondary_by_id = secondary_tables
        .into_iter()
        .map(|table| (table.id, table))
        .collect::<HashMap<_, _>>();
    let mut merged_by_id = HashMap::new();
    let mut primary_only = 0usize;
    let mut overlapping = 0usize;
    let mut drifted = Vec::new();

    for table in primary_tables {
        if let Some(secondary) = secondary_by_id.remove(&table.id) {
            overlapping += 1;
            if tables_differ(&table, &secondary) {
                drifted.push(MergedTableDrift {
                    table_id: table.id,
                    primary_name: table.name.clone(),
                    secondary_name: secondary.name,
                });
            }
        } else {
            primary_only += 1;
        }
        merged_by_id.insert(table.id, table);
    }

    let secondary_only = secondary_by_id.len();
    merged_by_id.extend(secondary_by_id);

    MergedLoadResult {
        merged_count: merged_by_id.len(),
        tables: merged_by_id.into_values().collect(),
        primary_count,
        secondary_count,
        primary_only,
        secondary_only,
        overlapping,
        drifted,
    }
}

#[async_trait]
impl<Primary, Secondary> DojoStoreTrait for MergedStore<Primary, Secondary>
where
    Primary: DojoStoreTrait + Send + Sync,
    Secondary: DojoStoreTrait + Send + Sync,
    Primary::Error: ToString,
    Secondary::Error: ToString,
{
    type Error = MergedStoreError;

    async fn save_table_at_block(
        &self,
        owner: &Felt,
        table: &DojoTable,
        block_number: Option<u64>,
    ) -> Result<(), Self::Error> {
        let primary = self
            .primary
            .save_table_at_block(owner, table, block_number)
            .await
            .map_err(|err| err.to_string());
        let secondary = self
            .secondary
            .save_table_at_block(owner, table, block_number)
            .await
            .map_err(|err| err.to_string());

        match (primary, secondary) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), Ok(())) => Err(MergedStoreError::PrimarySave(err)),
            (Ok(()), Err(err)) => Err(MergedStoreError::SecondarySave(err)),
            (Err(primary), Err(secondary)) => {
                Err(MergedStoreError::BothSave { primary, secondary })
            }
        }
    }

    async fn load_tables(&self, owners: &[Felt]) -> Result<Vec<DojoTable>, Self::Error> {
        self.load_tables_diagnostics(owners)
            .await
            .map(|result| result.tables)
    }

    async fn load_tables_at_blocks(
        &self,
        owner_blocks: &[(Felt, u64)],
    ) -> Result<Vec<DojoTable>, Self::Error> {
        self.primary
            .load_tables_at_blocks(owner_blocks)
            .await
            .map_err(|err| MergedStoreError::PrimaryLoad(err.to_string()))
    }
}

impl DojoTableStore<MergedStore<PostgresStore, PgPool>> {
    pub async fn new_merged_postgres(database_url: &str) -> DojoToriiResult<Self> {
        let secondary = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(DojoToriiError::store_error)?;
        initialize_dojo_schema(&secondary)
            .await
            .map_err(DojoToriiError::store_error)?;
        let primary = PostgresStore::new(database_url).await?;
        let store = MergedStore::new(primary, secondary);
        let merged = store
            .load_tables_diagnostics(&[])
            .await
            .map_err(DojoToriiError::store_error)?;

        tracing::info!(
            target: "torii::dojo::metadata",
            manager_state_tables = merged.primary_count,
            dojo_schema_tables = merged.secondary_count,
            merged_tables = merged.merged_count,
            manager_only = merged.primary_only,
            dojo_only = merged.secondary_only,
            overlapping = merged.overlapping,
            drifted = merged.drifted.len(),
            "Merged Dojo metadata sources for decoder bootstrap"
        );
        for drift in &merged.drifted {
            tracing::warn!(
                target: "torii::dojo::metadata",
                table_id = %drift.table_id,
                manager_name = %drift.primary_name,
                dojo_name = %drift.secondary_name,
                "Dojo metadata sources disagree; preferring persisted manager state"
            );
        }

        Ok(Self::from_loaded_tables(store, merged.tables))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::DojoStoreTrait;
    use crate::table::sort_columns;
    use introspect_types::{Attribute, ColumnDef, PrimaryDef, PrimaryTypeDef, TypeDef};
    use std::sync::Mutex;

    #[derive(Debug, Error)]
    #[error("{0}")]
    struct FakeStoreError(String);

    #[derive(Default)]
    struct FakeStore {
        tables: Vec<DojoTable>,
        saved: Mutex<Vec<Felt>>,
        fail_load: Option<String>,
        fail_save: Option<String>,
    }

    impl FakeStore {
        fn with_tables(tables: Vec<DojoTable>) -> Self {
            Self {
                tables,
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl DojoStoreTrait for FakeStore {
        type Error = FakeStoreError;

        async fn save_table_at_block(
            &self,
            _owner: &Felt,
            table: &DojoTable,
            _block_number: Option<u64>,
        ) -> Result<(), Self::Error> {
            if let Some(err) = &self.fail_save {
                return Err(FakeStoreError(err.clone()));
            }
            self.saved.lock().unwrap().push(table.id);
            Ok(())
        }

        async fn load_tables(&self, _owners: &[Felt]) -> Result<Vec<DojoTable>, Self::Error> {
            if let Some(err) = &self.fail_load {
                return Err(FakeStoreError(err.clone()));
            }
            Ok(self.tables.clone())
        }
    }

    fn table(id: u64, name: &str, columns: &[u64]) -> DojoTable {
        let columns = columns
            .iter()
            .enumerate()
            .map(|(idx, column_id)| ColumnDef {
                id: Felt::from(*column_id),
                name: format!("field_{idx}"),
                attributes: if idx == 0 {
                    vec![Attribute::new_empty("key".to_string())]
                } else {
                    vec![]
                },
                type_def: TypeDef::U32,
            })
            .collect::<Vec<_>>();
        let (columns, key_fields, value_fields) = sort_columns(columns);
        DojoTable {
            id: Felt::from(id),
            name: name.to_string(),
            attributes: vec![],
            primary: PrimaryDef {
                name: "entity_id".to_string(),
                attributes: vec![],
                type_def: PrimaryTypeDef::Felt252,
            },
            columns,
            key_fields,
            value_fields,
            legacy: false,
        }
    }

    #[tokio::test]
    async fn merged_store_prefers_primary_tables_and_keeps_secondary_only_ids() {
        let primary = FakeStore::with_tables(vec![table(1, "dojo-position", &[10, 11])]);
        let secondary = FakeStore::with_tables(vec![
            table(1, "manager-position", &[10, 12]),
            table(2, "manager-score", &[20, 21]),
        ]);
        let store = MergedStore::new(primary, secondary);

        let merged = store.load_tables_diagnostics(&[]).await.unwrap();
        let table_names = merged
            .tables
            .iter()
            .map(|table| (table.id, table.name.clone()))
            .collect::<HashMap<_, _>>();

        assert_eq!(merged.primary_count, 1);
        assert_eq!(merged.secondary_count, 2);
        assert_eq!(merged.primary_only, 0);
        assert_eq!(merged.secondary_only, 1);
        assert_eq!(merged.overlapping, 1);
        assert_eq!(merged.merged_count, 2);
        assert_eq!(merged.drifted.len(), 1);
        assert_eq!(
            table_names.get(&Felt::from(1_u64)).unwrap(),
            "dojo-position"
        );
        assert_eq!(
            table_names.get(&Felt::from(2_u64)).unwrap(),
            "manager-score"
        );
    }

    #[tokio::test]
    async fn merged_store_save_writes_to_both_sources() {
        let primary = FakeStore::default();
        let secondary = FakeStore::default();
        let store = MergedStore::new(primary, secondary);
        let table = table(3, "dojo-health", &[30, 31]);

        store.save_table(&Felt::ONE, &table).await.unwrap();

        assert_eq!(store.primary.saved.lock().unwrap().as_slice(), &[table.id]);
        assert_eq!(
            store.secondary.saved.lock().unwrap().as_slice(),
            &[table.id]
        );
    }

    #[tokio::test]
    async fn merged_store_falls_back_to_secondary_when_primary_load_fails() {
        let primary = FakeStore {
            fail_load: Some("primary unavailable".to_string()),
            ..FakeStore::default()
        };
        let secondary = FakeStore::with_tables(vec![table(4, "manager-fallback", &[40, 41])]);
        let store = MergedStore::new(primary, secondary);

        let merged = store.load_tables_diagnostics(&[]).await.unwrap();

        assert_eq!(merged.primary_count, 0);
        assert_eq!(merged.secondary_count, 1);
        assert_eq!(merged.merged_count, 1);
        assert_eq!(merged.tables[0].name, "manager-fallback");
    }
}
