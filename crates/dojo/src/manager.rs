pub use crate::store::json::JsonStore;
use crate::store::DojoStoreTrait;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
use async_trait::async_trait;
use itertools::Itertools;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::path::PathBuf;
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
            .map_err(DojoToriiError::store_error)?
            .into_iter()
            .map(|table| (table.id, table))
            .collect();
        Ok(Self { tables, store })
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

    async fn save_table(&self, owner: &Felt, table: &DojoTable) -> DojoToriiResult<()> {
        self.store
            .save_table(owner, table)
            .await
            .map_err(DojoToriiError::store_error)
    }

    async fn load_tables(&self, owners: &[Felt]) -> DojoToriiResult<Vec<DojoTable>> {
        self.store
            .load_tables(owners)
            .await
            .map_err(DojoToriiError::store_error)
    }
}

pub struct PostgresStore {
    pool: PgPool,
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

        Ok(())
    }
}

#[async_trait]
impl DojoStoreTrait for PostgresStore {
    type Error = sqlx::Error;

    async fn save_table(&self, owner: &Felt, table: &DojoTable) -> Result<(), Self::Error> {
        let json =
            serde_json::to_string(table).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
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
        .execute(&self.pool)
        .await?;
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
