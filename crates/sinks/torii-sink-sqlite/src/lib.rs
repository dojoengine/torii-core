//! Very simple SQLite sink implementation.
//! TO BE REWORKED properly with transactions and optimisations for speed.

use std::{str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use torii_core::{Batch, Envelope, Sink, SinkFactory, SinkRegistry};
use torii_types_erc20::TransferV1 as Erc20Transfer;
use torii_types_erc721::TransferV1 as Erc721Transfer;
use torii_types_introspect::{DeclareTableV1, UpdateRecordFieldsV1};
use tracing::field::Field;
mod types;
#[derive(Clone)]
pub struct SqliteSink {
    label: String,
    pool: SqlitePool,
}

impl SqliteSink {
    pub async fn connect(
        label: impl Into<String>,
        database_url: &str,
        max_connections: Option<u32>,
    ) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections.unwrap_or(5))
            .connect_with(options)
            .await
            .with_context(|| format!("failed to connect to sqlite database {database_url}"))?;

        let sink = Self {
            label: label.into(),
            pool,
        };
        sink.ensure_schema().await?;
        Ok(sink)
    }

    async fn ensure_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS introspect_tables (
                name TEXT PRIMARY KEY,
                fields_json TEXT NOT NULL,
                primary_key_json TEXT NOT NULL
            );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS introspect_records (
                table_name TEXT NOT NULL,
                cols_json TEXT NOT NULL,
                vals_json TEXT NOT NULL,
                PRIMARY KEY (table_name, cols_json)
            );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS erc20_transfers (
                block_number INTEGER NOT NULL,
                transaction_hash TEXT NOT NULL,
                contract TEXT NOT NULL,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                amount TEXT NOT NULL
            );
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS erc721_transfers (
                block_number INTEGER NOT NULL,
                transaction_hash TEXT NOT NULL,
                contract TEXT NOT NULL,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                token_id TEXT NOT NULL
            );
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn handle_declare(&self, env: &Envelope, event: &DeclareTableV1) -> Result<()> {
        let fields_json = serde_json::to_string(&event.fields)?;
        let pk_json = serde_json::to_string(&event.primary_key)?;
        sqlx::query(
            r#"
            INSERT INTO introspect_tables (name, fields_json, primary_key_json)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(name)
            DO UPDATE SET fields_json = excluded.fields_json,
                          primary_key_json = excluded.primary_key_json;
        "#,
        )
        .bind(&event.name)
        .bind(fields_json)
        .bind(pk_json)
        .execute(&self.pool)
        .await?;

        tracing::info!(sink = %self.label, table = %event.name, block = env.raw.block_number.unwrap_or_default(), "stored declare_table");
        Ok(())
    }

    async fn handle_set_record(&self, env: &Envelope, event: &SetRecordV1) -> Result<()> {
        let cols_json = serde_json::to_string(&event.cols)?;
        // Iterate on event.vals.
        let values: Vec<String> = vec!["test".to_string()];
        let vals_json = serde_json::to_string(&values)?;

        sqlx::query(
            r#"
            INSERT INTO introspect_records (table_name, cols_json, vals_json)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(table_name, cols_json)
            DO UPDATE SET vals_json = excluded.vals_json;
        "#,
        )
        .bind(&event.table)
        .bind(cols_json)
        .bind(vals_json)
        .execute(&self.pool)
        .await?;

        tracing::debug!(
            sink = %self.label,
            table = %event.table,
            block = env.raw.block_number.unwrap_or_default(),
            "stored set_record",
        );
        Ok(())
    }

    async fn handle_erc20(&self, env: &Envelope, event: &Erc20Transfer) -> Result<()> {
        let contract = format!("{:#066x}", event.contract);
        let from = format!("{:#066x}", event.from);
        let to = format!("{:#066x}", event.to);
        let amount = event.amount.to_string();

        sqlx::query(
            r#"
            INSERT INTO erc20_transfers (block_number, transaction_hash, contract, sender, recipient, amount)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6);
        "#,
        )
        .bind(env.raw.block_number.unwrap_or_default() as i64)
        .bind(format!("{:#066x}", env.raw.transaction_hash))
        .bind(contract)
        .bind(from)
        .bind(to)
        .bind(amount)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn handle_erc721(&self, env: &Envelope, event: &Erc721Transfer) -> Result<()> {
        let contract = format!("{:#066x}", event.contract);
        let from = format!("{:#066x}", event.from);
        let to = format!("{:#066x}", event.to);
        let token_id = format!("{:#066x}", event.token_id);

        sqlx::query(
            r#"
            INSERT INTO erc721_transfers (block_number, transaction_hash, contract, sender, recipient, token_id)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6);
        "#,
        )
        .bind(env.raw.block_number.unwrap_or_default() as i64)
        .bind(format!("{:#066x}", env.raw.transaction_hash))
        .bind(contract)
        .bind(from)
        .bind(to)
        .bind(token_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl Sink for SqliteSink {
    fn label(&self) -> &str {
        &self.label
    }

    async fn handle_batch(&self, batch: Batch) -> Result<()> {
        for env in &batch.items {
            if env.type_id == DeclareTableV1::type_id() {
                if let Some(event) = env.downcast::<DeclareTableV1>() {
                    self.handle_declare(env, event).await?;
                }
            } else if env.type_id == SetRecordV1::type_id() {
                if let Some(event) = env.downcast::<SetRecordV1>() {
                    self.handle_set_record(env, event).await?;
                }
            } else if env.type_id == Erc20Transfer::type_id() {
                if let Some(event) = env.downcast::<Erc20Transfer>() {
                    self.handle_erc20(env, event).await?;
                }
            } else if env.type_id == Erc721Transfer::type_id() {
                if let Some(event) = env.downcast::<Erc721Transfer>() {
                    self.handle_erc721(env, event).await?;
                }
            }
        }
        tracing::info!(sink = %self.label, processed = batch.items.len(), "sqlite sink processed batch");
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteSinkConfig {
    pub database_url: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub max_connections: Option<u32>,
}

pub struct SqliteSinkFactory;

#[async_trait]
impl SinkFactory for SqliteSinkFactory {
    fn kind(&self) -> &'static str {
        "sqlite"
    }

    async fn create(&self, name: &str, config: Value) -> Result<Arc<dyn Sink>> {
        let cfg: SqliteSinkConfig = serde_json::from_value(config)?;
        let label = cfg.label.clone().unwrap_or_else(|| name.to_string());
        let sink = SqliteSink::connect(label, &cfg.database_url, cfg.max_connections).await?;
        Ok(Arc::new(sink) as Arc<dyn Sink>)
    }
}

pub fn register(registry: &mut SinkRegistry) {
    registry.register_factory(Arc::new(SqliteSinkFactory));
}
