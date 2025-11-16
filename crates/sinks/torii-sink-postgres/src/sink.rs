use crate::json::ToPostgresJson;
use crate::manager::TableManagerError;
use crate::value::{PostgresValueError, ToPostgresValue};
use crate::TableManager;
use async_trait::async_trait;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Postgres, Transaction};
use starknet::core::types::EmittedEvent;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use torii_core::{Batch, Event, Sink};
use torii_types_introspect::{DeclareTableV1, DeleteRecordsV1, UpdateRecordFieldsV1};

#[derive(Debug, Error)]
pub enum PostgresSinkError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
    #[error("Invalid event format: {0}")]
    InvalidEventFormat(String),
    #[error("Migration error: {0}")]
    MigrationError(#[from] sqlx::migrate::MigrateError),
    #[error("Json error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Postgres value error: {0}")]
    PostgresValueError(#[from] PostgresValueError),
    #[error("Table manager error: {0}")]
    TableManagerError(#[from] TableManagerError),
}

type Result<T> = std::result::Result<T, PostgresSinkError>;

pub struct PostgresSink {
    label: String,
    pool: PgPool,
    manager: TableManager,
}

impl PostgresSink {
    pub async fn connect(
        label: impl Into<String>,
        database_url: &str,
        max_connections: Option<u32>,
    ) -> Result<Self> {
        let options = PgConnectOptions::from_str(database_url)?;
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.unwrap_or(5))
            .connect_with(options)
            .await?;

        Ok(Self {
            label: label.into(),
            pool,
            manager: TableManager::default(),
        })
    }

    pub async fn initialize(&self) -> Result<()> {
        match sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(PostgresSinkError::from)
        {
            Ok(_) => {
                println!("Postgres sink migrations applied successfully");
                tracing::info!(sink = %self.label, "Postgres sink migrations applied successfully");
                Ok(())
            }
            Err(err) => {
                tracing::error!(error = %err, "Failed to run migrations");
                Err(err)
            }
        }
    }

    async fn handle_declare_table(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        raw: Arc<EmittedEvent>,
        event: &DeclareTableV1,
    ) -> Result<()> {
        let table_name = &event.name;
        let queries = self.manager.declare_table(table_name, event)?;
        for query in queries {
            sqlx::query(&query).execute(&mut **tx).await?;
        }

        tracing::info!(
            sink = %self.label,
            table = %event.name,
            storage_table = %table_name,
            block = %raw.block_number.unwrap_or_default(),
            "stored declare_table"
        );
        Ok(())
    }

    async fn handle_update_record(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        raw: Arc<EmittedEvent>,
        event: &UpdateRecordFieldsV1,
    ) -> Result<()> {
        let table_name = &event.table_name;
        let primary_key_name = event.primary.name.clone();
        let update_fields = event
            .fields
            .iter()
            .map(|f| {
                format!(
                    r#""{field}" = COALESCE(EXCLUDED."{field}", "{table_name}"."{field}")"#,
                    field = f.name,
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let json_data = serde_json::to_string(&event.to_postgres_json())?;
        let query = format!(
            r#"INSERT INTO "{table_name}"
            SELECT * FROM jsonb_populate_record(NULL::"{table_name}", $${json_data}$$)
            ON CONFLICT ("{primary_key_name}") DO UPDATE SET 
            {update_fields}
            ;"#,
        );
        sqlx::query(&query).execute(&mut **tx).await?;
        tracing::info!(
            sink = %self.label,
            table = %event.table_name,
            storage_table = %table_name,
            block = %raw.block_number.unwrap_or_default(),
            "stored update_record"
        );

        Ok(())
    }

    async fn handle_delete_records(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        raw: Arc<EmittedEvent>,
        event: &DeleteRecordsV1,
    ) -> Result<()> {
        let table_name = &event.table_name;
        let formatted_values: Result<Vec<String>> = event
            .values
            .iter()
            .map(|v| Ok(format!(r#"'{}'"#, v.to_postgres_value()?)))
            .collect();

        sqlx::query(&format!(
            r#"DELETE FROM "{}" WHERE "{}" IN ({})"#,
            event.table_name,
            event.id_field,
            formatted_values?.join(", ")
        ))
        .execute(&mut **tx)
        .await?;

        tracing::info!(
            sink = %self.label,
            table = %event.table_name,
            storage_table = %table_name,
            block = %raw.block_number.unwrap_or_default(),
            "stored delete_records"
        );

        Ok(())
    }
}

#[async_trait]
impl Sink for PostgresSink {
    fn label(&self) -> &str {
        &self.label
    }

    async fn handle_batch(&self, batch: Batch) -> anyhow::Result<()> {
        for env in &batch.items {
            let mut tx = self.pool.begin().await?;
            if env.type_id == DeclareTableV1::TYPE_ID {
                if let Some(event) = env.downcast::<DeclareTableV1>() {
                    println!("{}", event.name);
                    match self
                        .handle_declare_table(&mut tx, env.raw.clone(), event)
                        .await
                    {
                        Ok(_) => (),
                        Err(err) => {
                            println!("Error declaring table: {err:#?}");
                            return Err(err.into());
                        }
                    }
                }
            } else if env.type_id == UpdateRecordFieldsV1::TYPE_ID {
                if let Some(event) = env.downcast::<UpdateRecordFieldsV1>() {
                    match self
                        .handle_update_record(&mut tx, env.raw.clone(), event)
                        .await
                    {
                        Ok(_) => (),
                        Err(err) => {
                            // println!("Error updating record: {err:#?}");
                        }
                    }
                }
            } else if env.type_id == DeleteRecordsV1::TYPE_ID {
                if let Some(event) = env.downcast::<DeleteRecordsV1>() {
                    match self
                        .handle_delete_records(&mut tx, env.raw.clone(), event)
                        .await
                    {
                        Ok(_) => (),
                        Err(_err) => {
                            println!("Error deleting records");
                        }
                    }
                }
            }
            tx.commit().await?;
        }

        tracing::info!(sink = %self.label, processed = batch.items.len(), "sqlite sink processed batch");
        Ok(())
    }
}
