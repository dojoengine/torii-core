use crate::json::ToPostgresJson;
use crate::types::{PostgresComplexTypes, PostgresTypeExtractor};
use crate::value::{PostgresValueError, ToPostgresValue};
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
}

type Result<T> = std::result::Result<T, PostgresSinkError>;

pub struct PostgresSink {
    label: String,
    pool: PgPool,
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
            // types: RwLock::new(PostgresTypes::default()),
        })
    }

    pub async fn initialize(&self) -> Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(PostgresSinkError::from)
    }

    async fn handle_declare_table(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        raw: Arc<EmittedEvent>,
        event: &DeclareTableV1,
    ) -> Result<()> {
        let table_name = &event.name;

        // Create SQL statements in a separate scope to ensure RwLock is released
        let (create_table_sql, create_types) = {
            let mut types = vec![];
            let primary_field = event.id_field.type_def.extract_type_string(&mut types);
            // Build the CREATE TABLE statement
            let mut create_table_sql = format!(r#"CREATE TABLE IF NOT EXISTS "{table_name}" ("#,);
            let primary_key = event.id_field.name.as_str();
            let columns = event
                .fields
                .iter()
                .map(|f| (f.name.clone(), f.type_def.extract_type(&mut types)))
                .collect::<Vec<_>>();
            create_table_sql.push_str(&format!(r#""{primary_key}" {primary_field} PRIMARY KEY, "#));
            let columns = columns
                .iter()
                .map(|(name, ty)| format!(r#""{}" {}"#, name, ty.to_string()))
                .collect::<Vec<_>>();

            create_table_sql.push_str(&columns.join(", "));
            create_table_sql.push_str(");");

            (create_table_sql, types.create_type_queries())
        }; // RwLock is automatically dropped here
           // Now we can safely use await since the lock is released
        for create_type in create_types {
            sqlx::query(&create_type).execute(&mut **tx).await?;
        }
        sqlx::query(&create_table_sql).execute(&mut **tx).await?;

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
        let primary_key_name = event.id_field.name.clone();
        let update_fields = event
            .fields
            .iter()
            .map(|f| format!(r#""{}" = EXCLUDED."{}""#, f.name, f.name))
            .collect::<Vec<_>>()
            .join(", ");
        let json_data = serde_json::to_string(&event.to_postgres_json())?;
        let query = format!(
            r#"INSERT INTO "{table_name}"
            SELECT * FROM jsonb_populate_record(NULL::"{table_name}", $${json_data}$$)
            ON CONFLICT ("{primary_key_name}") DO UPDATE SET {update_fields}
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
            .map(|v| Ok(format!(r#""{}""#, v.to_postgres_value()?)))
            .collect();
        sqlx::query(r#"DELETE FROM "$1" WHERE "$2" IN ($3))"#)
            .bind(event.table_name.to_string())
            .bind(event.id_field.to_string())
            .bind(formatted_values?.join(", "))
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
        let mut tx = self.pool.begin().await?;
        for env in &batch.items {
            if env.type_id == DeclareTableV1::TYPE_ID {
                if let Some(event) = env.downcast::<DeclareTableV1>() {
                    self.handle_declare_table(&mut tx, env.raw.clone(), event)
                        .await?;
                }
            } else if env.type_id == UpdateRecordFieldsV1::TYPE_ID {
                if let Some(event) = env.downcast::<UpdateRecordFieldsV1>() {
                    self.handle_update_record(&mut tx, env.raw.clone(), event)
                        .await?;
                }
            } else if env.type_id == DeleteRecordsV1::TYPE_ID {
                if let Some(event) = env.downcast::<DeleteRecordsV1>() {
                    self.handle_delete_records(&mut tx, env.raw.clone(), event)
                        .await?;
                }
            }
        }
        tx.commit().await?;

        tracing::info!(sink = %self.label, processed = batch.items.len(), "sqlite sink processed batch");
        Ok(())
    }
}
