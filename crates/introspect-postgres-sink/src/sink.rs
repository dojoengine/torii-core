use crate::manager::{PgTableManager, TableManagerError};
use crate::value::PostgresValueError;
use crate::{table, TableManager};
use async_trait::async_trait;
use introspect_types::{ResultInto, TableSchema};
use serde_json::Serializer as JsonSerializer;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Postgres, Transaction};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use torii_introspect::events::IntrospectMsg;
use torii_introspect::tables::{IntrospectTables, TableError as IntrospectTableError};
use torii_introspect::{
    AddColumns, CreateTable, DeleteRecords, DeletesFields, DropColumns, DropTable, InsertsFields,
    IntrospectMsgTrait, Record, RenameColumns, RenamePrimary, RenameTable, RetypeColumns,
    RetypePrimary, UpdateTable,
};

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
    #[error(transparent)]
    IntrospectError(#[from] IntrospectTableError),
}

type PGSinkResult<T> = std::result::Result<T, PostgresSinkError>;

pub struct PostgresSink {
    label: String,
    pool: PgPool,
    manager: TableManager,
}

pub struct PostgresDb {
    pool: PgPool,
    tables: TableManager,
}

pub struct BlockData {
    pub block_number: u64,
    pub block_hash: Felt,
    pub block_timestamp: u64,
}

pub struct MetaData {
    from_address: Felt,
    transaction_hash: Felt,
    block_data: Arc<BlockData>,
}

impl MetaData {
    pub fn from_address(&self) -> &Felt {
        &self.from_address
    }
    pub fn transaction_hash(&self) -> &Felt {
        &self.transaction_hash
    }
    pub fn block_number(&self) -> u64 {
        self.block_data.block_number
    }
    pub fn block_hash(&self) -> &Felt {
        &self.block_data.block_hash
    }
    pub fn block_timestamp(&self) -> u64 {
        self.block_data.block_timestamp
    }
}

#[async_trait]
pub trait PGIntrospectMsgProcessor<Db>: IntrospectMsgTrait {
    async fn process(
        &self,
        db: &Db,
        tx: &mut Transaction<'_, Postgres>,
        event_data: &MetaData,
    ) -> PGSinkResult<()>;
}

#[async_trait]
impl<Db: PgTableManager> PGIntrospectMsgProcessor<Db> for CreateTable {
    async fn process(
        &self,
        db: &Db,
        tx: &mut Transaction<'_, Postgres>,
        event_data: &MetaData,
    ) -> PGSinkResult<()> {
        db.declare_table(
            self.id,
            &self.name,
            &self.attributes,
            &self.primary,
            &self.columns,
            tx,
        )
        .map_err(Into::into)
    }
}

#[async_trait]
impl<Db: PgTableManager> PGIntrospectMsgProcessor<Db> for RenameTable {
    async fn process(
        &self,
        db: &Db,
        tx: &mut Transaction<'_, Postgres>,
        event_data: &MetaData,
    ) -> PGSinkResult<()> {
        db.rename_table(self.id, &self.name, tx).map_err(Into::into)
    }
}

#[async_trait]
impl<Db: PgTableManager> PGIntrospectMsgProcessor<Db> for AddColumns {
    async fn process(
        &self,
        db: &Db,
        tx: &mut Transaction<'_, Postgres>,
        event_data: &MetaData,
    ) -> PGSinkResult<()> {
        db.add_columns(self.id, &self.columns, tx)
            .map_err(Into::into)
    }
}

#[async_trait]
impl<Db: PgTableManager> PGIntrospectMsgProcessor<Db> for RenameColumns {
    async fn process(
        &self,
        db: &Db,
        tx: &mut Transaction<'_, Postgres>,
        event_data: &MetaData,
    ) -> PGSinkResult<()> {
        db.rename_columns(self.id, &self.columns, tx)
            .map_err(Into::into)
    }
}

#[async_trait]
impl<Db: IntrospectTables> PGIntrospectMsgProcessor<Db> for InsertsFields {
    async fn process(
        &self,
        db: &Db,
        tx: &mut Transaction<'_, Postgres>,
        event_data: &MetaData,
    ) -> PGSinkResult<()> {
        let schema = db.get_record_schema(self.table, &self.columns)?;
        let table_name = schema.name();
        let update_fields = self
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

        let mut writer = String::new();
        write!(
            &mut writer,
            r#"INSERT INTO "{table_name}"
            SELECT * FROM jsonb_populate_record(NULL::"{table_name}", $$"#
        );
        schema.parse_record()
        JsonSerializer::new(&mut writer).serialize_map()
        write!(
            &mut writer,
            r#"$$) ON CONFLICT ("{}") DO UPDATE SET "#,
            schema.primary().name
        );
        if let Some((cols, coln)) = schema.columns().split_last() {
            for column in schema.columns() {
                let name = &column.name;
                write_conflict_res(&mut writer, table_name, name)?;
                write.write_char
            }
        }
        sqlx::query(&writer).execute(&mut **tx).await?;
        tracing::info!(
            sink = %self.label,
            table = %event.table_name,
            storage_table = %table_name,
            block = %raw.block_number.unwrap_or_default(),
            "stored update_record"
        );

        Ok(())
    }
}

pub fn write_conflict_res<W: Write, const LAST: bool>(
    writer: &mut W,
    table: &str,
    column: &str,
) -> std::io::Result<()> {
    let separator = if LAST { "" } else { ", " };
    write!(
        writer,
        r#""{column}" = COALESCE(EXCLUDED."{column}", "{table}"."{column}"){separator}"#,
    )
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
        event: &CreateTable,
    ) -> Result<()> {
        let table_name = &event.name;
        let queries = self
            .manager
            .declare_table(table_name, &event.primary, &event.columns)?;
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

    async fn handle_update_table(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        raw: Arc<EmittedEvent>,
        event: &UpdateTable,
    ) -> Result<()> {
        let table_name = &event.name;
        let queries = self
            .manager
            .declare_table(table_name, &event.primary, &event.columns)?;
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
        event: &InsertsFields,
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
        event: &DeleteRecords,
    ) -> Result<()> {
        let table_name = &event.table_name;
        let formatted_values: Result<Vec<String>> = event
            .records
            .iter()
            .map(|v| Ok(format!(r#"'{}'"#, v.to_string())))
            .collect();

        sqlx::query(&format!(
            r#"DELETE FROM "{}" WHERE "{}" IN ({})"#,
            event.table_name,
            event.primary_name,
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

    async fn handle_event(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        event: IntrospectMsg,
        metadata: &Metadata,
    ) -> PGSinkResult<()> {
        match event {
            IntrospectMsg::CreateTable(event) => {
                self.handle_declare_table(tx, metadata.raw, &event).await
            }
            IntrospectMsg::UpdateTable(event) => {
                self.handle_update_table(tx, metadata.raw, &event).await
            }
            IntrospectMsg::InsertsFields(event) => {
                self.handle_update_record(tx, metadata.raw, &event).await
            }
            IntrospectMsg::DeleteRecords(event) => {
                self.handle_delete_records(tx, metadata.raw, &event).await
            }
            _ => {
                println!("Unsupported event type: {:?}", event);
                Ok(())
            }
        }
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
            if env.type_id == CreateTable::TYPE_ID {
                if let Some(event) = env.downcast::<CreateTable>() {
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
            }
            if env.type_id == UpdateTable::TYPE_ID {
                if let Some(event) = env.downcast::<UpdateTable>() {
                    println!("{}", event.name);
                    match self
                        .handle_update_table(&mut tx, env.raw.clone(), event)
                        .await
                    {
                        Ok(_) => (),
                        Err(err) => {
                            println!("Error declaring table: {err:#?}");
                            return Err(err.into());
                        }
                    }
                }
            } else if env.type_id == UpdateRecordFields::TYPE_ID {
                if let Some(event) = env.downcast::<UpdateRecordFields>() {
                    match self
                        .handle_update_record(&mut tx, env.raw.clone(), event)
                        .await
                    {
                        Ok(_) => (),
                        Err(err) => {
                            println!("Error updating record: {err:#?}");
                        }
                    }
                }
            } else if env.type_id == DeleteRecords::TYPE_ID {
                if let Some(event) = env.downcast::<DeleteRecords>() {
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
        }
        tx.commit().await?;

        tracing::info!(sink = %self.label, processed = batch.items.len(), "sqlite sink processed batch");
        Ok(())
    }
}
