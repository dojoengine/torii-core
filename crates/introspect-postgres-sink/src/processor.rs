use crate::json::PostgresJsonSerializer;
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
use torii::etl::{BlockContext, EventContext, TransactionContext};
use torii_introspect::events::IntrospectMsg;
use torii_introspect::tables::{IntrospectTables, TableError as IntrospectTableError};
use torii_introspect::{
    AddColumns, CreateTable, DeleteRecords, DeletesFields, DropColumns, DropTable, EventId,
    InsertsFields, Record, RenameColumns, RenamePrimary, RenameTable, RetypeColumns, RetypePrimary,
    UpdateTable,
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
    #[error("IO error: {0}")]
    IoWriteError(#[from] std::io::Error),
}

type PGSinkResult<T> = std::result::Result<T, PostgresSinkError>;

pub struct PostgresDb {
    pool: PgPool,
    tables: TableManager,
}

#[async_trait]
pub trait PGIntrospectMsgProcessor<Db>: EventId {
    async fn process(
        &self,
        db: &Db,
        context: &EventContext,
        tx: &mut Transaction<'_, Postgres>,
    ) -> PGSinkResult<()>;
}

#[async_trait]
impl<Db: PgTableManager + Sync> PGIntrospectMsgProcessor<Db> for CreateTable {
    async fn process(
        &self,
        db: &Db,
        _context: &EventContext,
        tx: &mut Transaction<'_, Postgres>,
    ) -> PGSinkResult<()> {
        db.declare_table(
            self.id,
            &self.name,
            &self.attributes,
            &self.primary,
            &self.columns,
            tx,
        )
        .err_into()
    }
}

#[async_trait]
impl<Db: PgTableManager + Sync> PGIntrospectMsgProcessor<Db> for RenameTable {
    async fn process(
        &self,
        db: &Db,
        _context: &EventContext,
        tx: &mut Transaction<'_, Postgres>,
    ) -> PGSinkResult<()> {
        db.rename_table(self.id, &self.name, tx).err_into()
    }
}

#[async_trait]
impl<Db: PgTableManager + Sync> PGIntrospectMsgProcessor<Db> for AddColumns {
    async fn process(
        &self,
        db: &Db,
        _context: &EventContext,
        tx: &mut Transaction<'_, Postgres>,
    ) -> PGSinkResult<()> {
        db.add_columns(self.table, &self.columns, tx).err_into()
    }
}

#[async_trait]
impl<Db: PgTableManager + Sync> PGIntrospectMsgProcessor<Db> for RenameColumns {
    async fn process(
        &self,
        db: &Db,
        _context: &EventContext,
        tx: &mut Transaction<'_, Postgres>,
    ) -> PGSinkResult<()> {
        db.rename_columns(self.table, &self.columns, tx).err_into()
    }
}

#[async_trait]
impl<Db: IntrospectTables + Sync> PGIntrospectMsgProcessor<Db> for InsertsFields {
    async fn process(
        &self,
        db: &Db,
        context: &EventContext,
        tx: &mut Transaction<'_, Postgres>,
    ) -> PGSinkResult<()> {
        let schema = db.get_record_schema(self.table, &self.columns)?;
        let table_name = schema.name();

        let mut writer = Vec::new();
        write!(
            writer,
            r#"INSERT INTO "{table_name}"
            SELECT * FROM jsonb_populate_record(NULL::"{table_name}", $$"#
        );
        let serializer = &mut JsonSerializer::new(writer);
        schema.parse_records_with_metadata(
            &self.records,
            &self.metadata,
            serializer,
            &PostgresJsonSerializer,
        )?;
        write!(
            writer,
            r#"$$) ON CONFLICT ("{}") DO UPDATE SET "#,
            schema.primary().name
        );
        if let Some((coln, cols)) = schema.columns().split_last() {
            for column in cols {
                write_conflict_res::<true, _>(&mut writer, table_name, &column.name)?;
            }
            write_conflict_res::<false, _>(&mut writer, table_name, &coln.name)?;
        }
        let string = unsafe { String::from_utf8_unchecked(writer) };
        sqlx::query(&string).execute(&mut **tx).await?;
        tracing::info!(
            sink = "Postgres-introspect",
            table = %table_name,
            storage_table = %table_name,
            block = %context.block.number,
            "stored update_record"
        );

        Ok(())
    }
}

pub fn write_conflict_res<const DELIMINATOR: bool, W: Write>(
    writer: &mut W,
    table: &str,
    column: &str,
) -> std::io::Result<()> {
    let separator = if DELIMINATOR { ", " } else { "" };
    write!(
        writer,
        r#""{column}" = COALESCE(EXCLUDED."{column}", "{table}"."{column}"){separator}"#,
    )
}

#[async_trait]
impl<Db: PgTableManager + IntrospectTables + Send + Sync> PGIntrospectMsgProcessor<Db>
    for IntrospectMsg
{
    async fn process(
        &self,
        db: &Db,
        context: &EventContext,
        tx: &mut Transaction<'_, Postgres>,
    ) -> PGSinkResult<()> {
        match self {
            IntrospectMsg::CreateTable(event) => event.process(db, context, tx).await,
            IntrospectMsg::RenameTable(event) => event.process(db, context, tx).await,
            IntrospectMsg::AddColumns(event) => event.process(db, context, tx).await,
            IntrospectMsg::RenameColumns(event) => event.process(db, context, tx).await,
            IntrospectMsg::InsertsFields(event) => event.process(db, context, tx).await,
            _ => {
                println!("Unsupported event type: {:?}", self);
                Ok(())
            }
        }
    }
}

impl PostgresDb {
    pub async fn connect(database_url: &str, max_connections: Option<u32>) -> PGSinkResult<Self> {
        let options = PgConnectOptions::from_str(database_url)?;
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.unwrap_or(5))
            .connect_with(options)
            .await?;

        Ok(Self {
            pool,
            tables: TableManager::default(),
        })
    }

    pub async fn initialize(&self) -> PGSinkResult<()> {
        match sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .err_into()
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
}

// #[async_trait]
// impl Sink for PostgresSink {
//     fn label(&self) -> &str {
//         &self.label
//     }

//     async fn handle_batch(&self, batch: Batch) -> anyhow::Result<()> {
//         let mut tx = self.pool.begin().await?;

//         for env in &batch.items {
//             if env.type_id == CreateTable::TYPE_ID {
//                 if let Some(event) = env.downcast::<CreateTable>() {
//                     println!("{}", event.name);
//                     match self
//                         .handle_declare_table(&mut tx, env.raw.clone(), event)
//                         .await
//                     {
//                         Ok(_) => (),
//                         Err(err) => {
//                             println!("Error declaring table: {err:#?}");
//                             return Err(err.into());
//                         }
//                     }
//                 }
//             }
//             if env.type_id == UpdateTable::TYPE_ID {
//                 if let Some(event) = env.downcast::<UpdateTable>() {
//                     println!("{}", event.name);
//                     match self
//                         .handle_update_table(&mut tx, env.raw.clone(), event)
//                         .await
//                     {
//                         Ok(_) => (),
//                         Err(err) => {
//                             println!("Error declaring table: {err:#?}");
//                             return Err(err.into());
//                         }
//                     }
//                 }
//             } else if env.type_id == UpdateRecordFields::TYPE_ID {
//                 if let Some(event) = env.downcast::<UpdateRecordFields>() {
//                     match self
//                         .handle_update_record(&mut tx, env.raw.clone(), event)
//                         .await
//                     {
//                         Ok(_) => (),
//                         Err(err) => {
//                             println!("Error updating record: {err:#?}");
//                         }
//                     }
//                 }
//             } else if env.type_id == DeleteRecords::TYPE_ID {
//                 if let Some(event) = env.downcast::<DeleteRecords>() {
//                     match self
//                         .handle_delete_records(&mut tx, env.raw.clone(), event)
//                         .await
//                     {
//                         Ok(_) => (),
//                         Err(_err) => {
//                             println!("Error deleting records");
//                         }
//                     }
//                 }
//             }
//         }
//         tx.commit().await?;

//         tracing::info!(sink = %self.label, processed = batch.items.len(), "sqlite sink processed batch");
//         Ok(())
//     }
// }
