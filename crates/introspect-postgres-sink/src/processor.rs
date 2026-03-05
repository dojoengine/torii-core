use crate::json::PostgresJsonSerializer;
use crate::sql::write_conflict_res;
use crate::table::{PgTable, PgTableError};
use async_trait::async_trait;
use introspect_types::ResultInto;
use serde_json::Serializer as JsonSerializer;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::io::Write;
use thiserror::Error;
use torii::etl::EventContext;
use torii_introspect::events::IntrospectMsg;
use torii_introspect::{CreateTable, InsertsFields};
#[derive(Debug, Error)]
pub enum DbError {
    #[error(transparent)]
    DatabaseError(#[from] sqlx::Error),
    #[error("Invalid event format: {0}")]
    InvalidEventFormat(String),
    #[error(transparent)]
    MigrationError(#[from] sqlx::migrate::MigrateError),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    TableError(#[from] PgTableError),
    #[error("Table with id: {0} already exists, incoming name: {1}, existing name: {2}")]
    TableAlreadyExists(Felt, String, String),
    #[error("Table not found with id: {0}")]
    TableNotFound(Felt),
    #[error("Table not alive - id: {0}, name: {1}")]
    TableNotAlive(Felt, String),
    #[error("Manager does not support updating")]
    UpdateNotSupported,
}

type PGSinkResult<T> = std::result::Result<T, DbError>;

#[derive(Debug, Default)]
pub struct PostgresSchema {
    namespace: Option<String>,
    tables: HashMap<Felt, PgTable>,
}

impl PostgresSchema {
    pub fn get_table(&self, id: &Felt) -> PGSinkResult<&PgTable> {
        match self.tables.get(id) {
            Some(table) => Ok(table),
            None => Err(DbError::TableNotFound(*id)),
        }
    }

    pub fn get_living_table(&self, id: &Felt) -> PGSinkResult<&PgTable> {
        let table = self.get_table(id)?;
        if table.alive {
            Ok(table)
        } else {
            Err(DbError::TableNotAlive(*id, table.name.clone()))
        }
    }

    pub fn create_table(
        &mut self,
        event: CreateTable,
        _context: &EventContext,
        queries: &mut Vec<String>,
    ) -> PGSinkResult<()> {
        if let Some(table) = self.tables.get(&event.id) {
            Err(DbError::TableAlreadyExists(
                event.id,
                event.name,
                table.name().to_string(),
            ))
        } else {
            let (id, table) = PgTable::new_from_event(&self.namespace, event, queries)?;
            self.tables.insert(id, table);
            Ok(())
        }
    }

    pub fn set_table_dead(&mut self, id: &Felt) -> PGSinkResult<()> {
        match self.tables.get_mut(id) {
            Some(table) => {
                table.alive = false;
                Ok(())
            }
            None => Err(DbError::TableNotFound(*id)),
        }
    }

    pub fn insert_fields(
        &self,
        event: &InsertsFields,
        _context: &EventContext,
        queries: &mut Vec<String>,
    ) -> PGSinkResult<()> {
        let table = self.get_living_table(&event.table)?;
        let schema = table.get_schema(&event.columns)?;
        let table_name = table.name();

        let mut writer = Vec::new();
        write!(
            writer,
            r#"INSERT INTO "{table_name}"
            SELECT * FROM jsonb_populate_recordset(NULL::"{table_name}", $$"#
        )?;
        schema.parse_records_with_metadata(
            &event.records,
            &(),
            &mut JsonSerializer::new(&mut writer),
            &PostgresJsonSerializer,
        )?;
        write!(
            writer,
            r#"$$) ON CONFLICT ("{}") DO UPDATE SET "#,
            schema.primary().name
        )?;
        if let Some((coln, cols)) = schema.columns().split_last() {
            for column in cols {
                write_conflict_res::<true, _>(&mut writer, table_name, &column.name)?;
            }
            write_conflict_res::<false, _>(&mut writer, table_name, &coln.name)?;
        }
        let string = unsafe { String::from_utf8_unchecked(writer) };
        queries.push(string);
        Ok(())
    }

    pub fn handle_message(
        &mut self,
        msg: &IntrospectMsg,
        context: &EventContext,
        queries: &mut Vec<String>,
    ) -> PGSinkResult<()> {
        match msg {
            IntrospectMsg::CreateTable(event) => self.create_table(event.clone(), context, queries),
            IntrospectMsg::AddColumns(event) => self.set_table_dead(&event.table),
            IntrospectMsg::DropColumns(event) => self.set_table_dead(&event.table),
            IntrospectMsg::RetypeColumns(event) => self.set_table_dead(&event.table),
            IntrospectMsg::RetypePrimary(event) => self.set_table_dead(&event.table),
            IntrospectMsg::UpdateTable(event) => self.set_table_dead(&event.id),
            IntrospectMsg::RenameTable(_)
            | IntrospectMsg::DropTable(_)
            | IntrospectMsg::RenameColumns(_)
            | IntrospectMsg::RenamePrimary(_) => Ok(()),
            IntrospectMsg::InsertsFields(event) => self.insert_fields(event, context, queries),
            IntrospectMsg::DeleteRecords(_) | IntrospectMsg::DeletesFields(_) => Ok(()),
            IntrospectMsg::None => Ok(()),
        }
    }
    // pub fn handle_message(
    //     &mut self,
    //     msg: &IntrospectMsg,
    //     context: &EventContext,
    //     queries: &mut Vec<String>,
    // ) -> PGSinkResult<()> {
    //     match msg {
    //         IntrospectMsg::CreateTable(event) => self.create_table(event.clone(), context, queries),
    //         IntrospectMsg::AddColumns(event) => self.set_table_dead(&event.table),
    //         IntrospectMsg::DropColumns(event) => self.set_table_dead(&event.table),
    //         IntrospectMsg::RetypeColumns(event) => self.set_table_dead(&event.table),
    //         IntrospectMsg::RetypePrimary(event) => self.set_table_dead(&event.table),
    //         IntrospectMsg::UpdateTable(event) => self.set_table_dead(&event.id),
    //         IntrospectMsg::RenameTable(_)
    //         | IntrospectMsg::DropTable(_)
    //         | IntrospectMsg::RenameColumns(_)
    //         | IntrospectMsg::RenamePrimary(_) => Ok(()),
    //         IntrospectMsg::InsertsFields(_)
    //         | IntrospectMsg::DeleteRecords(_)
    //         | IntrospectMsg::DeletesFields(_) => Ok(()),
    //         IntrospectMsg::None => Ok(()),
    //     }
    // }
}

pub struct PostgresSimpleDb {
    schema: PostgresSchema,
    pool: PgPool,
}

impl PostgresSimpleDb {
    pub async fn new(database_url: &str, max_connections: Option<u32>) -> PGSinkResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.unwrap_or(5))
            .connect(database_url)
            .await?;
        Ok(Self {
            schema: PostgresSchema::default(),
            pool,
        })
    }

    pub async fn initialize(&self) -> PGSinkResult<()> {
        self.migrate().await?;
        Ok(())
    }

    pub async fn process_message(
        &mut self,
        msg: &IntrospectMsg,
        context: &EventContext,
    ) -> PGSinkResult<()> {
        let mut queries = Vec::new();
        self.schema.handle_message(msg, context, &mut queries)?;
        self.execute_queries(&queries).await
    }
}

pub struct MessageWithContext<'a, M> {
    pub msg: &'a M,
    pub context: &'a EventContext,
}

#[async_trait]
pub trait PostgresConnection {
    fn pool(&self) -> &PgPool;

    async fn new_transaction(&self) -> PGSinkResult<Transaction<'_, Postgres>> {
        Ok(self.pool().begin().await?)
    }

    async fn migrate(&self) -> PGSinkResult<()> {
        sqlx::migrate!("./migrations")
            .run(self.pool())
            .await
            .err_into()
    }
    async fn execute_queries(&self, queries: &[String]) -> PGSinkResult<()> {
        let mut transaction = self.new_transaction().await?;
        for query in queries {
            sqlx::query(query).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

impl PostgresConnection for PostgresSimpleDb {
    fn pool(&self) -> &PgPool {
        &self.pool
    }
}
