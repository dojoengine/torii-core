use crate::json::PostgresJsonSerializer;
use crate::sql::{
    add_column_if_not_exists_query, add_column_query, alter_table_query, modify_column_query,
    write_conflict_res,
};
use crate::table::{PgTable, PgTableError};
use crate::types::{PgTableStructure, PgTypeError, PostgresFieldExtractor};
use crate::HasherExt;
use crate::INTROSPECT_PG_SINK_MIGRATIONS;
use async_trait::async_trait;
use introspect_types::ResultInto;
use introspect_types::ResultInto;
use serde_json::Serializer as JsonSerializer;
use serde_json::Serializer as JsonSerializer;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use sqlx::Row;
use sqlx::{PgPool, Postgres, Transaction};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::io::Write;
use thiserror::Error;
use torii::etl::EventContext;
use torii_introspect::events::IntrospectMsg;
use torii_introspect::{CreateTable, InsertsFields, UpdateTable};
use torii_postgres::PostgresConnection;

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
    #[error(transparent)]
    TypeError(#[from] PgTypeError),
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
            if table.name() == event.name {
                Ok(())
            } else {
                Err(DbError::TableAlreadyExists(
                    event.id,
                    event.name,
                    table.name().to_string(),
                ))
            }
        } else {
            let (id, table) = PgTable::new_from_event(&self.namespace, event, queries)?;
            self.tables.insert(id, table);
            Ok(())
        }
    }

    pub fn update_table(
        &mut self,
        event: UpdateTable,
        context: &EventContext,
        queries: &mut Vec<String>,
    ) -> PGSinkResult<()> {
        let create_snapshot = CreateTable {
            id: event.id,
            name: event.name,
            attributes: event.attributes,
            primary: event.primary,
            columns: event.columns,
        };
        let Some(existing) = self.tables.get(&create_snapshot.id) else {
            return self.create_table(create_snapshot, context, queries);
        };

        if existing.name() != create_snapshot.name {
            return Err(DbError::TableAlreadyExists(
                create_snapshot.id,
                create_snapshot.name,
                existing.name().to_string(),
            ));
        }

        let mut type_queries = Vec::new();
        let mut type_schema = PgTableStructure::default();
        let branch = xxhash_rust::xxh3::Xxh3::new_based(existing.name());
        let mut alterations = Vec::new();
        for column in &create_snapshot.columns {
            let pg_type = column.extract_type(&mut type_schema, &branch, &mut type_queries)?;
            match existing.columns.get(&column.id) {
                None => alterations.push(add_column_query(&column.name, &pg_type)),
                Some(old) if old.type_def != column.type_def => {
                    alterations.push(modify_column_query(&column.name, &pg_type));
                }
                _ => {}
            }
        }
        queries.extend(type_queries);
        if !alterations.is_empty() {
            queries.push(alter_table_query(existing.name(), &alterations));
        }

        let mut ignored_queries = Vec::new();
        let (_, replacement_table) = PgTable::new_from_event(
            &self.namespace,
            create_snapshot.clone(),
            &mut ignored_queries,
        )?;
        self.tables.insert(event.id, replacement_table);

        Ok(())
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
            IntrospectMsg::UpdateTable(event) => self.update_table(event.clone(), context, queries),
            IntrospectMsg::AddColumns(event) => self.set_table_dead(&event.table),
            IntrospectMsg::DropColumns(event) => self.set_table_dead(&event.table),
            IntrospectMsg::RetypeColumns(event) => self.set_table_dead(&event.table),
            IntrospectMsg::RetypePrimary(event) => self.set_table_dead(&event.table),
            IntrospectMsg::RenameTable(_)
            | IntrospectMsg::DropTable(_)
            | IntrospectMsg::RenameColumns(_)
            | IntrospectMsg::RenamePrimary(_) => Ok(()),
            IntrospectMsg::InsertsFields(event) => self.insert_fields(event, context, queries),
            IntrospectMsg::DeleteRecords(_) | IntrospectMsg::DeletesFields(_) => Ok(()),
        }
    }
}

pub struct PostgresSimpleDb<T> {
    schema: PostgresSchema,
    pool: T,
}

impl<T: PostgresConnection> PostgresConnection for PostgresSimpleDb<T> {
    fn pool(&self) -> &PgPool {
        self.pool.pool()
    }
}

impl<T: PostgresConnection + Send + Sync> PostgresSimpleDb<T> {
    pub fn new(pool: T) -> Self {
        Self {
            schema: PostgresSchema::default(),
            pool,
        }
    }

    pub async fn initialize(&self) -> PGSinkResult<()> {
        self.migrate(INTROSPECT_PG_SINK_MIGRATIONS)
            .await
            .err_into()?;
        let tables = self.load_stored_tables().await?;
        if !tables.is_empty() {
            self.bootstrap_tables(&tables).await?;
            tracing::info!(
                target: "torii::sinks::introspect::postgres",
                tables = tables.len(),
                "Loaded sink schema state from PostgreSQL storage"
            );
        }
        Ok(())
    }

    pub async fn bootstrap_tables(&mut self, tables: &[CreateTable]) -> PGSinkResult<()> {
        let context = EventContext::default();
        for table in tables {
            self.process_message(&IntrospectMsg::CreateTable(table.clone()), &context)
                .await?;
            self.execute_queries(&self.bootstrap_reconcile_queries(table)?)
                .await?;
        }
        Ok(())
    }

    fn bootstrap_reconcile_queries(&self, table: &CreateTable) -> PGSinkResult<Vec<String>> {
        let mut type_queries = Vec::new();
        let mut type_schema = PgTableStructure::default();
        let branch = xxhash_rust::xxh3::Xxh3::new_based(&table.name);
        let mut alterations = Vec::new();
        for column in &table.columns {
            let pg_type = column.extract_type(&mut type_schema, &branch, &mut type_queries)?;
            alterations.push(add_column_if_not_exists_query(&column.name, &pg_type));
        }
        if !alterations.is_empty() {
            type_queries.push(alter_table_query(&table.name, &alterations));
        }
        Ok(type_queries)
    }

    pub fn has_tables(&self) -> bool {
        !self.schema.tables.is_empty()
    }

    pub async fn process_message(
        &mut self,
        msg: &IntrospectMsg,
        context: &EventContext,
    ) -> PGSinkResult<()> {
        let mut queries = Vec::new();
        self.schema.handle_message(msg, context, &mut queries)?;
        self.execute_queries(&queries).await.err_into()?;
        match msg {
            IntrospectMsg::CreateTable(table) => {
                self.persist_table_state(table).await?;
            }
            IntrospectMsg::UpdateTable(table) => {
                self.persist_table_state(&CreateTable {
                    id: table.id,
                    name: table.name.clone(),
                    attributes: table.attributes.clone(),
                    primary: table.primary.clone(),
                    columns: table.columns.clone(),
                })
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn load_stored_tables(&self) -> PGSinkResult<Vec<CreateTable>> {
        let rows = sqlx::query(
            r#"
            SELECT create_table_json
            FROM torii_introspect_schema_state
            ORDER BY updated_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let json: String = row.try_get("create_table_json")?;
                Ok(serde_json::from_str(&json)?)
            })
            .collect()
    }

    async fn persist_table_state(&self, table: &CreateTable) -> PGSinkResult<()> {
        let json = serde_json::to_string(table)?;
        sqlx::query(
            r#"
            INSERT INTO torii_introspect_schema_state (table_id, create_table_json, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (table_id)
            DO UPDATE SET
                create_table_json = EXCLUDED.create_table_json,
                updated_at = NOW()
            "#,
        )
        .bind(table.id.to_bytes_be().to_vec())
        .bind(json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

pub struct MessageWithContext<'a, M> {
    pub msg: &'a M,
    pub context: &'a EventContext,
}

#[cfg(test)]
mod tests {
    use super::*;
    use introspect_types::{ColumnDef, PrimaryDef, PrimaryTypeDef, TypeDef};

    fn primary() -> PrimaryDef {
        PrimaryDef {
            name: "entity_id".to_string(),
            attributes: vec![],
            type_def: PrimaryTypeDef::Felt252,
        }
    }

    fn column(id: u64, name: &str) -> ColumnDef {
        ColumnDef {
            id: Felt::from(id),
            name: name.to_string(),
            attributes: vec![],
            type_def: TypeDef::U32,
        }
    }

    fn create_table(id: u64, columns: Vec<ColumnDef>) -> CreateTable {
        CreateTable {
            id: Felt::from(id),
            name: "duel-state".to_string(),
            attributes: vec![],
            primary: primary(),
            columns,
        }
    }

    fn update_table(id: u64, columns: Vec<ColumnDef>) -> UpdateTable {
        UpdateTable {
            id: Felt::from(id),
            name: "duel-state".to_string(),
            attributes: vec![],
            primary: primary(),
            columns,
        }
    }

    #[test]
    fn update_table_creates_unknown_table() {
        let mut schema = PostgresSchema::default();
        let mut queries = Vec::new();

        schema
            .update_table(
                update_table(1, vec![column(10, "duelist_count")]),
                &EventContext::default(),
                &mut queries,
            )
            .unwrap();

        assert!(!queries.is_empty());
        assert!(queries
            .iter()
            .any(|query| query.contains("CREATE TABLE IF NOT EXISTS")));
        assert!(schema.tables.contains_key(&Felt::from(1_u64)));
    }

    #[test]
    fn update_table_emits_alter_queries_for_new_columns() {
        let mut schema = PostgresSchema::default();
        let mut create_queries = Vec::new();
        schema
            .create_table(
                create_table(1, vec![column(10, "duelist_count")]),
                &EventContext::default(),
                &mut create_queries,
            )
            .unwrap();

        let mut update_queries = Vec::new();
        schema
            .update_table(
                update_table(
                    1,
                    vec![
                        column(10, "duelist_count"),
                        column(11, "alive_duelist_count"),
                    ],
                ),
                &EventContext::default(),
                &mut update_queries,
            )
            .unwrap();

        assert!(update_queries
            .iter()
            .any(|query| query.contains("ALTER TABLE") && query.contains("alive_duelist_count")));
        assert!(schema.tables[&Felt::from(1_u64)]
            .columns
            .values()
            .any(|column| column.name == "alive_duelist_count"));
    }
}
