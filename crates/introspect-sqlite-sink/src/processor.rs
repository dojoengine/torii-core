use crate::json::SqliteJsonSerializer;
use crate::table::{SqliteTable, SqliteTableError};
use crate::INTROSPECT_SQLITE_SINK_MIGRATIONS;
use serde_json::{Serializer as JsonSerializer, Value};
use sqlx::Error as SqlxError;
use sqlx::Row;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::{PoisonError, RwLock};
use torii::etl::envelope::MetaData;
use torii_introspect::events::{IntrospectBody, IntrospectMsg};
use torii_introspect::schema::TableSchema;
use torii_introspect::InsertsFields;
use torii_sqlite::SqliteConnection;

#[derive(Debug, thiserror::Error)]
pub enum SqliteDbError {
    #[error(transparent)]
    DatabaseError(#[from] SqlxError),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    TableError(#[from] SqliteTableError),
    #[error("record frame must serialize to an object")]
    InvalidRecordFrame,
    #[error("Table with id: {0} already exists, incoming name: {1}, existing name: {2}")]
    TableAlreadyExists(Felt, String, String),
    #[error("Table not found with id: {0}")]
    TableNotFound(Felt),
    #[error("Table poison error: {0}")]
    PoisonError(String),
}

type SqliteDbResult<T> = std::result::Result<T, SqliteDbError>;

impl<T> From<PoisonError<T>> for SqliteDbError {
    fn from(err: PoisonError<T>) -> Self {
        Self::PoisonError(err.to_string())
    }
}

#[derive(Debug, Default)]
pub struct SqliteTables(pub RwLock<HashMap<Felt, SqliteTable>>);

impl Deref for SqliteTables {
    type Target = RwLock<HashMap<Felt, SqliteTable>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Default)]
pub enum SqliteNamespace {
    #[default]
    None,
    Custom(String),
}

impl SqliteNamespace {
    pub fn prefix(&self) -> &str {
        match self {
            Self::None => "",
            Self::Custom(prefix) => prefix,
        }
    }
}

impl From<()> for SqliteNamespace {
    fn from((): ()) -> Self {
        Self::None
    }
}

impl From<String> for SqliteNamespace {
    fn from(value: String) -> Self {
        if value.is_empty() {
            Self::None
        } else {
            Self::Custom(value)
        }
    }
}

impl From<&str> for SqliteNamespace {
    fn from(value: &str) -> Self {
        if value.is_empty() {
            Self::None
        } else {
            Self::Custom(value.to_string())
        }
    }
}

impl Display for SqliteNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("main"),
            Self::Custom(prefix) => f.write_str(prefix),
        }
    }
}

impl SqliteTables {
    pub fn assert_table_not_exists(&self, id: &Felt, name: &str) -> SqliteDbResult<()> {
        match self.read()?.get(id) {
            Some(existing) => Err(SqliteDbError::TableAlreadyExists(
                *id,
                name.to_string(),
                existing.name.clone(),
            )),
            None => Ok(()),
        }
    }

    pub fn create_table(
        &self,
        namespace: &SqliteNamespace,
        to_table: impl Into<TableSchema>,
    ) -> SqliteDbResult<(Felt, String)> {
        let table = to_table.into();
        self.assert_table_not_exists(&table.id, &table.name)?;
        let (id, sqlite_table) = SqliteTable::new_from_table(namespace.prefix(), table);
        let create_query = create_table_query(&sqlite_table);
        self.write()?.insert(id, sqlite_table);
        Ok((id, create_query))
    }

    pub fn set_table_dead(&self, id: &Felt) -> SqliteDbResult<()> {
        if let Some(table) = self.write()?.get_mut(id) {
            table.alive = false;
            return Ok(());
        }
        Err(SqliteDbError::TableNotFound(*id))
    }
}

fn create_table_query(table: &SqliteTable) -> String {
    let mut columns = Vec::with_capacity(table.columns.len() + 1);
    columns.push(format!(r#""{}" TEXT PRIMARY KEY"#, table.primary.name));
    for column_id in &table.order {
        let column = &table.columns[column_id];
        columns.push(format!(r#""{}" TEXT"#, column.name));
    }
    format!(
        r#"CREATE TABLE IF NOT EXISTS "{}" ({});"#,
        table.storage_name,
        columns.join(", ")
    )
}

fn serialize_sqlite_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        _ => Some(value.to_string()),
    }
}

pub struct IntrospectSqliteDb<T> {
    tables: SqliteTables,
    namespace: SqliteNamespace,
    pool: T,
}

impl<T: SqliteConnection> SqliteConnection for IntrospectSqliteDb<T> {
    fn pool(&self) -> &sqlx::SqlitePool {
        self.pool.pool()
    }
}

impl<T: SqliteConnection + Send + Sync> IntrospectSqliteDb<T> {
    pub fn new(pool: T, namespace: impl Into<SqliteNamespace>) -> Self {
        Self {
            tables: SqliteTables::default(),
            namespace: namespace.into(),
            pool,
        }
    }

    pub async fn initialize_introspect_sqlite_sink(&self) -> SqliteDbResult<()> {
        self.migrate(Some("introspect"), INTROSPECT_SQLITE_SINK_MIGRATIONS)
            .await?;
        self.load_persisted_state().await?;
        Ok(())
    }

    async fn load_persisted_state(&self) -> SqliteDbResult<()> {
        let rows = sqlx::query(
            r"
            SELECT table_schema_json, alive
            FROM introspect_sink_schema_state
            ORDER BY updated_at ASC
            ",
        )
        .fetch_all(self.pool())
        .await?;

        let mut tables = self.tables.write()?;
        for row in rows {
            let schema_json: String = row.try_get("table_schema_json")?;
            let alive: i64 = row.try_get("alive")?;
            let table_schema: TableSchema = serde_json::from_str(&schema_json)?;
            let (id, mut table) =
                SqliteTable::new_from_table(self.namespace.prefix(), table_schema);
            table.alive = alive != 0;
            tables.insert(id, table);
        }

        Ok(())
    }

    async fn persist_table_state(&self, table: &TableSchema, alive: bool) -> SqliteDbResult<()> {
        let schema_json = serde_json::to_string(table)?;
        let alive = i64::from(alive);
        sqlx::query(
            r"
            INSERT INTO introspect_sink_schema_state (table_id, table_schema_json, alive, updated_at)
            VALUES (?1, ?2, ?3, unixepoch())
            ON CONFLICT (table_id)
            DO UPDATE SET
                table_schema_json = excluded.table_schema_json,
                alive = excluded.alive,
                updated_at = unixepoch()
            ",
        )
        .bind(format!("{:#x}", table.id))
        .bind(schema_json)
        .bind(alive)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn persist_alive_state(&self, table_id: Felt, alive: bool) -> SqliteDbResult<()> {
        let alive = i64::from(alive);
        sqlx::query(
            r"
            UPDATE introspect_sink_schema_state
            SET alive = ?1, updated_at = unixepoch()
            WHERE table_id = ?2
            ",
        )
        .bind(alive)
        .bind(format!("{table_id:#x}"))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub fn load_tables_no_commit(&self, table_schemas: Vec<TableSchema>) -> SqliteDbResult<()> {
        let mut tables = self.tables.write()?;
        for table in table_schemas {
            let (id, sqlite_table) = SqliteTable::new_from_table(self.namespace.prefix(), table);
            tables.insert(id, sqlite_table);
        }
        Ok(())
    }

    pub async fn process_message(
        &self,
        msg: &IntrospectMsg,
        metadata: &MetaData,
    ) -> SqliteDbResult<()> {
        match msg {
            IntrospectMsg::CreateTable(event) => {
                let (_, query) = self.tables.create_table(&self.namespace, event.clone())?;
                self.execute_queries(&[query]).await?;
                self.persist_table_state(&event.clone().into(), true)
                    .await?;
                Ok(())
            }
            IntrospectMsg::UpdateTable(event) => {
                self.tables.set_table_dead(&event.id)?;
                self.persist_alive_state(event.id, false).await
            }
            IntrospectMsg::AddColumns(event) => {
                self.tables.set_table_dead(&event.table)?;
                self.persist_alive_state(event.table, false).await
            }
            IntrospectMsg::DropColumns(event) => {
                self.tables.set_table_dead(&event.table)?;
                self.persist_alive_state(event.table, false).await
            }
            IntrospectMsg::RetypeColumns(event) => {
                self.tables.set_table_dead(&event.table)?;
                self.persist_alive_state(event.table, false).await
            }
            IntrospectMsg::RetypePrimary(event) => {
                self.tables.set_table_dead(&event.table)?;
                self.persist_alive_state(event.table, false).await
            }
            IntrospectMsg::RenameTable(_)
            | IntrospectMsg::DropTable(_)
            | IntrospectMsg::RenameColumns(_)
            | IntrospectMsg::RenamePrimary(_)
            | IntrospectMsg::DeleteRecords(_)
            | IntrospectMsg::DeletesFields(_) => Ok(()),
            IntrospectMsg::InsertsFields(event) => self.insert_fields(event, metadata).await,
        }
    }

    pub async fn process_messages(
        &self,
        msgs: Vec<&IntrospectBody>,
    ) -> SqliteDbResult<Vec<SqliteDbResult<()>>> {
        let mut results = Vec::with_capacity(msgs.len());
        for body in msgs {
            let (msg, metadata) = body.into();
            results.push(self.process_message(msg, metadata).await);
        }
        Ok(results)
    }

    async fn insert_fields(
        &self,
        event: &InsertsFields,
        _metadata: &MetaData,
    ) -> SqliteDbResult<()> {
        let table = self
            .tables
            .read()?
            .get(&event.table)
            .ok_or(SqliteDbError::TableNotFound(event.table))?
            .clone();
        if !table.alive {
            return Ok(());
        }

        let record_schema = table.get_schema(&event.columns)?;
        let column_names = std::iter::once(table.primary.name.as_str())
            .chain(
                event
                    .columns
                    .iter()
                    .map(|id| table.columns[id].name.as_str()),
            )
            .collect::<Vec<_>>();

        let update_columns = column_names
            .iter()
            .skip(1)
            .map(|name| {
                format!(
                    r#""{name}" = COALESCE(excluded."{name}", "{table_name}"."{name}")"#,
                    table_name = table.storage_name
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            r#"INSERT INTO "{}" ({}) VALUES ({}) ON CONFLICT("{}") DO UPDATE SET {}"#,
            table.storage_name,
            column_names
                .iter()
                .map(|name| format!(r#""{name}""#))
                .collect::<Vec<_>>()
                .join(", "),
            vec!["?"; column_names.len()].join(", "),
            table.primary.name,
            update_columns
        );

        let mut bytes = Vec::new();
        let mut serializer = JsonSerializer::new(&mut bytes);
        record_schema.parse_records_with_metadata(
            &event.records,
            &(),
            &mut serializer,
            &SqliteJsonSerializer,
        )?;
        let rows = serde_json::from_slice::<Vec<Value>>(&bytes)?;

        let mut tx = self.begin().await?;
        for value in rows {
            let object = value.as_object().ok_or(SqliteDbError::InvalidRecordFrame)?;

            let mut query = sqlx::query(&sql);
            for column_name in &column_names {
                let value = object.get(*column_name).and_then(serialize_sqlite_value);
                query = query.bind(value);
            }
            query.execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
