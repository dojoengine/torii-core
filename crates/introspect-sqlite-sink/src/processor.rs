use crate::json::SqliteJsonSerializer;
use crate::table::{SqliteTable, SqliteTableError};
use crate::INTROSPECT_SQLITE_SINK_MIGRATIONS;
use introspect_types::{PrimaryTypeDef, TypeDef};
use serde_json::{Serializer as JsonSerializer, Value};
use sqlx::Error as SqlxError;
use sqlx::Row;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::{PoisonError, RwLock};
use torii::etl::envelope::MetaData;
use torii::etl::EventMsg;
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

fn sqlite_column_type(type_def: &TypeDef) -> &'static str {
    if is_json_type(type_def) {
        "JSONB"
    } else if matches!(
        type_def,
        TypeDef::Bool
            | TypeDef::I8
            | TypeDef::I16
            | TypeDef::I32
            | TypeDef::U8
            | TypeDef::U16
            | TypeDef::U32
    ) {
        "INTEGER"
    } else {
        "TEXT"
    }
}

fn sqlite_primary_type(type_def: &PrimaryTypeDef) -> &'static str {
    if matches!(
        type_def,
        PrimaryTypeDef::Bool
            | PrimaryTypeDef::I8
            | PrimaryTypeDef::I16
            | PrimaryTypeDef::I32
            | PrimaryTypeDef::U8
            | PrimaryTypeDef::U16
            | PrimaryTypeDef::U32
    ) {
        "INTEGER"
    } else {
        "TEXT"
    }
}

fn is_json_type(type_def: &TypeDef) -> bool {
    matches!(
        type_def,
        TypeDef::Struct(_)
            | TypeDef::Enum(_)
            | TypeDef::Tuple(_)
            | TypeDef::Array(_)
            | TypeDef::FixedArray(_)
            | TypeDef::Option(_)
            | TypeDef::Nullable(_)
            | TypeDef::Result(_)
    )
}

fn create_table_query(table: &SqliteTable) -> String {
    let primary_type = sqlite_primary_type(&table.primary.type_def);
    let mut columns = Vec::with_capacity(table.columns.len() + 1);
    columns.push(format!(
        r#""{}" {primary_type} PRIMARY KEY"#,
        table.primary.name
    ));
    for column_id in &table.order {
        let column = &table.columns[column_id];
        let col_type = sqlite_column_type(&column.type_def);
        columns.push(format!(r#""{}" {col_type}"#, column.name));
    }
    format!(
        r#"CREATE TABLE IF NOT EXISTS "{}" ({});"#,
        table.storage_name,
        columns.join(", ")
    )
}

enum SqliteBindValue {
    Null,
    Integer(i64),
    Text(String),
}

fn to_bind_value(value: &Value, type_def: &TypeDef) -> SqliteBindValue {
    if value.is_null() {
        return SqliteBindValue::Null;
    }

    match type_def {
        TypeDef::Bool => match value.as_bool() {
            Some(b) => SqliteBindValue::Integer(i64::from(b)),
            None => SqliteBindValue::Null,
        },
        TypeDef::I8 | TypeDef::I16 | TypeDef::I32 | TypeDef::U8 | TypeDef::U16 | TypeDef::U32 => {
            match value.as_i64() {
                Some(n) => SqliteBindValue::Integer(n),
                None => SqliteBindValue::Null,
            }
        }
        TypeDef::U64 => match value.as_u64() {
            Some(n) => SqliteBindValue::Text(format!("0x{n:x}")),
            None => match value.as_str() {
                Some(s) => SqliteBindValue::Text(s.to_string()),
                None => SqliteBindValue::Null,
            },
        },
        TypeDef::I64 => match value.as_i64() {
            Some(n) => SqliteBindValue::Text(format!("0x{n:x}")),
            None => match value.as_str() {
                Some(s) => SqliteBindValue::Text(s.to_string()),
                None => SqliteBindValue::Null,
            },
        },

        TypeDef::Felt252
        | TypeDef::ClassHash
        | TypeDef::ContractAddress
        | TypeDef::StorageAddress
        | TypeDef::StorageBaseAddress
        | TypeDef::EthAddress
        | TypeDef::U256
        | TypeDef::U512 => match value.as_str() {
            Some(s) => SqliteBindValue::Text(s.to_string()),
            None => SqliteBindValue::Null,
        },

        TypeDef::U128 => match value.as_u64() {
            Some(n) => SqliteBindValue::Text(format!("0x{:032x}", n as u128)),
            None => match value.as_str() {
                Some(s) => match s.parse::<u128>() {
                    Ok(n) => SqliteBindValue::Text(format!("0x{n:032x}")),
                    Err(_) => SqliteBindValue::Text(s.to_string()),
                },
                None => SqliteBindValue::Null,
            },
        },
        TypeDef::I128 => match value.as_i64() {
            Some(n) => SqliteBindValue::Text(format!("{}", n as i128)),
            None => match value.as_str() {
                Some(s) => SqliteBindValue::Text(s.to_string()),
                None => SqliteBindValue::Null,
            },
        },

        TypeDef::Struct(_)
        | TypeDef::Enum(_)
        | TypeDef::Tuple(_)
        | TypeDef::Array(_)
        | TypeDef::FixedArray(_)
        | TypeDef::Option(_)
        | TypeDef::Nullable(_)
        | TypeDef::Result(_) => SqliteBindValue::Text(value.to_string()),

        _ => match value {
            Value::String(s) => SqliteBindValue::Text(s.clone()),
            _ => SqliteBindValue::Text(value.to_string()),
        },
    }
}

fn primary_to_bind_value(value: &Value, type_def: &PrimaryTypeDef) -> SqliteBindValue {
    if value.is_null() {
        return SqliteBindValue::Null;
    }

    match type_def {
        PrimaryTypeDef::Bool => match value.as_bool() {
            Some(b) => SqliteBindValue::Integer(i64::from(b)),
            None => SqliteBindValue::Null,
        },
        PrimaryTypeDef::I8
        | PrimaryTypeDef::I16
        | PrimaryTypeDef::I32
        | PrimaryTypeDef::U8
        | PrimaryTypeDef::U16
        | PrimaryTypeDef::U32 => match value.as_i64() {
            Some(n) => SqliteBindValue::Integer(n),
            None => SqliteBindValue::Null,
        },
        PrimaryTypeDef::U64 => match value.as_u64() {
            Some(n) => SqliteBindValue::Text(format!("0x{n:x}")),
            None => match value.as_str() {
                Some(s) => SqliteBindValue::Text(s.to_string()),
                None => SqliteBindValue::Null,
            },
        },
        PrimaryTypeDef::I64 => match value.as_i64() {
            Some(n) => SqliteBindValue::Text(format!("0x{n:x}")),
            None => match value.as_str() {
                Some(s) => SqliteBindValue::Text(s.to_string()),
                None => SqliteBindValue::Null,
            },
        },

        PrimaryTypeDef::Felt252
        | PrimaryTypeDef::ClassHash
        | PrimaryTypeDef::ContractAddress
        | PrimaryTypeDef::StorageAddress
        | PrimaryTypeDef::StorageBaseAddress
        | PrimaryTypeDef::EthAddress => match value.as_str() {
            Some(s) => SqliteBindValue::Text(s.to_string()),
            None => SqliteBindValue::Null,
        },

        PrimaryTypeDef::U128 => match value.as_u64() {
            Some(n) => SqliteBindValue::Text(format!("0x{:032x}", n as u128)),
            None => match value.as_str() {
                Some(s) => match s.parse::<u128>() {
                    Ok(n) => SqliteBindValue::Text(format!("0x{n:032x}")),
                    Err(_) => SqliteBindValue::Text(s.to_string()),
                },
                None => SqliteBindValue::Null,
            },
        },
        PrimaryTypeDef::I128 => match value.as_i64() {
            Some(n) => SqliteBindValue::Text(format!("{}", n as i128)),
            None => match value.as_str() {
                Some(s) => SqliteBindValue::Text(s.to_string()),
                None => SqliteBindValue::Null,
            },
        },

        _ => match value {
            Value::String(s) => SqliteBindValue::Text(s.clone()),
            _ => SqliteBindValue::Text(value.to_string()),
        },
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

    async fn update_table(&self, event: impl Into<TableSchema>) -> SqliteDbResult<()> {
        let table_schema: TableSchema = event.into();
        let id = table_schema.id;
        let exists_in_memory = self.tables.read()?.contains_key(&id);

        if !exists_in_memory {
            let (_, query) = self
                .tables
                .create_table(&self.namespace, table_schema.clone())?;
            self.execute_queries(&[query]).await?;
            self.persist_table_state(&table_schema, true).await?;
            return Ok(());
        }

        let (old_columns, storage_name) = {
            let tables = self.tables.read()?;
            let old = tables.get(&id).unwrap();
            (old.columns.clone(), old.storage_name.clone())
        };

        let (_, new_table) =
            SqliteTable::new_from_table(self.namespace.prefix(), table_schema.clone());

        let mut alter_queries = Vec::new();
        for (col_id, col_info) in &new_table.columns {
            if !old_columns.contains_key(col_id) {
                let col_type = sqlite_column_type(&col_info.type_def);
                alter_queries.push(format!(
                    r#"ALTER TABLE "{storage_name}" ADD COLUMN "{}" {col_type}"#,
                    col_info.name
                ));
            }
        }

        if !alter_queries.is_empty() {
            self.execute_queries(&alter_queries).await?;
        }

        self.tables.write()?.insert(id, new_table);
        self.persist_table_state(&table_schema, true).await?;
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
            IntrospectMsg::UpdateTable(event) => self.update_table(event.clone()).await,
            IntrospectMsg::AddColumns(event) => {
                tracing::warn!(
                    target: "torii::introspect_sqlite_sink",
                    table = %event.table,
                    "AddColumns received — table kept alive, new columns ignored until next UpdateTable"
                );
                Ok(())
            }
            IntrospectMsg::DropColumns(event) => {
                tracing::warn!(
                    target: "torii::introspect_sqlite_sink",
                    table = %event.table,
                    "DropColumns received — table kept alive, columns left in place"
                );
                Ok(())
            }
            IntrospectMsg::RetypeColumns(event) => {
                tracing::warn!(
                    target: "torii::introspect_sqlite_sink",
                    table = %event.table,
                    "RetypeColumns received — table kept alive, types unchanged"
                );
                Ok(())
            }
            IntrospectMsg::RetypePrimary(event) => {
                tracing::warn!(
                    target: "torii::introspect_sqlite_sink",
                    table = %event.table,
                    "RetypePrimary received — table kept alive, primary type unchanged"
                );
                Ok(())
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
            let result = self.process_message(msg, metadata).await;
            if let Err(ref err) = result {
                tracing::warn!(
                    target: "torii::introspect_sqlite_sink",
                    event_id = msg.event_id(),
                    error = %err,
                    "Failed to process introspect message"
                );
            }
            results.push(result);
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

        let column_type_defs: Vec<&TypeDef> = event
            .columns
            .iter()
            .map(|id| &table.columns[id].type_def)
            .collect();

        let placeholders = std::iter::once("?".to_string())
            .chain(column_type_defs.iter().map(|td| {
                if is_json_type(td) {
                    "jsonb(?)".to_string()
                } else {
                    "?".to_string()
                }
            }))
            .collect::<Vec<_>>()
            .join(", ");

        let update_columns = column_names
            .iter()
            .skip(1)
            .zip(column_type_defs.iter())
            .map(|(name, td)| {
                if is_json_type(td) {
                    format!(
                        r#""{name}" = COALESCE(jsonb(excluded."{name}"), "{table_name}"."{name}")"#,
                        table_name = table.storage_name
                    )
                } else {
                    format!(
                        r#""{name}" = COALESCE(excluded."{name}", "{table_name}"."{name}")"#,
                        table_name = table.storage_name
                    )
                }
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
            placeholders,
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

            let primary_value = object
                .get(table.primary.name.as_str())
                .cloned()
                .unwrap_or(Value::Null);
            match primary_to_bind_value(&primary_value, &table.primary.type_def) {
                SqliteBindValue::Null => {
                    query = query.bind(None::<Vec<u8>>);
                }
                SqliteBindValue::Integer(n) => {
                    query = query.bind(n);
                }
                SqliteBindValue::Text(s) => {
                    query = query.bind(s);
                }
            }

            for (column_name, type_def) in column_names.iter().skip(1).zip(column_type_defs.iter())
            {
                let val = object.get(*column_name).cloned().unwrap_or(Value::Null);
                match to_bind_value(&val, type_def) {
                    SqliteBindValue::Null => {
                        query = query.bind(None::<String>);
                    }
                    SqliteBindValue::Integer(n) => {
                        query = query.bind(n);
                    }
                    SqliteBindValue::Text(s) => {
                        query = query.bind(s);
                    }
                }
            }
            query.execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
