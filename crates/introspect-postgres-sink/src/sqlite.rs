use anyhow::Context;
use introspect_types::serialize::ToCairoDeSeFrom;
use introspect_types::serialize_def::CairoTypeSerialization;
use introspect_types::{
    CairoDeserializer, ColumnDef, PrimaryDef, PrimaryTypeDef, ResultDef, TupleDef, TypeDef,
};
use serde::ser::SerializeMap;
use serde::Serializer;
use serde_json::{Serializer as JsonSerializer, Value};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};
use starknet_types_core::felt::Felt;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use thiserror::Error;
use torii::etl::EventContext;
use torii_introspect::events::{InsertsFields, IntrospectMsg};
use torii_introspect::{CreateTable, UpdateTable};

#[derive(Debug, Error)]
pub enum SqliteDbError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("table with id {0:#x} not found")]
    TableNotFound(Felt),
    #[error("table with id {0:#x} is no longer live")]
    TableNotAlive(Felt),
    #[error("column with id {column_id:#x} not found in table {table_name}")]
    ColumnNotFound { table_name: String, column_id: Felt },
    #[error("table with id {0:#x} already exists as {2}, incoming name {1}")]
    TableAlreadyExists(Felt, String, String),
    #[error("record missing column {0}")]
    MissingColumn(String),
}

type SqliteResult<T> = Result<T, SqliteDbError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqliteAffinity {
    Integer,
    Text,
}

impl SqliteAffinity {
    fn sql_type(self) -> &'static str {
        match self {
            Self::Integer => "INTEGER",
            Self::Text => "TEXT",
        }
    }
}

#[derive(Debug)]
struct SqliteTable {
    name: String,
    primary: PrimaryDef,
    columns: HashMap<Felt, ColumnDef>,
    alive: bool,
}

impl SqliteTable {
    fn new(event: CreateTable) -> (Felt, Self) {
        let columns = event
            .columns
            .into_iter()
            .map(|column| (column.id, column))
            .collect();

        (
            event.id,
            Self {
                name: event.name,
                primary: event.primary,
                columns,
                alive: true,
            },
        )
    }

    fn get_living_column(&self, id: &Felt) -> SqliteResult<&ColumnDef> {
        self.columns
            .get(id)
            .ok_or_else(|| SqliteDbError::ColumnNotFound {
                table_name: self.name.clone(),
                column_id: *id,
            })
    }
}

#[derive(Debug, Default)]
struct SqliteSchema {
    tables: HashMap<Felt, SqliteTable>,
}

impl SqliteSchema {
    fn get_living_table(&self, id: &Felt) -> SqliteResult<&SqliteTable> {
        let table = self
            .tables
            .get(id)
            .ok_or(SqliteDbError::TableNotFound(*id))?;
        if table.alive {
            Ok(table)
        } else {
            Err(SqliteDbError::TableNotAlive(*id))
        }
    }

    fn create_table_sql(&mut self, event: CreateTable) -> SqliteResult<(String, String)> {
        if let Some(existing) = self.tables.get(&event.id) {
            if existing.name == event.name {
                return Ok((existing.name.clone(), String::new()));
            }
            return Err(SqliteDbError::TableAlreadyExists(
                event.id,
                event.name,
                existing.name.clone(),
            ));
        }

        let table_name = event.name.clone();
        let create_sql = build_create_table_sql(&event);
        let (id, table) = SqliteTable::new(event);
        self.tables.insert(id, table);
        Ok((table_name, create_sql))
    }

    fn update_table_sql(&mut self, event: UpdateTable) -> SqliteResult<(String, Vec<String>)> {
        let snapshot = CreateTable {
            id: event.id,
            name: event.name,
            attributes: event.attributes,
            primary: event.primary,
            columns: event.columns,
        };

        let Some(existing) = self.tables.get(&snapshot.id) else {
            let (table_name, create_sql) = self.create_table_sql(snapshot)?;
            return Ok((table_name, vec![create_sql]));
        };

        if existing.name != snapshot.name {
            return Err(SqliteDbError::TableAlreadyExists(
                snapshot.id,
                snapshot.name,
                existing.name.clone(),
            ));
        }

        let mut alterations = Vec::new();
        for column in &snapshot.columns {
            if !existing.columns.contains_key(&column.id) {
                alterations.push(build_add_column_sql(&existing.name, column));
            }
        }

        let table_name = existing.name.clone();
        let (_, replacement) = SqliteTable::new(snapshot);
        self.tables.insert(event.id, replacement);

        Ok((table_name, alterations))
    }

    fn set_table_dead(&mut self, id: &Felt) -> SqliteResult<()> {
        let table = self
            .tables
            .get_mut(id)
            .ok_or(SqliteDbError::TableNotFound(*id))?;
        table.alive = false;
        Ok(())
    }

    fn insert_rows(&self, event: &InsertsFields) -> SqliteResult<(String, Vec<SqliteRow>)> {
        let table = self.get_living_table(&event.table)?;
        let records = sqlite_records_for_event(table, event)?;
        Ok((table.name.clone(), records))
    }
}

#[derive(Debug)]
struct SqliteRow {
    columns: Vec<String>,
    primary: String,
    values: Vec<(SqliteAffinity, Value)>,
}

pub struct SqliteSimpleDb {
    schema: SqliteSchema,
    pool: SqlitePool,
}

impl SqliteSimpleDb {
    pub async fn new(database_url: &str, max_connections: Option<u32>) -> SqliteResult<Self> {
        let options = sqlite_connect_options(database_url)?;
        if let Some(parent) = sqlite_parent_dir(&options) {
            std::fs::create_dir_all(parent).map_err(sqlx::Error::Io)?;
        }

        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections.unwrap_or(1))
            .connect_with(options)
            .await?;

        Ok(Self {
            schema: SqliteSchema::default(),
            pool,
        })
    }

    pub async fn initialize(&mut self) -> SqliteResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS torii_introspect_schema_state (
                table_id TEXT PRIMARY KEY,
                create_table_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL DEFAULT (unixepoch())
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        let tables = self.load_stored_tables().await?;
        if !tables.is_empty() {
            self.bootstrap_tables(&tables).await?;
            tracing::info!(
                target: "torii::sinks::introspect::sqlite",
                tables = tables.len(),
                "Loaded sink schema state from SQLite storage"
            );
        }

        Ok(())
    }

    pub fn has_tables(&self) -> bool {
        !self.schema.tables.is_empty()
    }

    pub async fn bootstrap_tables(&mut self, tables: &[CreateTable]) -> SqliteResult<()> {
        let context = EventContext::default();
        for table in tables {
            self.process_message(&IntrospectMsg::CreateTable(table.clone()), &context)
                .await?;
            self.reconcile_table_columns(&table.name, &table.columns)
                .await?;
        }
        Ok(())
    }

    pub async fn process_message(
        &mut self,
        msg: &IntrospectMsg,
        _context: &EventContext,
    ) -> SqliteResult<()> {
        match msg {
            IntrospectMsg::CreateTable(table) => {
                let (table_name, create_sql) = self.schema.create_table_sql(table.clone())?;
                if !create_sql.is_empty() {
                    sqlx::query(&create_sql).execute(&self.pool).await?;
                }
                self.reconcile_table_columns(&table_name, &table.columns)
                    .await?;
                self.persist_table_state(table).await?;
            }
            IntrospectMsg::UpdateTable(table) => {
                let (table_name, alter_sql) = self.schema.update_table_sql(table.clone())?;
                for sql in alter_sql {
                    sqlx::query(&sql).execute(&self.pool).await?;
                }
                self.reconcile_table_columns(&table_name, &table.columns)
                    .await?;
                self.persist_table_state(&CreateTable {
                    id: table.id,
                    name: table.name.clone(),
                    attributes: table.attributes.clone(),
                    primary: table.primary.clone(),
                    columns: table.columns.clone(),
                })
                .await?;
            }
            IntrospectMsg::AddColumns(event) => {
                self.schema.set_table_dead(&event.table)?;
            }
            IntrospectMsg::DropColumns(event) => {
                self.schema.set_table_dead(&event.table)?;
            }
            IntrospectMsg::RetypeColumns(event) => {
                self.schema.set_table_dead(&event.table)?;
            }
            IntrospectMsg::RetypePrimary(event) => {
                self.schema.set_table_dead(&event.table)?;
            }
            IntrospectMsg::InsertsFields(event) => {
                let (table_name, rows) = self.schema.insert_rows(event)?;
                let insert_sql = rows
                    .first()
                    .map(|row| build_insert_row_sql(&table_name, row));
                for row in rows {
                    self.insert_row(insert_sql.as_deref(), row).await?;
                }
            }
            IntrospectMsg::RenameTable(_)
            | IntrospectMsg::DropTable(_)
            | IntrospectMsg::RenameColumns(_)
            | IntrospectMsg::RenamePrimary(_)
            | IntrospectMsg::DeleteRecords(_)
            | IntrospectMsg::DeletesFields(_) => {}
        }

        Ok(())
    }

    async fn reconcile_table_columns(
        &self,
        table_name: &str,
        columns: &[ColumnDef],
    ) -> SqliteResult<()> {
        let existing = self.table_columns(table_name).await?;
        for column in columns {
            if existing.contains(&column.name) {
                continue;
            }

            let sql = build_add_column_sql(table_name, column);
            sqlx::query(&sql).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn table_columns(&self, table_name: &str) -> SqliteResult<HashSet<String>> {
        let pragma = format!("PRAGMA table_info({})", quote_ident(table_name));
        let rows = sqlx::query(&pragma).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| row.try_get::<String, _>("name").ok())
            .collect())
    }

    async fn load_stored_tables(&self) -> SqliteResult<Vec<CreateTable>> {
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

    async fn persist_table_state(&self, table: &CreateTable) -> SqliteResult<()> {
        let json = serde_json::to_string(table)?;
        sqlx::query(
            r#"
            INSERT INTO torii_introspect_schema_state (table_id, create_table_json, updated_at)
            VALUES (?1, ?2, unixepoch())
            ON CONFLICT (table_id)
            DO UPDATE SET
                create_table_json = excluded.create_table_json,
                updated_at = unixepoch()
            "#,
        )
        .bind(felt_key(&table.id))
        .bind(json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn insert_row(&self, sql: Option<&str>, row: SqliteRow) -> SqliteResult<()> {
        let Some(sql) = sql else {
            return Ok(());
        };
        let mut query = sqlx::query(sql).persistent(true);
        for (affinity, value) in row.values {
            query = bind_value(query, affinity, value);
        }
        query.execute(&self.pool).await?;
        Ok(())
    }
}

fn build_insert_row_sql(table_name: &str, row: &SqliteRow) -> String {
    let column_names = row
        .columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; row.columns.len()].join(", ");
    let conflict_sql = row
        .columns
        .iter()
        .filter(|column| *column != &row.primary)
        .map(|column| {
            format!(
                "{column_ident} = COALESCE(excluded.{column_ident}, {table_ident}.{column_ident})",
                column_ident = quote_ident(column),
                table_ident = quote_ident(table_name),
            )
        })
        .collect::<Vec<_>>();

    if conflict_sql.is_empty() {
        format!(
            "INSERT INTO {table_ident} ({column_names}) VALUES ({placeholders}) \
             ON CONFLICT ({primary}) DO NOTHING",
            table_ident = quote_ident(table_name),
            primary = quote_ident(&row.primary),
        )
    } else {
        format!(
            "INSERT INTO {table_ident} ({column_names}) VALUES ({placeholders}) \
             ON CONFLICT ({primary}) DO UPDATE SET {updates}",
            table_ident = quote_ident(table_name),
            primary = quote_ident(&row.primary),
            updates = conflict_sql.join(", "),
        )
    }
}

fn sqlite_records_for_event(
    table: &SqliteTable,
    event: &InsertsFields,
) -> SqliteResult<Vec<SqliteRow>> {
    let schema = torii_introspect::tables::RecordSchema::new(
        &table.primary,
        event
            .columns
            .iter()
            .map(|column_id| table.get_living_column(column_id))
            .collect::<SqliteResult<Vec<_>>>()?,
    );

    let mut bytes = Vec::new();
    let mut serializer = JsonSerializer::new(&mut bytes);
    schema.parse_records_with_metadata(
        &event.records,
        &(),
        &mut serializer,
        &SqliteJsonSerializer,
    )?;

    let rows = serde_json::from_slice::<Vec<Value>>(&bytes)?;
    rows.into_iter()
        .map(|row| sqlite_row_from_value(&schema, row))
        .collect()
}

fn sqlite_row_from_value(
    schema: &torii_introspect::tables::RecordSchema<'_>,
    row: Value,
) -> SqliteResult<SqliteRow> {
    let object = row
        .as_object()
        .cloned()
        .context("serialized record must be an object")
        .map_err(|err| SqliteDbError::Json(serde_json::Error::io(std::io::Error::other(err))))?;

    let mut columns = Vec::with_capacity(1 + schema.columns().len());
    let mut values = Vec::with_capacity(1 + schema.columns().len());

    columns.push(schema.primary().name.clone());
    values.push((
        affinity_for_primary(&schema.primary().type_def),
        object
            .get(&schema.primary().name)
            .cloned()
            .ok_or_else(|| SqliteDbError::MissingColumn(schema.primary().name.clone()))?,
    ));

    for column in schema.columns() {
        columns.push(column.name.clone());
        values.push((
            affinity_for_type(&column.type_def),
            object
                .get(&column.name)
                .cloned()
                .ok_or_else(|| SqliteDbError::MissingColumn(column.name.clone()))?,
        ));
    }

    Ok(SqliteRow {
        columns,
        primary: schema.primary().name.clone(),
        values,
    })
}

fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    affinity: SqliteAffinity,
    value: Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match affinity {
        SqliteAffinity::Integer => match value {
            Value::Bool(value) => query.bind(i64::from(value)),
            Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    query.bind(value)
                } else if let Some(value) = number.as_u64() {
                    match i64::try_from(value) {
                        Ok(value) => query.bind(value),
                        Err(_) => query.bind(number.to_string()),
                    }
                } else {
                    query.bind(number.to_string())
                }
            }
            Value::String(value) => query.bind(value),
            Value::Null => query.bind(Option::<i64>::None),
            other => query.bind(other.to_string()),
        },
        SqliteAffinity::Text => match value {
            Value::Null => query.bind(Option::<String>::None),
            Value::String(value) => query.bind(value),
            other => query.bind(other.to_string()),
        },
    }
}

fn build_create_table_sql(event: &CreateTable) -> String {
    let mut columns = Vec::with_capacity(1 + event.columns.len());
    columns.push(format!(
        "{} {} PRIMARY KEY",
        quote_ident(&event.primary.name),
        affinity_for_primary(&event.primary.type_def).sql_type()
    ));
    columns.extend(event.columns.iter().map(|column| {
        format!(
            "{} {}",
            quote_ident(&column.name),
            affinity_for_type(&column.type_def).sql_type()
        )
    }));

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_ident(&event.name),
        columns.join(", ")
    )
}

fn build_add_column_sql(table_name: &str, column: &ColumnDef) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        quote_ident(table_name),
        quote_ident(&column.name),
        affinity_for_type(&column.type_def).sql_type()
    )
}

fn affinity_for_primary(type_def: &PrimaryTypeDef) -> SqliteAffinity {
    match type_def {
        PrimaryTypeDef::Bool
        | PrimaryTypeDef::I8
        | PrimaryTypeDef::I16
        | PrimaryTypeDef::I32
        | PrimaryTypeDef::I64
        | PrimaryTypeDef::U8
        | PrimaryTypeDef::U16
        | PrimaryTypeDef::U32 => SqliteAffinity::Integer,
        PrimaryTypeDef::U64
        | PrimaryTypeDef::U128
        | PrimaryTypeDef::I128
        | PrimaryTypeDef::Felt252
        | PrimaryTypeDef::ContractAddress
        | PrimaryTypeDef::ClassHash
        | PrimaryTypeDef::StorageAddress
        | PrimaryTypeDef::StorageBaseAddress
        | PrimaryTypeDef::EthAddress
        | PrimaryTypeDef::ShortUtf8
        | PrimaryTypeDef::Bytes31
        | PrimaryTypeDef::Bytes31Encoded(_) => SqliteAffinity::Text,
    }
}

fn affinity_for_type(type_def: &TypeDef) -> SqliteAffinity {
    match type_def {
        TypeDef::Bool
        | TypeDef::I8
        | TypeDef::I16
        | TypeDef::I32
        | TypeDef::I64
        | TypeDef::U8
        | TypeDef::U16
        | TypeDef::U32 => SqliteAffinity::Integer,
        TypeDef::None
        | TypeDef::U64
        | TypeDef::U128
        | TypeDef::I128
        | TypeDef::U256
        | TypeDef::U512
        | TypeDef::Felt252
        | TypeDef::ContractAddress
        | TypeDef::ClassHash
        | TypeDef::StorageAddress
        | TypeDef::StorageBaseAddress
        | TypeDef::EthAddress
        | TypeDef::Utf8String
        | TypeDef::ShortUtf8
        | TypeDef::ByteArray
        | TypeDef::ByteArrayEncoded(_)
        | TypeDef::Bytes31
        | TypeDef::Bytes31Encoded(_)
        | TypeDef::Tuple(_)
        | TypeDef::Enum(_)
        | TypeDef::Array(_)
        | TypeDef::FixedArray(_)
        | TypeDef::Struct(_)
        | TypeDef::Option(_)
        | TypeDef::Nullable(_)
        | TypeDef::Felt252Dict(_)
        | TypeDef::Result(_)
        | TypeDef::Ref(_)
        | TypeDef::Custom(_) => SqliteAffinity::Text,
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn felt_key(value: &Felt) -> String {
    format!("{value:#x}")
}

struct SqliteJsonSerializer;

fn is_sqlite_memory_path(path: &str) -> bool {
    path == ":memory:"
        || path == "sqlite::memory:"
        || path == "sqlite://:memory:"
        || path.contains("mode=memory")
}

fn sqlite_connect_options(path: &str) -> Result<SqliteConnectOptions, sqlx::Error> {
    if path == ":memory:" || path == "sqlite::memory:" {
        return SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|err| sqlx::Error::Configuration(Box::new(err)));
    }

    let options = if path.starts_with("sqlite:") {
        SqliteConnectOptions::from_str(path)
            .map_err(|err| sqlx::Error::Configuration(Box::new(err)))?
    } else {
        SqliteConnectOptions::new().filename(path)
    };

    if path.starts_with("sqlite:") && path.contains("mode=") {
        Ok(options)
    } else {
        Ok(options.create_if_missing(true))
    }
}

fn sqlite_parent_dir(options: &SqliteConnectOptions) -> Option<PathBuf> {
    let filename = options.get_filename();
    if is_sqlite_memory_path(filename.to_string_lossy().as_ref()) {
        return None;
    }
    filename
        .parent()
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

impl CairoTypeSerialization for SqliteJsonSerializer {
    fn serialize_byte_array<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8],
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("0x{}", hex::encode(value)))
    }

    fn serialize_felt<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8; 32],
    ) -> Result<S::Ok, S::Error> {
        self.serialize_byte_array(serializer, value)
    }

    fn serialize_eth_address<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8; 20],
    ) -> Result<S::Ok, S::Error> {
        self.serialize_byte_array(serializer, value)
    }

    fn serialize_tuple<'a, S: Serializer>(
        &'a self,
        data: &mut impl CairoDeserializer,
        serializer: S,
        tuple: &'a TupleDef,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_map(Some(tuple.elements.len()))?;
        for (index, element) in tuple.elements.iter().enumerate() {
            seq.serialize_entry(&format!("_{index}"), &element.to_de_se(data, self))?;
        }
        seq.end()
    }

    fn serialize_variant<'a, S: Serializer>(
        &'a self,
        data: &mut impl CairoDeserializer,
        serializer: S,
        name: &str,
        type_def: &'a TypeDef,
    ) -> Result<S::Ok, S::Error> {
        match type_def {
            TypeDef::None => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("variant", name)?;
                map
            }
            _ => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("variant", name)?;
                map.serialize_entry(&format!("_{name}"), &type_def.to_de_se(data, self))?;
                map
            }
        }
        .end()
    }

    fn serialize_result<'a, S: Serializer>(
        &'a self,
        data: &mut impl CairoDeserializer,
        serializer: S,
        result: &'a ResultDef,
        is_ok: bool,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("is_ok", &is_ok)?;
        match is_ok {
            true => map.serialize_entry("Ok", &result.ok.to_de_se(data, self))?,
            false => map.serialize_entry("Err", &result.err.to_de_se(data, self))?,
        }
        map.end()
    }
}
