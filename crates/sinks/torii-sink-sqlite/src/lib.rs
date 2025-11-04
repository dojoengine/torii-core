//! Very simple SQLite sink implementation.
//! TO BE REWORKED properly with transactions and optimisations for speed.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use introspect_types::{ColumnDef, TypeDef};
use introspect_value::{Field, Value};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::{
    sqlite::{SqliteArguments, SqliteConnectOptions, SqlitePoolOptions},
    Sqlite, Transaction,
};
use sqlx::{Arguments, Row, SqlitePool};
use tokio::sync::RwLock;
use torii_core::{
    format::felt_to_padded_hex, Batch, Envelope, Event, FieldElement, Sink, SinkFactory,
    SinkRegistry,
};
use torii_types_erc20::TransferV1 as Erc20Transfer;
use torii_types_erc721::TransferV1 as Erc721Transfer;
use torii_types_introspect::{DeclareTableV1, UpdateRecordFieldsV1};
mod types;
use types::{
    collect_columns, field_values, sanitize_identifier, ColumnInfo, ColumnValue, SqliteType,
};

use crate::types::u256_to_padded_hex;

#[derive(Clone)]
struct FieldSchema {
    column: ColumnDef,
    columns: Vec<ColumnInfo>,
}

fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn push_argument(args: &mut SqliteArguments, value: &ColumnValue, sql_type: SqliteType) {
    match (value, sql_type) {
        (ColumnValue::Null, SqliteType::Integer) => args.add(Option::<i64>::None),
        (ColumnValue::Null, SqliteType::Text) => args.add(Option::<String>::None),
        (ColumnValue::Integer(v), _) => args.add(*v),
        (ColumnValue::Text(v), _) => args.add(v.clone()),
    }
}

fn format_id_field(field: &Field) -> Result<String> {
    match &field.value {
        Value::Felt252(felt)
        | Value::ClassHash(felt)
        | Value::ContractAddress(felt)
        | Value::EthAddress(felt) => Ok(felt_to_padded_hex(felt)),
        Value::ShortString(value) | Value::ByteArray(value) => Ok(value.clone()),
        Value::U64(value) => Ok(value.to_string()),
        Value::U128(value) => Ok(value.to_string()),
        Value::I128(value) => Ok(value.to_string()),
        Value::USize(value) => Ok(value.to_string()),
        Value::U256(value) => Ok(u256_to_padded_hex(value)),
        Value::None => Err(anyhow!("id field {} has no value", field.name)),
        other => Ok(serde_json::to_string(other)?),
    }
}

#[derive(Clone)]
struct TableSchema {
    storage_name: String,
    id_column: ColumnInfo,
    fields: HashMap<String, FieldSchema>,
    declare: DeclareTableV1,
}

impl TableSchema {
    fn from_declare(event: DeclareTableV1, storage_name: String) -> Result<Self> {
        if event.id_field.type_def != TypeDef::Felt252 {
            bail!(
                "only Felt252 id fields are supported, got {:?}",
                event.id_field.type_def
            );
        }
        let id_column = ColumnInfo {
            name: sanitize_identifier(&event.id_field.name),
            sql_type: SqliteType::Text,
        };

        let mut fields = HashMap::new();
        for column in &event.fields {
            let columns = collect_columns(column);
            fields.insert(
                column.name.clone(),
                FieldSchema {
                    column: column.clone(),
                    columns,
                },
            );
        }

        Ok(Self {
            storage_name,
            id_column,
            fields,
            declare: event,
        })
    }

    fn table_name(&self) -> &str {
        &self.storage_name
    }

    fn id_column(&self) -> &ColumnInfo {
        &self.id_column
    }

    fn field(&self, name: &str) -> Option<&FieldSchema> {
        self.fields.get(name)
    }

    fn all_columns(&self) -> Vec<ColumnInfo> {
        let mut seen = HashSet::new();
        let mut columns = Vec::new();
        seen.insert(self.id_column.name.clone());
        columns.push(self.id_column.clone());

        for field in self.fields.values() {
            for column in &field.columns {
                if seen.insert(column.name.clone()) {
                    columns.push(column.clone());
                }
            }
        }
        columns
    }
}

#[derive(Clone)]
pub struct SqliteSink {
    label: String,
    pool: SqlitePool,
    schemas: Arc<RwLock<HashMap<String, Arc<TableSchema>>>>,
    contract_labels: HashMap<FieldElement, String>,
}

impl SqliteSink {
    pub async fn connect(
        label: impl Into<String>,
        database_url: &str,
        max_connections: Option<u32>,
        contract_labels: HashMap<FieldElement, String>,
    ) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections.unwrap_or(5))
            .connect_with(options)
            .await
            .with_context(|| format!("failed to connect to sqlite database {database_url}"))?;

        let contract_labels = contract_labels
            .into_iter()
            .map(|(address, label)| (address, sanitize_identifier(&label)))
            .collect();

        let sink = Self {
            label: label.into(),
            pool,
            schemas: Arc::new(RwLock::new(HashMap::new())),
            contract_labels,
        };
        sink.ensure_schema().await?;
        Ok(sink)
    }

    fn contract_prefix(&self, address: &FieldElement) -> String {
        self.contract_labels
            .get(address)
            .cloned()
            .unwrap_or_else(|| sanitize_identifier(&felt_to_padded_hex(address)))
    }

    fn storage_table_name(&self, address: &FieldElement, logical_name: &str) -> String {
        let prefix = self.contract_prefix(address);
        let logical = sanitize_table_segment(logical_name);
        if prefix.is_empty() {
            logical
        } else if logical.is_empty() {
            prefix
        } else {
            format!("{prefix}__{logical}")
        }
    }

    async fn ensure_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS introspect_table_schemas (
                name TEXT PRIMARY KEY,
                schema_json TEXT NOT NULL
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

    async fn persist_schema(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        schema: &TableSchema,
    ) -> Result<()> {
        let schema_json = serde_json::to_string(&schema.declare)?;
        sqlx::query(
            r#"
            INSERT INTO introspect_table_schemas (name, schema_json)
            VALUES (?1, ?2)
            ON CONFLICT(name) DO UPDATE SET schema_json = excluded.schema_json;
        "#,
        )
        .bind(schema.table_name())
        .bind(schema_json)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn ensure_table(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        schema: &TableSchema,
    ) -> Result<()> {
        let table_ident = quote_ident(schema.table_name());
        let pk_ident = quote_ident(&schema.id_column().name);
        let columns = schema.all_columns();
        let column_defs = columns
            .iter()
            .map(|column| format!("{} {}", quote_ident(&column.name), column.sql_type.as_sql()))
            .collect::<Vec<_>>()
            .join(", ");

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}))",
            table_ident, column_defs, pk_ident
        );
        sqlx::query(&create_sql).execute(&mut **tx).await?;

        let pragma_sql = format!("PRAGMA table_info({})", table_ident);
        let rows = sqlx::query(&pragma_sql).fetch_all(&mut **tx).await?;
        let mut existing = HashSet::new();
        for row in rows {
            let name: String = row.try_get("name")?;
            existing.insert(name);
        }

        for column in columns {
            if existing.contains(&column.name) {
                continue;
            }
            let alter_sql = format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                table_ident,
                quote_ident(&column.name),
                column.sql_type.as_sql()
            );
            sqlx::query(&alter_sql).execute(&mut **tx).await?;
        }

        Ok(())
    }

    async fn load_schema(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        storage_name: &str,
    ) -> Result<Arc<TableSchema>> {
        if let Some(schema) = self.schemas.read().await.get(storage_name).cloned() {
            return Ok(schema);
        }

        let row = sqlx::query(
            r#"
            SELECT schema_json
            FROM introspect_table_schemas
            WHERE name = ?1
        "#,
        )
        .bind(storage_name)
        .fetch_optional(&mut **tx)
        .await?;

        let row = row.ok_or_else(|| anyhow!("no schema found for table {}", storage_name))?;
        let schema_json: String = row.try_get("schema_json")?;
        let declare: DeclareTableV1 = serde_json::from_str(&schema_json)?;
        let schema = Arc::new(TableSchema::from_declare(
            declare,
            storage_name.to_string(),
        )?);
        self.ensure_table(tx, &schema).await?;

        let mut cache = self.schemas.write().await;
        cache.insert(storage_name.to_string(), schema.clone());
        Ok(schema)
    }

    async fn handle_declare(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        env: &Envelope,
        event: &DeclareTableV1,
    ) -> Result<()> {
        let storage_name = self.storage_table_name(&env.raw.from_address, event.name.as_str());
        let schema = Arc::new(TableSchema::from_declare(
            event.clone(),
            storage_name.clone(),
        )?);
        self.persist_schema(tx, &schema).await?;
        self.ensure_table(tx, &schema).await?;

        {
            let mut cache = self.schemas.write().await;
            cache.insert(storage_name, schema.clone());
        }

        tracing::info!(
            sink = %self.label,
            table = %event.name,
            storage_table = %schema.table_name(),
            block = env.raw.block_number.unwrap_or_default(),
            "stored declare_table"
        );
        Ok(())
    }

    async fn handle_update_record(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        env: &Envelope,
        event: &UpdateRecordFieldsV1,
    ) -> Result<()> {
        let storage_name =
            self.storage_table_name(&env.raw.from_address, event.table_name.as_str());
        let schema = self.load_schema(tx, &storage_name).await?;
        let id_value = format_id_field(&event.id_field)?;

        let column_types: HashMap<_, _> = schema
            .all_columns()
            .into_iter()
            .map(|info| (info.name.clone(), info.sql_type))
            .collect();

        let mut values = BTreeMap::new();
        values.insert(
            schema.id_column().name.clone(),
            ColumnValue::Text(id_value.clone()),
        );

        for field in &event.fields {
            match schema.field(&field.name) {
                Some(field_schema) => {
                    let entries =
                        field_values(&field_schema.column, &field_schema.columns, &field.value)?;
                    for (column_name, column_value) in entries {
                        values.insert(column_name, column_value);
                    }
                }
                None => {
                    tracing::warn!(
                        sink = %self.label,
                        table = %event.table_name,
                        field = %field.name,
                        "missing field schema for update"
                    );
                }
            }
        }

        let mut args = SqliteArguments::default();
        let mut column_names = Vec::with_capacity(values.len());
        for (name, value) in values {
            let sql_type = column_types.get(&name).copied().unwrap_or(SqliteType::Text);
            push_argument(&mut args, &value, sql_type);
            column_names.push(name);
        }

        let table_ident = quote_ident(schema.table_name());
        let columns_sql = column_names
            .iter()
            .map(|name| quote_ident(name))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = (0..column_names.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");

        let pk_name = schema.id_column().name.clone();
        let pk_ident = quote_ident(&pk_name);

        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_ident, columns_sql, placeholders
        );

        let update_columns: Vec<&String> = column_names
            .iter()
            .filter(|name| **name != pk_name)
            .collect();

        if !update_columns.is_empty() {
            let set_clause = update_columns
                .iter()
                .map(|name| format!("{} = excluded.{}", quote_ident(name), quote_ident(name)))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(
                " ON CONFLICT({}) DO UPDATE SET {}",
                pk_ident, set_clause
            ));
        } else {
            sql.push_str(&format!(" ON CONFLICT({}) DO NOTHING", pk_ident));
        }

        sqlx::query_with(&sql, args).execute(&mut **tx).await?;

        tracing::debug!(
            sink = %self.label,
            table = %event.table_name,
            storage_table = %schema.table_name(),
            id = %id_value,
            block = env.raw.block_number.unwrap_or_default(),
            "stored update_record",
        );
        Ok(())
    }

    async fn handle_erc20(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        env: &Envelope,
        event: &Erc20Transfer,
    ) -> Result<()> {
        let contract = felt_to_padded_hex(&event.contract);
        let from = felt_to_padded_hex(&event.from);
        let to = felt_to_padded_hex(&event.to);
        let amount = event.amount.to_string();

        sqlx::query(
            r#"
            INSERT INTO erc20_transfers (block_number, transaction_hash, contract, sender, recipient, amount)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6);
        "#,
        )
        .bind(env.raw.block_number.unwrap_or_default() as i64)
        .bind(felt_to_padded_hex(&&env.raw.transaction_hash))
        .bind(contract)
        .bind(from)
        .bind(to)
        .bind(amount)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn handle_erc721(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        env: &Envelope,
        event: &Erc721Transfer,
    ) -> Result<()> {
        let contract = felt_to_padded_hex(&event.contract);
        let from = felt_to_padded_hex(&event.from);
        let to = felt_to_padded_hex(&event.to);
        let token_id = felt_to_padded_hex(&event.token_id);

        sqlx::query(
            r#"
            INSERT INTO erc721_transfers (block_number, transaction_hash, contract, sender, recipient, token_id)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6);
        "#,
        )
        .bind(env.raw.block_number.unwrap_or_default() as i64)
        .bind(felt_to_padded_hex(&env.raw.transaction_hash))
        .bind(contract)
        .bind(from)
        .bind(to)
        .bind(token_id)
        .execute(&mut **tx)
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
        let mut tx = self.pool.begin().await?;
        for env in &batch.items {
            match env.type_id {
                DeclareTableV1::TYPE_ID => {
                    if let Some(event) = env.downcast::<DeclareTableV1>() {
                        self.handle_declare(&mut tx, env, event).await?;
                    }
                }
                UpdateRecordFieldsV1::TYPE_ID => {
                    if let Some(event) = env.downcast::<UpdateRecordFieldsV1>() {
                        self.handle_update_record(&mut tx, env, event).await?;
                    }
                }
                Erc20Transfer::TYPE_ID => {
                    if let Some(event) = env.downcast::<Erc20Transfer>() {
                        self.handle_erc20(&mut tx, env, event).await?;
                    }
                }
                Erc721Transfer::TYPE_ID => {
                    if let Some(event) = env.downcast::<Erc721Transfer>() {
                        self.handle_erc721(&mut tx, env, event).await?;
                    }
                }
                _ => {
                    tracing::warn!(sink = %self.label, "event type_id not handled: {}", env.type_id);
                }
            }
        }

        tx.commit().await?;

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
    #[serde(default)]
    pub clean_on_start: bool,
}

pub struct SqliteSinkFactory;

#[async_trait]
impl SinkFactory for SqliteSinkFactory {
    fn kind(&self) -> &'static str {
        "sqlite"
    }

    async fn create(&self, name: &str, config: JsonValue) -> Result<Arc<dyn Sink>> {
        let cfg: SqliteSinkConfig = serde_json::from_value(config)?;

        // Temporary flag to clean the db. On sqlite, currently we are using
        // a file for the db.
        if cfg.clean_on_start {
            std::fs::remove_file(&cfg.database_url)?;
        }

        let label = cfg.label.clone().unwrap_or_else(|| name.to_string());
        let sink = SqliteSink::connect(
            label,
            &cfg.database_url,
            cfg.max_connections,
            HashMap::new(),
        )
        .await?;
        Ok(Arc::new(sink) as Arc<dyn Sink>)
    }
}

fn sanitize_table_segment(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut chars = name.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '(' | '<' => break,
            c if c.is_ascii_alphanumeric() || c == '_' || c == '-' => result.push(c),
            ':' => result.push('_'),
            _ => result.push('_'),
        }
    }
    if result.is_empty() {
        "_".to_string()
    } else {
        result
    }
}

pub fn register(registry: &mut SinkRegistry) {
    registry.register_factory(Arc::new(SqliteSinkFactory));
}
