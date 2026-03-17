use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Result};
use chrono::Utc;
use introspect_types::serialize::ToCairoDeSeFrom;
use introspect_types::serialize_def::CairoTypeSerialization;
use introspect_types::{
    Attributes, CairoDeserializer, ColumnDef, PrimaryTypeDef, ResultDef, TupleDef, TypeDef,
};
use serde::ser::SerializeMap;
use serde::Serializer;
use serde_json::{Map, Serializer as JsonSerializer, Value};
use sqlx::{
    any::AnyPoolOptions, postgres::PgPoolOptions, sqlite::SqliteConnectOptions,
    sqlite::SqlitePoolOptions, Any, ConnectOptions, Pool, QueryBuilder, Row,
};
use starknet::core::types::Felt;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use torii_dojo::store::postgres::PgStore;
use torii_dojo::store::sqlite::SqliteStore;
use torii_dojo::store::DojoStoreTrait;
use torii_dojo::DojoTable;
use torii_introspect::events::{CreateTable, Record, UpdateTable};
use torii_introspect::schema::TableSchema;

use crate::proto::types::{
    self, clause::ClauseType, member_value::ValueType, ComparisonOperator, ContractType,
    LogicalOperator, PaginationDirection, PatternMatching,
};
use crate::proto::world::{
    world_server::World, RetrieveEntitiesRequest, RetrieveEntitiesResponse, RetrieveEventsRequest,
    RetrieveEventsResponse, SubscribeContractsRequest, SubscribeContractsResponse,
    SubscribeEntitiesRequest, SubscribeEntityResponse, SubscribeEventsRequest,
    SubscribeEventsResponse, UpdateEntitiesSubscriptionRequest, WorldsRequest, WorldsResponse,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableKind {
    Entity,
    EventMessage,
}

impl TableKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Entity => "entity",
            Self::EventMessage => "event_message",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "entity" => Some(Self::Entity),
            "event_message" => Some(Self::EventMessage),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DbBackend {
    Sqlite,
    Postgres,
}

impl DbBackend {
    fn detect(database_url: &str) -> Self {
        if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
            Self::Postgres
        } else {
            Self::Sqlite
        }
    }
}

#[derive(Clone)]
pub struct EcsService {
    state: Arc<EcsState>,
}

struct EcsState {
    pool: Pool<Any>,
    backend: DbBackend,
    database_url: String,
    managed_tables: Mutex<Option<Arc<HashMap<String, ManagedTable>>>>,
    entity_subscriptions: Mutex<HashMap<u64, EntitySubscription>>,
    event_message_subscriptions: Mutex<HashMap<u64, EntitySubscription>>,
    event_subscriptions: Mutex<HashMap<u64, EventSubscription>>,
    contract_subscriptions: Mutex<HashMap<u64, ContractSubscription>>,
}

struct EntitySubscription {
    clause: Option<types::Clause>,
    world_addresses: HashSet<Vec<u8>>,
    sender: mpsc::Sender<Result<SubscribeEntityResponse, Status>>,
}

struct EventSubscription {
    keys: Vec<types::KeysClause>,
    sender: mpsc::Sender<Result<SubscribeEventsResponse, Status>>,
}

struct ContractSubscription {
    query: types::ContractQuery,
    sender: mpsc::Sender<Result<SubscribeContractsResponse, Status>>,
}

#[derive(Clone)]
struct ManagedTable {
    world_address: Felt,
    table: DojoTable,
    kind: TableKind,
}

#[derive(Default)]
struct EntityAggregate {
    world_address: Vec<u8>,
    hashed_keys: Vec<u8>,
    models: Vec<types::Struct>,
    created_at: u64,
    updated_at: u64,
    executed_at: u64,
}

impl EntityAggregate {
    fn into_proto(self) -> types::Entity {
        types::Entity {
            hashed_keys: self.hashed_keys,
            models: self.models,
            created_at: self.created_at,
            updated_at: self.updated_at,
            executed_at: self.executed_at,
            world_address: self.world_address,
        }
    }
}

impl EcsService {
    pub async fn new(database_url: &str, max_connections: Option<u32>) -> Result<Self> {
        sqlx::any::install_default_drivers();

        let backend = DbBackend::detect(database_url);
        let database_url = match backend {
            DbBackend::Postgres => database_url.to_string(),
            DbBackend::Sqlite => sqlite_url(database_url)?,
        };

        let pool = AnyPoolOptions::new()
            .max_connections(max_connections.unwrap_or(if backend == DbBackend::Sqlite {
                1
            } else {
                5
            }))
            .connect(&database_url)
            .await?;

        let service = Self {
            state: Arc::new(EcsState {
                pool,
                backend,
                database_url,
                managed_tables: Mutex::new(None),
                entity_subscriptions: Mutex::new(HashMap::new()),
                event_message_subscriptions: Mutex::new(HashMap::new()),
                event_subscriptions: Mutex::new(HashMap::new()),
                contract_subscriptions: Mutex::new(HashMap::new()),
            }),
        };
        service.initialize().await?;
        Ok(service)
    }

    async fn initialize(&self) -> Result<()> {
        if self.state.backend == DbBackend::Sqlite {
            sqlx::query("PRAGMA journal_mode=WAL")
                .execute(&self.state.pool)
                .await
                .ok();
        }

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS torii_ecs_contracts (
                contract_address TEXT PRIMARY KEY,
                contract_type INTEGER NOT NULL,
                head BIGINT,
                tps BIGINT,
                last_block_timestamp BIGINT,
                last_pending_block_tx TEXT,
                updated_at BIGINT NOT NULL,
                created_at BIGINT NOT NULL
            )",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS torii_ecs_table_kinds (
                table_id TEXT PRIMARY KEY,
                world_address TEXT NOT NULL,
                kind TEXT NOT NULL,
                updated_at BIGINT NOT NULL
            )",
        )
        .execute(&self.state.pool)
        .await?;

        let entity_meta_sql = match self.state.backend {
            DbBackend::Sqlite => {
                "CREATE TABLE IF NOT EXISTS torii_ecs_entity_meta (
                    kind TEXT NOT NULL,
                    world_address TEXT NOT NULL,
                    table_id TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    executed_at BIGINT NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(kind, world_address, table_id, entity_id)
                )"
            }
            DbBackend::Postgres => {
                "CREATE TABLE IF NOT EXISTS torii_ecs_entity_meta (
                    kind TEXT NOT NULL,
                    world_address TEXT NOT NULL,
                    table_id TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    executed_at BIGINT NOT NULL,
                    deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY(kind, world_address, table_id, entity_id)
                )"
            }
        };
        sqlx::query(entity_meta_sql)
            .execute(&self.state.pool)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS torii_ecs_events (
                event_id TEXT PRIMARY KEY,
                world_address TEXT NOT NULL,
                block_number BIGINT NOT NULL,
                executed_at BIGINT NOT NULL,
                transaction_hash TEXT NOT NULL,
                keys_json TEXT NOT NULL,
                data_json TEXT NOT NULL
            )",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS torii_ecs_entity_models (
                kind TEXT NOT NULL,
                world_address TEXT NOT NULL,
                table_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                row_json TEXT NOT NULL,
                updated_at BIGINT NOT NULL,
                PRIMARY KEY(kind, world_address, table_id, entity_id)
            )",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_meta_lookup_idx
             ON torii_ecs_entity_meta(kind, world_address, entity_id)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_meta_table_lookup_idx
             ON torii_ecs_entity_meta(kind, table_id, world_address, entity_id, deleted)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_meta_keyset_idx
             ON torii_ecs_entity_meta(kind, deleted, entity_id, world_address)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_models_lookup_idx
             ON torii_ecs_entity_models(kind, world_address, entity_id)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_models_table_lookup_idx
             ON torii_ecs_entity_models(kind, table_id, world_address, entity_id)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_models_key_lookup_idx
             ON torii_ecs_entity_models(kind, world_address, entity_id)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_events_cursor_idx
             ON torii_ecs_events(event_id)",
        )
        .execute(&self.state.pool)
        .await?;

        Ok(())
    }

    pub async fn record_contract_progress(
        &self,
        contract_address: Felt,
        contract_type: ContractType,
        head: u64,
        executed_at: u64,
        last_pending_block_tx: Option<Felt>,
    ) -> Result<()> {
        let now = Utc::now().timestamp() as u64;
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_ecs_contracts (
                    contract_address, contract_type, head, tps, last_block_timestamp,
                    last_pending_block_tx, updated_at, created_at
                ) VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, ?6)
                ON CONFLICT(contract_address) DO UPDATE SET
                    contract_type = excluded.contract_type,
                    head = excluded.head,
                    last_block_timestamp = excluded.last_block_timestamp,
                    last_pending_block_tx = excluded.last_pending_block_tx,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_ecs_contracts (
                    contract_address, contract_type, head, tps, last_block_timestamp,
                    last_pending_block_tx, updated_at, created_at
                ) VALUES ($1, $2, $3, NULL, $4, $5, $6, $6)
                ON CONFLICT(contract_address) DO UPDATE SET
                    contract_type = EXCLUDED.contract_type,
                    head = EXCLUDED.head,
                    last_block_timestamp = EXCLUDED.last_block_timestamp,
                    last_pending_block_tx = EXCLUDED.last_pending_block_tx,
                    updated_at = EXCLUDED.updated_at"
            }
        };
        sqlx::query(sql)
            .bind(felt_hex(contract_address))
            .bind(contract_type as i32)
            .bind(head as i64)
            .bind(executed_at as i64)
            .bind(last_pending_block_tx.map(felt_hex))
            .bind(now as i64)
            .execute(&self.state.pool)
            .await?;

        if self.state.contract_subscriptions.lock().await.is_empty() {
            return Ok(());
        }

        let contract = self
            .load_contracts(&types::ContractQuery {
                contract_addresses: vec![contract_address.to_bytes_be().to_vec()],
                contract_types: vec![],
            })
            .await?
            .into_iter()
            .next();

        if let Some(contract) = contract {
            self.publish_contract_update(contract).await;
        }
        Ok(())
    }

    pub async fn record_table_kind(
        &self,
        world_address: Felt,
        table_id: Felt,
        kind: TableKind,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_ecs_table_kinds (table_id, world_address, kind, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(table_id) DO UPDATE SET
                    world_address = excluded.world_address,
                    kind = excluded.kind,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_ecs_table_kinds (table_id, world_address, kind, updated_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT(table_id) DO UPDATE SET
                    world_address = EXCLUDED.world_address,
                    kind = EXCLUDED.kind,
                    updated_at = EXCLUDED.updated_at"
            }
        };
        sqlx::query(sql)
            .bind(felt_hex(table_id))
            .bind(felt_hex(world_address))
            .bind(kind.as_str())
            .bind(now)
            .execute(&self.state.pool)
            .await?;
        self.update_managed_table_kind(table_id, world_address, kind)
            .await;
        Ok(())
    }

    pub async fn cache_created_table(&self, world_address: Felt, table: &CreateTable) {
        let schema = TableSchema {
            id: table.id,
            name: table.name.clone(),
            attributes: table.attributes.clone(),
            primary: table.primary.clone(),
            columns: table.columns.clone(),
        };
        self.put_managed_table(world_address, DojoTable::from(schema))
            .await;
    }

    pub async fn cache_updated_table(&self, world_address: Felt, table: &UpdateTable) {
        let schema = TableSchema {
            id: table.id,
            name: table.name.clone(),
            attributes: table.attributes.clone(),
            primary: table.primary.clone(),
            columns: table.columns.clone(),
        };
        self.put_managed_table(world_address, DojoTable::from(schema))
            .await;
    }

    pub async fn upsert_entity_meta(
        &self,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
        entity_id: Felt,
        executed_at: u64,
        deleted: bool,
    ) -> Result<()> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_ecs_entity_meta (
                    kind, world_address, table_id, entity_id, created_at, updated_at, executed_at, deleted
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?5, ?6)
                 ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    executed_at = excluded.executed_at,
                    deleted = excluded.deleted"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_ecs_entity_meta (
                    kind, world_address, table_id, entity_id, created_at, updated_at, executed_at, deleted
                 ) VALUES ($1, $2, $3, $4, $5, $5, $5, $6)
                 ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    executed_at = EXCLUDED.executed_at,
                    deleted = EXCLUDED.deleted"
            }
        };
        let mut query = sqlx::query(sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(table_id))
            .bind(felt_hex(entity_id))
            .bind(executed_at as i64);
        query = match self.state.backend {
            DbBackend::Sqlite => query.bind(i64::from(deleted)),
            DbBackend::Postgres => query.bind(deleted),
        };
        query.execute(&self.state.pool).await?;
        Ok(())
    }

    pub async fn upsert_entity_model(
        &self,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
        columns: &[Felt],
        record: &Record,
        executed_at: u64,
    ) -> Result<()> {
        let Some(table) = self.load_managed_table(table_id).await? else {
            return Ok(());
        };
        let row = record_to_json_map(&table.table, columns, record)?;
        let row_json = serde_json::to_string(&row)?;
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_ecs_entity_models (
                    kind, world_address, table_id, entity_id, row_json, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                    row_json = excluded.row_json,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_ecs_entity_models (
                    kind, world_address, table_id, entity_id, row_json, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                    row_json = EXCLUDED.row_json,
                    updated_at = EXCLUDED.updated_at"
            }
        };
        sqlx::query(sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(table_id))
            .bind(felt_hex(Felt::from_bytes_be(&record.id)))
            .bind(row_json)
            .bind(executed_at as i64)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_entity_model(
        &self,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
        entity_id: Felt,
    ) -> Result<()> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "DELETE FROM torii_ecs_entity_models
                 WHERE kind = ?1 AND world_address = ?2 AND table_id = ?3 AND entity_id = ?4"
            }
            DbBackend::Postgres => {
                "DELETE FROM torii_ecs_entity_models
                 WHERE kind = $1 AND world_address = $2 AND table_id = $3 AND entity_id = $4"
            }
        };
        sqlx::query(sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(table_id))
            .bind(felt_hex(entity_id))
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    pub async fn table_kind(&self, table_id: Felt) -> Result<TableKind> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => "SELECT kind FROM torii_ecs_table_kinds WHERE table_id = ?1",
            DbBackend::Postgres => "SELECT kind FROM torii_ecs_table_kinds WHERE table_id = $1",
        };
        let row = sqlx::query(sql)
            .bind(felt_hex(table_id))
            .fetch_optional(&self.state.pool)
            .await?;
        Ok(row
            .and_then(|row| row.try_get::<String, _>("kind").ok())
            .and_then(|kind| TableKind::from_str(&kind))
            .unwrap_or(TableKind::Entity))
    }

    pub async fn store_event(
        &self,
        world_address: Felt,
        transaction_hash: Felt,
        block_number: u64,
        executed_at: u64,
        keys: &[Felt],
        data: &[Felt],
        ordinal: usize,
    ) -> Result<()> {
        let event_id = format!(
            "{}:{}:{}",
            felt_hex(world_address),
            felt_hex(transaction_hash),
            ordinal
        );
        let keys_json =
            serde_json::to_string(&keys.iter().map(|felt| felt_hex(*felt)).collect::<Vec<_>>())?;
        let data_json =
            serde_json::to_string(&data.iter().map(|felt| felt_hex(*felt)).collect::<Vec<_>>())?;

        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_ecs_events (
                    event_id, world_address, block_number, executed_at, transaction_hash, keys_json, data_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(event_id) DO NOTHING"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_ecs_events (
                    event_id, world_address, block_number, executed_at, transaction_hash, keys_json, data_json
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT(event_id) DO NOTHING"
            }
        };
        sqlx::query(sql)
            .bind(event_id)
            .bind(felt_hex(world_address))
            .bind(block_number as i64)
            .bind(executed_at as i64)
            .bind(felt_hex(transaction_hash))
            .bind(keys_json)
            .bind(data_json)
            .execute(&self.state.pool)
            .await?;

        self.publish_event_update(types::Event {
            keys: keys
                .iter()
                .map(|felt| felt.to_bytes_be().to_vec())
                .collect(),
            data: data
                .iter()
                .map(|felt| felt.to_bytes_be().to_vec())
                .collect(),
            transaction_hash: transaction_hash.to_bytes_be().to_vec(),
        })
        .await;

        Ok(())
    }

    pub async fn publish_entity_update(
        &self,
        kind: TableKind,
        world_address: Felt,
        entity_id: Felt,
    ) -> Result<()> {
        let start = Instant::now();
        let subscriptions = match kind {
            TableKind::Entity => &self.state.entity_subscriptions,
            TableKind::EventMessage => &self.state.event_message_subscriptions,
        };
        if subscriptions.lock().await.is_empty() {
            return Ok(());
        }

        let entity = self
            .load_entity_by_id(kind, world_address, entity_id)
            .await?
            .map(EntityAggregate::into_proto);
        let Some(entity) = entity else {
            return Ok(());
        };

        let (checked, targets) = {
            let subscriptions = subscriptions.lock().await;
            let checked = subscriptions.len();
            let targets = subscriptions
                .iter()
                .filter_map(|(&subscription_id, subscription)| {
                    if !subscription.world_addresses.is_empty()
                        && !subscription.world_addresses.contains(&entity.world_address)
                    {
                        return None;
                    }
                    if let Some(clause) = &subscription.clause {
                        if !entity_matches_clause(&entity, clause) {
                            return None;
                        }
                    }
                    Some((subscription_id, subscription.sender.clone()))
                })
                .collect::<Vec<_>>();
            (checked, targets)
        };
        let matched = targets.len();

        let mut closed = Vec::new();
        for (subscription_id, sender) in targets {
            if let Err(err) = sender.try_send(Ok(SubscribeEntityResponse {
                entity: Some(entity.clone()),
                subscription_id,
            })) {
                if matches!(err, TrySendError::Closed(_)) {
                    closed.push(subscription_id);
                }
            }
        }

        if !closed.is_empty() {
            let mut subscriptions = subscriptions.lock().await;
            for subscription_id in closed {
                subscriptions.remove(&subscription_id);
            }
        }
        ::metrics::histogram!("torii_ecs_publish_entity_update_seconds")
            .record(start.elapsed().as_secs_f64());
        ::metrics::counter!("torii_ecs_publish_entity_subscribers_checked_total")
            .increment(checked as u64);
        ::metrics::counter!("torii_ecs_publish_entity_subscribers_matched_total")
            .increment(matched as u64);
        Ok(())
    }

    async fn publish_event_update(&self, event: types::Event) {
        let start = Instant::now();
        let (checked, targets) = {
            let subscriptions = self.state.event_subscriptions.lock().await;
            if subscriptions.is_empty() {
                return;
            }
            let checked = subscriptions.len();
            let targets = subscriptions
                .iter()
                .filter_map(|(&subscription_id, subscription)| {
                    if !match_keys(&event.keys, &subscription.keys) {
                        return None;
                    }
                    Some((subscription_id, subscription.sender.clone()))
                })
                .collect::<Vec<_>>();
            (checked, targets)
        };
        let matched = targets.len();

        let mut closed = Vec::new();
        for (subscription_id, sender) in targets {
            if let Err(err) = sender.try_send(Ok(SubscribeEventsResponse {
                event: Some(event.clone()),
            })) {
                if matches!(err, TrySendError::Closed(_)) {
                    closed.push(subscription_id);
                }
            }
        }

        if !closed.is_empty() {
            let mut subscriptions = self.state.event_subscriptions.lock().await;
            for subscription_id in closed {
                subscriptions.remove(&subscription_id);
            }
        }
        ::metrics::histogram!("torii_ecs_publish_event_update_seconds")
            .record(start.elapsed().as_secs_f64());
        ::metrics::counter!("torii_ecs_publish_event_subscribers_checked_total")
            .increment(checked as u64);
        ::metrics::counter!("torii_ecs_publish_event_subscribers_matched_total")
            .increment(matched as u64);
    }

    async fn publish_contract_update(&self, contract: types::Contract) {
        let start = Instant::now();
        let (checked, targets) = {
            let subscriptions = self.state.contract_subscriptions.lock().await;
            if subscriptions.is_empty() {
                return;
            }
            let checked = subscriptions.len();
            let targets = subscriptions
                .iter()
                .filter_map(|(&subscription_id, subscription)| {
                    if !contract_matches_query(&contract, &subscription.query) {
                        return None;
                    }
                    Some((subscription_id, subscription.sender.clone()))
                })
                .collect::<Vec<_>>();
            (checked, targets)
        };
        let matched = targets.len();

        let mut closed = Vec::new();
        for (subscription_id, sender) in targets {
            if let Err(err) = sender.try_send(Ok(SubscribeContractsResponse {
                contract: Some(contract.clone()),
            })) {
                if matches!(err, TrySendError::Closed(_)) {
                    closed.push(subscription_id);
                }
            }
        }

        if !closed.is_empty() {
            let mut subscriptions = self.state.contract_subscriptions.lock().await;
            for subscription_id in closed {
                subscriptions.remove(&subscription_id);
            }
        }
        ::metrics::histogram!("torii_ecs_publish_contract_update_seconds")
            .record(start.elapsed().as_secs_f64());
        ::metrics::counter!("torii_ecs_publish_contract_subscribers_checked_total")
            .increment(checked as u64);
        ::metrics::counter!("torii_ecs_publish_contract_subscribers_matched_total")
            .increment(matched as u64);
    }

    async fn load_contracts(&self, query: &types::ContractQuery) -> Result<Vec<types::Contract>> {
        let mut builder = QueryBuilder::<Any>::new(
            "SELECT contract_address, contract_type, head, tps, last_block_timestamp, \
                    last_pending_block_tx, updated_at, created_at \
             FROM torii_ecs_contracts",
        );
        if !query.contract_addresses.is_empty() || !query.contract_types.is_empty() {
            builder.push(" WHERE ");
            if !query.contract_addresses.is_empty() {
                builder.push("contract_address IN (");
                {
                    let mut separated = builder.separated(", ");
                    for address in &query.contract_addresses {
                        separated.push_bind(felt_hex(felt_from_bytes(address)?));
                    }
                }
                builder.push(")");
                if !query.contract_types.is_empty() {
                    builder.push(" AND ");
                }
            }
            if !query.contract_types.is_empty() {
                builder.push("contract_type IN (");
                {
                    let mut separated = builder.separated(", ");
                    for contract_type in &query.contract_types {
                        separated.push_bind(*contract_type);
                    }
                }
                builder.push(")");
            }
        }

        let rows = builder.build().fetch_all(&self.state.pool).await?;
        let mut contracts = Vec::new();
        for row in rows {
            let contract_address = row.try_get::<String, _>("contract_address")?;
            let contract_type = row.try_get::<i32, _>("contract_type")?;
            let contract = types::Contract {
                contract_address: felt_from_hex(&contract_address)?.to_bytes_be().to_vec(),
                contract_type,
                head: row
                    .try_get::<Option<i64>, _>("head")?
                    .map(|value| value as u64),
                tps: row
                    .try_get::<Option<i64>, _>("tps")?
                    .map(|value| value as u64),
                last_block_timestamp: row
                    .try_get::<Option<i64>, _>("last_block_timestamp")?
                    .map(|value| value as u64),
                last_pending_block_tx: row
                    .try_get::<Option<String>, _>("last_pending_block_tx")?
                    .map(|value| felt_from_hex(&value))
                    .transpose()?
                    .map(|felt| felt.to_bytes_be().to_vec()),
                updated_at: row.try_get::<i64, _>("updated_at")? as u64,
                created_at: row.try_get::<i64, _>("created_at")? as u64,
            };
            contracts.push(contract);
        }
        Ok(contracts)
    }

    async fn load_worlds(&self, requested: &[Felt]) -> Result<Vec<types::World>> {
        let requested = requested.iter().copied().collect::<HashSet<_>>();
        let mut by_world: HashMap<Vec<u8>, Vec<types::Model>> = HashMap::new();
        for table in self.load_managed_tables(None).await? {
            if !requested.is_empty() && !requested.contains(&table.world_address) {
                continue;
            }
            by_world
                .entry(table.world_address.to_bytes_be().to_vec())
                .or_default()
                .push(model_from_table(&table));
        }

        let mut worlds = by_world
            .into_iter()
            .map(|(world_address, models)| types::World {
                world_address: felt_from_bytes(&world_address)
                    .unwrap_or_default()
                    .to_string(),
                models,
            })
            .collect::<Vec<_>>();
        worlds.sort_by(|a, b| a.world_address.cmp(&b.world_address));
        Ok(worlds)
    }

    async fn put_managed_table(&self, world_address: Felt, table: DojoTable) {
        if self.state.managed_tables.lock().await.is_none() {
            let _ = self.load_managed_table_map().await;
        }
        let mut managed_tables = self.state.managed_tables.lock().await;
        if let Some(tables) = managed_tables.as_ref() {
            let mut next = (**tables).clone();
            let key = felt_hex(table.id);
            let kind = next.get(&key).map_or(TableKind::Entity, |table| table.kind);
            next.insert(
                key,
                ManagedTable {
                    world_address,
                    table,
                    kind,
                },
            );
            *managed_tables = Some(Arc::new(next));
        }
    }

    async fn update_managed_table_kind(
        &self,
        table_id: Felt,
        world_address: Felt,
        kind: TableKind,
    ) {
        let mut managed_tables = self.state.managed_tables.lock().await;
        let Some(tables) = managed_tables.as_ref() else {
            return;
        };
        let mut next = (**tables).clone();
        let key = felt_hex(table_id);
        if let Some(table) = next.get_mut(&key) {
            table.world_address = world_address;
            table.kind = kind;
            *managed_tables = Some(Arc::new(next));
        } else {
            *managed_tables = None;
        }
    }

    async fn load_managed_table(&self, table_id: Felt) -> Result<Option<ManagedTable>> {
        let key = felt_hex(table_id);
        Ok(self.load_managed_table_map().await?.get(&key).cloned())
    }

    async fn load_managed_table_map(&self) -> Result<Arc<HashMap<String, ManagedTable>>> {
        {
            let managed_tables = self.state.managed_tables.lock().await;
            if let Some(tables) = managed_tables.as_ref() {
                return Ok(Arc::clone(tables));
            }
        }

        let kinds = self.load_table_kind_rows().await?;
        let tables = self.load_dojo_tables().await?;
        let mut managed = HashMap::with_capacity(tables.len());
        for table in tables {
            let key = felt_hex(table.id);
            let (world_address, table_kind) = if let Some((world, kind_name)) = kinds.get(&key) {
                let kind = TableKind::from_str(kind_name).unwrap_or(TableKind::Entity);
                (felt_from_hex(world)?, kind)
            } else {
                (Felt::ZERO, TableKind::Entity)
            };
            managed.insert(
                key,
                ManagedTable {
                    world_address,
                    table,
                    kind: table_kind,
                },
            );
        }

        let mut managed_tables = self.state.managed_tables.lock().await;
        let managed = Arc::new(managed);
        *managed_tables = Some(Arc::clone(&managed));
        Ok(managed)
    }

    async fn load_table_kind_rows(&self) -> Result<HashMap<String, (String, String)>> {
        let table_kind_sql = "SELECT table_id, world_address, kind FROM torii_ecs_table_kinds";
        let kind_rows = sqlx::query(table_kind_sql)
            .fetch_all(&self.state.pool)
            .await?;
        let mut kinds = HashMap::new();
        for row in kind_rows {
            kinds.insert(
                row.try_get::<String, _>("table_id")?,
                (
                    row.try_get::<String, _>("world_address")?,
                    row.try_get::<String, _>("kind")?,
                ),
            );
        }
        Ok(kinds)
    }

    async fn load_dojo_tables(&self) -> Result<Vec<DojoTable>> {
        match self.state.backend {
            DbBackend::Sqlite => {
                let pool = SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect_with(SqliteConnectOptions::from_str(&self.state.database_url)?)
                    .await?;
                let store = SqliteStore(Arc::new(pool));
                Ok(store.load_tables(&[]).await?)
            }
            DbBackend::Postgres => {
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&self.state.database_url)
                    .await?;
                let store = PgStore(Arc::new(pool));
                Ok(store.load_tables(&[]).await?)
            }
        }
    }

    async fn load_managed_tables(&self, kind: Option<TableKind>) -> Result<Vec<ManagedTable>> {
        Ok(self
            .load_managed_table_map()
            .await?
            .values()
            .filter(|table| kind.is_none_or(|required| table.kind == required))
            .cloned()
            .collect())
    }

    async fn load_entity_page(
        &self,
        kind: TableKind,
        query: &types::Query,
    ) -> Result<(Vec<types::Entity>, String)> {
        let world_filters = query
            .world_addresses
            .iter()
            .map(|bytes| felt_from_bytes(bytes))
            .collect::<Result<Vec<_>>>()?;
        let world_filter_set = world_filters.iter().copied().collect::<HashSet<_>>();
        let model_filter_set = query
            .models
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let models = self
            .load_managed_tables(Some(kind))
            .await?
            .into_iter()
            .filter(|table| {
                (world_filter_set.is_empty() || world_filter_set.contains(&table.world_address))
                    && (model_filter_set.is_empty()
                        || model_filter_set.contains(table.table.name.as_str()))
            })
            .collect::<Vec<_>>();
        if models.is_empty() {
            return Ok((Vec::new(), String::new()));
        }

        let model_map = models
            .into_iter()
            .map(|table| (felt_hex(table.table.id), table))
            .collect::<HashMap<_, _>>();
        let table_ids = model_map.keys().cloned().collect::<Vec<_>>();
        let table_id_filter = if query.models.is_empty() {
            None
        } else {
            Some(table_ids.as_slice())
        };
        let member_pushdown = member_pushdown_from_clause(query.clause.as_ref(), &model_map);
        let target_limit = query.pagination.as_ref().map_or(100, |pagination| {
            if pagination.limit == 0 {
                100
            } else {
                pagination.limit as usize
            }
        });

        let direction = query
            .pagination
            .as_ref()
            .map_or(PaginationDirection::Forward as i32, |pagination| {
                pagination.direction
            });
        let cursor = query
            .pagination
            .as_ref()
            .map_or(String::new(), |pagination| pagination.cursor.clone());

        let keys_start = Instant::now();
        let mut candidate_keys = self
            .load_entity_page_keys(
                kind,
                &world_filters,
                table_id_filter,
                member_pushdown.as_ref(),
                &cursor,
                direction,
                target_limit.saturating_add(1),
            )
            .await?;
        ::metrics::histogram!("torii_ecs_load_entity_page_meta_seconds")
            .record(keys_start.elapsed().as_secs_f64());

        let has_more = target_limit != usize::MAX && candidate_keys.len() > target_limit;
        if has_more {
            candidate_keys.truncate(target_limit);
        }

        let meta_start = Instant::now();
        let candidate_meta = self
            .load_entity_meta_for_keys(kind, table_id_filter, candidate_keys.as_slice())
            .await?;
        ::metrics::histogram!("torii_ecs_load_entity_page_meta_rows_seconds")
            .record(meta_start.elapsed().as_secs_f64());
        ::metrics::gauge!("torii_ecs_load_entity_page_candidates").set(candidate_keys.len() as f64);

        let mut items = Vec::new();
        for chunk in candidate_keys.chunks(256) {
            let rows_start = Instant::now();
            let rows = self
                .load_entity_page_rows(kind, &world_filters, table_id_filter, chunk)
                .await?;
            ::metrics::histogram!("torii_ecs_load_entity_page_rows_seconds")
                .record(rows_start.elapsed().as_secs_f64());
            ::metrics::counter!("torii_ecs_load_entity_page_rows_total")
                .increment(rows.len() as u64);
            let row_map = rows.into_iter().fold(
                HashMap::<(String, String), Vec<(String, String)>>::new(),
                |mut acc, row| {
                    acc.entry((row.world_address, row.entity_id))
                        .or_default()
                        .push((row.table_id, row.row_json));
                    acc
                },
            );

            for key in chunk {
                let Some(meta) = candidate_meta.get(key) else {
                    continue;
                };
                let Some(entity) =
                    build_entity_from_snapshot(key, meta, row_map.get(key), &model_map)?
                else {
                    continue;
                };
                let entity = entity.into_proto();
                if query
                    .clause
                    .as_ref()
                    .is_some_and(|clause| !entity_matches_clause(&entity, clause))
                {
                    continue;
                }
                items.push(entity);
                if items.len() >= target_limit {
                    break;
                }
            }

            if items.len() >= target_limit {
                break;
            }
        }

        let next_cursor = if has_more {
            candidate_keys
                .last()
                .map(|(_, entity_id)| entity_id.trim_start_matches("0x").to_string())
                .unwrap_or_default()
        } else {
            String::new()
        };

        if query.no_hashed_keys {
            for entity in &mut items {
                entity.hashed_keys.clear();
            }
        }
        Ok((items, next_cursor))
    }

    async fn load_entity_page_from_storage(
        &self,
        kind: TableKind,
        query: &types::Query,
    ) -> Result<(Vec<types::Entity>, String)> {
        let world_filters = query
            .world_addresses
            .iter()
            .map(|bytes| felt_from_bytes(bytes))
            .collect::<Result<Vec<_>>>()?;
        let world_filter_set = world_filters.iter().copied().collect::<HashSet<_>>();
        let model_filter_set = query
            .models
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let models = self
            .load_managed_tables(Some(kind))
            .await?
            .into_iter()
            .filter(|table| {
                (world_filter_set.is_empty() || world_filter_set.contains(&table.world_address))
                    && (model_filter_set.is_empty()
                        || model_filter_set.contains(table.table.name.as_str()))
            })
            .collect::<Vec<_>>();

        let mut entities: HashMap<(Vec<u8>, Vec<u8>), EntityAggregate> = HashMap::new();
        for table in models {
            let rows = self.fetch_table_rows(&table).await?;
            let meta = self
                .load_entity_meta(table.kind, table.world_address, table.table.id)
                .await?;
            for row in rows {
                let entity_id = row
                    .get(&table.table.primary.name)
                    .cloned()
                    .unwrap_or(Value::Null);
                let entity_felt = value_to_primary_felt(&entity_id, &table.table.primary.type_def)
                    .unwrap_or(Felt::ZERO);
                let entity_key = entity_felt.to_bytes_be().to_vec();
                if meta
                    .get(&felt_hex(entity_felt))
                    .is_some_and(|meta| meta.deleted)
                {
                    continue;
                }

                let model = row_to_model_struct(&table.table, &row)?;
                let aggregate = entities
                    .entry((
                        table.world_address.to_bytes_be().to_vec(),
                        entity_key.clone(),
                    ))
                    .or_insert_with(|| EntityAggregate {
                        world_address: table.world_address.to_bytes_be().to_vec(),
                        hashed_keys: entity_key.clone(),
                        ..EntityAggregate::default()
                    });
                aggregate.models.push(model);
                if let Some(meta) = meta.get(&felt_hex(entity_felt)) {
                    if aggregate.created_at == 0 || meta.created_at < aggregate.created_at {
                        aggregate.created_at = meta.created_at;
                    }
                    aggregate.updated_at = aggregate.updated_at.max(meta.updated_at);
                    aggregate.executed_at = aggregate.executed_at.max(meta.executed_at);
                }
            }
        }

        let mut items = entities
            .into_values()
            .map(EntityAggregate::into_proto)
            .filter(|entity| {
                query
                    .clause
                    .as_ref()
                    .is_none_or(|clause| entity_matches_clause(entity, clause))
            })
            .collect::<Vec<_>>();
        items.sort_by(|a, b| a.hashed_keys.cmp(&b.hashed_keys));
        if query
            .pagination
            .as_ref()
            .is_some_and(|p| p.direction == PaginationDirection::Backward as i32)
        {
            items.reverse();
        }

        if let Some(pagination) = &query.pagination {
            if !pagination.cursor.is_empty() {
                items.retain(|entity| {
                    let key = hex::encode(&entity.hashed_keys);
                    if pagination.direction == PaginationDirection::Backward as i32 {
                        key < pagination.cursor
                    } else {
                        key > pagination.cursor
                    }
                });
            }
            let limit = if pagination.limit == 0 {
                100
            } else {
                pagination.limit as usize
            };
            let next_cursor = items
                .get(limit.saturating_sub(1))
                .map(|entity| hex::encode(&entity.hashed_keys))
                .unwrap_or_default();
            items.truncate(limit);
            if query.no_hashed_keys {
                for entity in &mut items {
                    entity.hashed_keys.clear();
                }
            }
            return Ok((items, next_cursor));
        }

        if query.no_hashed_keys {
            for entity in &mut items {
                entity.hashed_keys.clear();
            }
        }
        Ok((items, String::new()))
    }

    async fn load_entity_by_id(
        &self,
        kind: TableKind,
        world_address: Felt,
        entity_id: Felt,
    ) -> Result<Option<EntityAggregate>> {
        if let Some(entity) = self
            .load_entity_snapshot(kind, world_address, entity_id)
            .await?
        {
            return Ok(Some(entity));
        }

        let query = types::Query {
            clause: Some(types::Clause {
                clause_type: Some(ClauseType::HashedKeys(types::HashedKeysClause {
                    hashed_keys: vec![entity_id.to_bytes_be().to_vec()],
                })),
            }),
            no_hashed_keys: false,
            models: vec![],
            pagination: None,
            historical: false,
            world_addresses: vec![world_address.to_bytes_be().to_vec()],
        };
        Ok(self
            .load_entity_page(kind, &query)
            .await?
            .0
            .into_iter()
            .next()
            .map(|entity| EntityAggregate {
                world_address: entity.world_address,
                hashed_keys: entity.hashed_keys,
                models: entity.models,
                created_at: entity.created_at,
                updated_at: entity.updated_at,
                executed_at: entity.executed_at,
            }))
    }

    async fn load_entity_page_keys(
        &self,
        kind: TableKind,
        world_filters: &[Felt],
        table_ids: Option<&[String]>,
        member_pushdown: Option<&MemberPushdown>,
        cursor: &str,
        direction: i32,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        if table_ids.is_some_and(|table_ids| table_ids.is_empty()) {
            return Ok(Vec::new());
        }
        let limit = limit.clamp(1, 200);

        let cursor_op = if direction == PaginationDirection::Backward as i32 {
            "<"
        } else {
            ">"
        };
        let order = if direction == PaginationDirection::Backward as i32 {
            "DESC"
        } else {
            "ASC"
        };

        let mut builder = QueryBuilder::<Any>::new(format!(
            "SELECT DISTINCT m.world_address, m.entity_id \
             FROM torii_ecs_entity_meta m \
             WHERE m.kind = "
        ));
        builder.push_bind(kind.as_str());
        if let Some(table_ids) = table_ids {
            builder.push(" AND m.table_id IN (");
            {
                let mut separated = builder.separated(", ");
                for table_id in table_ids {
                    separated.push_bind(table_id);
                }
            }
            builder.push(")");
        }
        if !world_filters.is_empty() {
            builder.push(" AND m.world_address IN (");
            {
                let mut separated = builder.separated(", ");
                for world_address in world_filters {
                    separated.push_bind(felt_hex(*world_address));
                }
            }
            builder.push(")");
        }
        match self.state.backend {
            DbBackend::Sqlite => {
                builder.push(" AND m.deleted = 0");
            }
            DbBackend::Postgres => {
                builder.push(" AND m.deleted = FALSE");
            }
        }
        if !cursor.is_empty() {
            builder.push(" AND m.entity_id ");
            builder.push(cursor_op);
            builder.push(" ");
            builder.push_bind(format!("0x{cursor}"));
        }
        if let Some(member_pushdown) = member_pushdown {
            append_member_pushdown_sql(&mut builder, self.state.backend, member_pushdown);
        }
        builder.push(" ORDER BY m.entity_id ");
        builder.push(order);
        builder.push(", m.world_address ");
        builder.push(order);
        builder.push(" LIMIT ");
        builder.push_bind(limit as i64);

        let rows = builder.build().fetch_all(&self.state.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("world_address")?,
                    row.try_get::<String, _>("entity_id")?,
                ))
            })
            .collect()
    }

    async fn load_entity_meta_for_keys(
        &self,
        kind: TableKind,
        table_ids: Option<&[String]>,
        entity_keys: &[(String, String)],
    ) -> Result<HashMap<(String, String), HashMap<String, EntityMetaRow>>> {
        if table_ids.is_some_and(|table_ids| table_ids.is_empty()) || entity_keys.is_empty() {
            return Ok(HashMap::new());
        }

        let deleted_sql = match self.state.backend {
            DbBackend::Sqlite => "CAST(m.deleted AS INTEGER)",
            DbBackend::Postgres => "CASE WHEN m.deleted THEN 1 ELSE 0 END",
        };

        let mut builder = QueryBuilder::<Any>::new(format!(
            "SELECT m.world_address, m.table_id, m.entity_id, m.created_at, m.updated_at, m.executed_at, \
                    {deleted_sql} AS deleted \
             FROM torii_ecs_entity_meta m WHERE m.kind = "
        ));
        builder.push_bind(kind.as_str());
        if let Some(table_ids) = table_ids {
            builder.push(" AND m.table_id IN (");
            {
                let mut separated = builder.separated(", ");
                for table_id in table_ids {
                    separated.push_bind(table_id);
                }
            }
            builder.push(")");
        }

        let world_addresses = entity_keys
            .iter()
            .map(|(world_address, _)| world_address.clone())
            .collect::<HashSet<_>>();
        let entity_ids = entity_keys
            .iter()
            .map(|(_, entity_id)| entity_id.clone())
            .collect::<HashSet<_>>();
        let allowed = entity_keys.iter().cloned().collect::<HashSet<_>>();

        builder.push(" AND m.world_address IN (");
        {
            let mut separated = builder.separated(", ");
            for world_address in &world_addresses {
                separated.push_bind(world_address);
            }
        }
        builder.push(")");
        builder.push(" AND m.entity_id IN (");
        {
            let mut separated = builder.separated(", ");
            for entity_id in &entity_ids {
                separated.push_bind(entity_id);
            }
        }
        builder.push(")");
        match self.state.backend {
            DbBackend::Sqlite => {
                builder.push(" AND m.deleted = 0");
            }
            DbBackend::Postgres => {
                builder.push(" AND m.deleted = FALSE");
            }
        }

        let rows = builder.build().fetch_all(&self.state.pool).await?;
        let mut meta = HashMap::new();
        for row in rows {
            let world_address = row.try_get::<String, _>("world_address")?;
            let entity_id = row.try_get::<String, _>("entity_id")?;
            if !allowed.contains(&(world_address.clone(), entity_id.clone())) {
                continue;
            }
            meta.entry((world_address, entity_id))
                .or_insert_with(HashMap::new)
                .insert(
                    row.try_get::<String, _>("table_id")?,
                    EntityMetaRow {
                        created_at: row.try_get::<i64, _>("created_at")? as u64,
                        updated_at: row.try_get::<i64, _>("updated_at")? as u64,
                        executed_at: row.try_get::<i64, _>("executed_at")? as u64,
                        deleted: row.try_get::<i64, _>("deleted")? != 0,
                    },
                );
        }
        Ok(meta)
    }

    async fn load_entity_page_rows(
        &self,
        kind: TableKind,
        _world_filters: &[Felt],
        table_ids: Option<&[String]>,
        entity_keys: &[(String, String)],
    ) -> Result<Vec<EntityModelRow>> {
        if entity_keys.is_empty() || table_ids.is_some_and(|table_ids| table_ids.is_empty()) {
            return Ok(Vec::new());
        }

        let mut builder = QueryBuilder::<Any>::new(
            "SELECT world_address, table_id, entity_id, row_json \
             FROM torii_ecs_entity_models WHERE kind = ",
        );
        builder.push_bind(kind.as_str());
        if let Some(table_ids) = table_ids {
            builder.push(" AND table_id IN (");
            {
                let mut separated = builder.separated(", ");
                for table_id in table_ids {
                    separated.push_bind(table_id);
                }
            }
            builder.push(")");
        }

        let world_addresses = entity_keys
            .iter()
            .map(|(world_address, _)| world_address.clone())
            .collect::<HashSet<_>>();
        let entity_ids = entity_keys
            .iter()
            .map(|(_, entity_id)| entity_id.clone())
            .collect::<HashSet<_>>();
        let allowed = entity_keys.iter().cloned().collect::<HashSet<_>>();

        builder.push(" AND world_address IN (");
        {
            let mut separated = builder.separated(", ");
            for world_address in &world_addresses {
                separated.push_bind(world_address);
            }
        }
        builder.push(")");
        builder.push(" AND entity_id IN (");
        {
            let mut separated = builder.separated(", ");
            for entity_id in &entity_ids {
                separated.push_bind(entity_id);
            }
        }
        builder.push(")");

        let rows = builder.build().fetch_all(&self.state.pool).await?;
        let mut output = Vec::new();
        for row in rows {
            let world_address: String = row.try_get("world_address")?;
            let entity_id: String = row.try_get("entity_id")?;
            if !allowed.contains(&(world_address.clone(), entity_id.clone())) {
                continue;
            }
            output.push(EntityModelRow {
                world_address,
                table_id: row.try_get("table_id")?,
                entity_id,
                row_json: row.try_get("row_json")?,
            });
        }
        Ok(output)
    }

    async fn load_entity_snapshot(
        &self,
        kind: TableKind,
        world_address: Felt,
        entity_id: Felt,
    ) -> Result<Option<EntityAggregate>> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "SELECT table_id, row_json
                 FROM torii_ecs_entity_models
                 WHERE kind = ?1 AND world_address = ?2 AND entity_id = ?3"
            }
            DbBackend::Postgres => {
                "SELECT table_id, row_json
                 FROM torii_ecs_entity_models
                 WHERE kind = $1 AND world_address = $2 AND entity_id = $3"
            }
        };
        let rows = sqlx::query(sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(entity_id))
            .fetch_all(&self.state.pool)
            .await?;
        if rows.is_empty() {
            return Ok(None);
        }

        let meta = self
            .load_entity_meta_rows(kind, world_address, entity_id)
            .await?;
        let managed_tables = self.load_managed_table_map().await?;
        let mut aggregate = EntityAggregate {
            world_address: world_address.to_bytes_be().to_vec(),
            hashed_keys: entity_id.to_bytes_be().to_vec(),
            ..EntityAggregate::default()
        };

        for row in rows {
            let table_id = row.try_get::<String, _>("table_id")?;
            let Some(table) = managed_tables.get(&table_id) else {
                continue;
            };
            let Some(table_meta) = meta.get(&table_id) else {
                continue;
            };
            if table_meta.deleted {
                continue;
            }

            let row_json: String = row.try_get("row_json")?;
            let value: Value = serde_json::from_str(&row_json)?;
            let object = value
                .as_object()
                .cloned()
                .ok_or_else(|| anyhow!("entity snapshot row must be an object"))?;
            aggregate
                .models
                .push(row_to_model_struct(&table.table, &object)?);
            if aggregate.created_at == 0 || table_meta.created_at < aggregate.created_at {
                aggregate.created_at = table_meta.created_at;
            }
            aggregate.updated_at = aggregate.updated_at.max(table_meta.updated_at);
            aggregate.executed_at = aggregate.executed_at.max(table_meta.executed_at);
        }

        if aggregate.models.is_empty() {
            return Ok(None);
        }
        Ok(Some(aggregate))
    }

    async fn load_entity_meta_rows(
        &self,
        kind: TableKind,
        world_address: Felt,
        entity_id: Felt,
    ) -> Result<HashMap<String, EntityMetaRow>> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => "SELECT table_id, created_at, updated_at, executed_at,
                        CAST(deleted AS INTEGER) AS deleted
                 FROM torii_ecs_entity_meta
                 WHERE kind = ? AND world_address = ? AND entity_id = ?"
                .to_string(),
            DbBackend::Postgres => "SELECT table_id, created_at, updated_at, executed_at,
                        CASE WHEN deleted THEN 1 ELSE 0 END AS deleted
                 FROM torii_ecs_entity_meta
                 WHERE kind = $1 AND world_address = $2 AND entity_id = $3"
                .to_string(),
        };
        let rows = sqlx::query(&sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(entity_id))
            .fetch_all(&self.state.pool)
            .await?;
        let mut meta = HashMap::new();
        for row in rows {
            meta.insert(
                row.try_get::<String, _>("table_id")?,
                EntityMetaRow {
                    created_at: row.try_get::<i64, _>("created_at")? as u64,
                    updated_at: row.try_get::<i64, _>("updated_at")? as u64,
                    executed_at: row.try_get::<i64, _>("executed_at")? as u64,
                    deleted: row.try_get::<i64, _>("deleted")? != 0,
                },
            );
        }
        Ok(meta)
    }

    async fn load_entity_meta(
        &self,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
    ) -> Result<HashMap<String, EntityMetaRow>> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => "SELECT entity_id, created_at, updated_at, executed_at,
                        CAST(deleted AS INTEGER) AS deleted
                 FROM torii_ecs_entity_meta
                 WHERE kind = ? AND world_address = ? AND table_id = ?"
                .to_string(),
            DbBackend::Postgres => "SELECT entity_id, created_at, updated_at, executed_at,
                        CASE WHEN deleted THEN 1 ELSE 0 END AS deleted
                 FROM torii_ecs_entity_meta
                 WHERE kind = $1 AND world_address = $2 AND table_id = $3"
                .to_string(),
        };
        let rows = sqlx::query(&sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(table_id))
            .fetch_all(&self.state.pool)
            .await?;
        let mut meta = HashMap::new();
        for row in rows {
            meta.insert(
                row.try_get::<String, _>("entity_id")?,
                EntityMetaRow {
                    created_at: row.try_get::<i64, _>("created_at")? as u64,
                    updated_at: row.try_get::<i64, _>("updated_at")? as u64,
                    executed_at: row.try_get::<i64, _>("executed_at")? as u64,
                    deleted: row.try_get::<i64, _>("deleted")? != 0,
                },
            );
        }
        Ok(meta)
    }

    async fn fetch_table_rows(&self, table: &ManagedTable) -> Result<Vec<Map<String, Value>>> {
        match self.state.backend {
            DbBackend::Sqlite => {
                let sql = format!(
                    "SELECT * FROM \"{}\"",
                    table.table.name.replace('"', "\"\"")
                );
                let rows = sqlx::query(&sql).fetch_all(&self.state.pool).await?;
                rows.into_iter()
                    .map(|row| any_row_to_json_map(&row, &table.table))
                    .collect()
            }
            DbBackend::Postgres => {
                let sql = format!(
                    "SELECT row_to_json(t)::text AS data FROM (SELECT * FROM \"{}\") t",
                    table.table.name.replace('"', "\"\"")
                );
                let rows = sqlx::query(&sql).fetch_all(&self.state.pool).await?;
                rows.into_iter()
                    .map(|row| {
                        let data: String = row.try_get("data")?;
                        let value: Value = serde_json::from_str(&data)?;
                        value
                            .as_object()
                            .cloned()
                            .ok_or_else(|| anyhow!("row_to_json must return an object"))
                    })
                    .collect()
            }
        }
    }

    async fn load_events_page(
        &self,
        query: &types::EventQuery,
    ) -> Result<(Vec<types::Event>, String)> {
        let pagination = query.pagination.clone().unwrap_or(types::Pagination {
            cursor: String::new(),
            limit: 100,
            direction: PaginationDirection::Forward as i32,
            order_by: Vec::new(),
        });
        let limit = if pagination.limit == 0 {
            100
        } else {
            pagination.limit as usize
        };
        let direction_is_backward = pagination.direction == PaginationDirection::Backward as i32;
        let order_sql = if direction_is_backward { "DESC" } else { "ASC" };

        let mut builder = QueryBuilder::<Any>::new(
            "SELECT event_id, transaction_hash, keys_json, data_json \
             FROM torii_ecs_events",
        );
        if !pagination.cursor.is_empty() {
            builder.push(" WHERE event_id ");
            builder.push(if direction_is_backward { "< " } else { "> " });
            builder.push_bind(pagination.cursor.clone());
        }
        builder.push(" ORDER BY event_id ");
        builder.push(order_sql);

        let fetch_limit = if query.keys.is_some() {
            limit.saturating_mul(20).max(100)
        } else {
            limit
        };
        builder.push(" LIMIT ");
        builder.push_bind(fetch_limit as i64);

        let rows = builder.build().fetch_all(&self.state.pool).await?;
        ::metrics::counter!("torii_ecs_retrieve_events_rows_fetched_total")
            .increment(rows.len() as u64);

        let mut events = Vec::new();
        for row in rows {
            let keys_hex: Vec<String> =
                serde_json::from_str(&row.try_get::<String, _>("keys_json")?)?;
            let data_hex: Vec<String> =
                serde_json::from_str(&row.try_get::<String, _>("data_json")?)?;
            let event = types::Event {
                keys: keys_hex
                    .into_iter()
                    .map(|value| felt_from_hex(&value).map(|felt| felt.to_bytes_be().to_vec()))
                    .collect::<Result<Vec<_>>>()?,
                data: data_hex
                    .into_iter()
                    .map(|value| felt_from_hex(&value).map(|felt| felt.to_bytes_be().to_vec()))
                    .collect::<Result<Vec<_>>>()?,
                transaction_hash: felt_from_hex(&row.try_get::<String, _>("transaction_hash")?)?
                    .to_bytes_be()
                    .to_vec(),
            };
            if query
                .keys
                .as_ref()
                .is_none_or(|keys| match_keys(&event.keys, std::slice::from_ref(keys)))
            {
                events.push((row.try_get::<String, _>("event_id")?, event));
            }
            if events.len() >= limit {
                break;
            }
        }
        ::metrics::counter!("torii_ecs_retrieve_events_rows_decoded_total")
            .increment(events.len() as u64);

        if direction_is_backward {
            events.reverse();
        }
        let next_cursor = events
            .last()
            .map(|(cursor, _)| cursor.clone())
            .unwrap_or_default();
        Ok((
            events.into_iter().map(|(_, event)| event).collect(),
            next_cursor,
        ))
    }

    async fn next_subscription_id() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    }
}

#[tonic::async_trait]
impl World for EcsService {
    type SubscribeContractsStream = ReceiverStream<Result<SubscribeContractsResponse, Status>>;
    type SubscribeEntitiesStream = ReceiverStream<Result<SubscribeEntityResponse, Status>>;
    type SubscribeEventMessagesStream = ReceiverStream<Result<SubscribeEntityResponse, Status>>;
    type SubscribeEventsStream = ReceiverStream<Result<SubscribeEventsResponse, Status>>;

    async fn subscribe_contracts(
        &self,
        request: Request<SubscribeContractsRequest>,
    ) -> Result<Response<Self::SubscribeContractsStream>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let (sender, receiver) = mpsc::channel(256);
        for contract in self.load_contracts(&query).await.map_err(internal_status)? {
            let _ = sender
                .send(Ok(SubscribeContractsResponse {
                    contract: Some(contract),
                }))
                .await;
        }
        self.state.contract_subscriptions.lock().await.insert(
            Self::next_subscription_id().await,
            ContractSubscription { query, sender },
        );
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn worlds(
        &self,
        request: Request<WorldsRequest>,
    ) -> Result<Response<WorldsResponse>, Status> {
        let start = Instant::now();
        let world_addresses = request
            .into_inner()
            .world_addresses
            .iter()
            .map(|bytes| felt_from_bytes(bytes))
            .collect::<Result<Vec<_>>>()
            .map_err(internal_status)?;
        let worlds = self.load_worlds(&world_addresses).await;
        ::metrics::histogram!("torii_ecs_worlds_latency_seconds")
            .record(start.elapsed().as_secs_f64());
        let worlds = worlds.map_err(internal_status)?;
        Ok(Response::new(WorldsResponse { worlds }))
    }

    async fn subscribe_entities(
        &self,
        request: Request<SubscribeEntitiesRequest>,
    ) -> Result<Response<Self::SubscribeEntitiesStream>, Status> {
        let request = request.into_inner();
        let subscription_id = Self::next_subscription_id().await;
        let (sender, receiver) = mpsc::channel(256);
        sender
            .send(Ok(SubscribeEntityResponse {
                entity: None,
                subscription_id,
            }))
            .await
            .map_err(|_| Status::internal("subscription setup failed"))?;
        self.state.entity_subscriptions.lock().await.insert(
            subscription_id,
            EntitySubscription {
                clause: request.clause,
                world_addresses: request.world_addresses.into_iter().collect(),
                sender,
            },
        );
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn update_entities_subscription(
        &self,
        request: Request<UpdateEntitiesSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        if let Some(subscription) = self
            .state
            .entity_subscriptions
            .lock()
            .await
            .get_mut(&request.subscription_id)
        {
            subscription.clause = request.clause;
            subscription.world_addresses = request.world_addresses.into_iter().collect();
        }
        Ok(Response::new(()))
    }

    async fn retrieve_entities(
        &self,
        request: Request<RetrieveEntitiesRequest>,
    ) -> Result<Response<RetrieveEntitiesResponse>, Status> {
        let start = Instant::now();
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let result = self.load_entity_page(TableKind::Entity, &query).await;
        ::metrics::histogram!("torii_ecs_retrieve_entities_latency_seconds")
            .record(start.elapsed().as_secs_f64());
        let (entities, next_cursor) = result.map_err(internal_status)?;
        ::metrics::counter!("torii_ecs_retrieve_entities_returned_total")
            .increment(entities.len() as u64);
        Ok(Response::new(RetrieveEntitiesResponse {
            next_cursor,
            entities,
        }))
    }

    async fn subscribe_event_messages(
        &self,
        request: Request<SubscribeEntitiesRequest>,
    ) -> Result<Response<Self::SubscribeEventMessagesStream>, Status> {
        let request = request.into_inner();
        let subscription_id = Self::next_subscription_id().await;
        let (sender, receiver) = mpsc::channel(256);
        sender
            .send(Ok(SubscribeEntityResponse {
                entity: None,
                subscription_id,
            }))
            .await
            .map_err(|_| Status::internal("subscription setup failed"))?;
        self.state.event_message_subscriptions.lock().await.insert(
            subscription_id,
            EntitySubscription {
                clause: request.clause,
                world_addresses: request.world_addresses.into_iter().collect(),
                sender,
            },
        );
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn update_event_messages_subscription(
        &self,
        request: Request<UpdateEntitiesSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        if let Some(subscription) = self
            .state
            .event_message_subscriptions
            .lock()
            .await
            .get_mut(&request.subscription_id)
        {
            subscription.clause = request.clause;
            subscription.world_addresses = request.world_addresses.into_iter().collect();
        }
        Ok(Response::new(()))
    }

    async fn retrieve_event_messages(
        &self,
        request: Request<RetrieveEntitiesRequest>,
    ) -> Result<Response<RetrieveEntitiesResponse>, Status> {
        let start = Instant::now();
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let result = self.load_entity_page(TableKind::EventMessage, &query).await;
        ::metrics::histogram!("torii_ecs_retrieve_event_messages_latency_seconds")
            .record(start.elapsed().as_secs_f64());
        let (entities, next_cursor) = result.map_err(internal_status)?;
        ::metrics::counter!("torii_ecs_retrieve_event_messages_returned_total")
            .increment(entities.len() as u64);
        Ok(Response::new(RetrieveEntitiesResponse {
            next_cursor,
            entities,
        }))
    }

    async fn retrieve_events(
        &self,
        request: Request<RetrieveEventsRequest>,
    ) -> Result<Response<RetrieveEventsResponse>, Status> {
        let start = Instant::now();
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let result = self.load_events_page(&query).await;
        ::metrics::histogram!("torii_ecs_retrieve_events_latency_seconds")
            .record(start.elapsed().as_secs_f64());
        let (events, next_cursor) = result.map_err(internal_status)?;
        ::metrics::counter!("torii_ecs_retrieve_events_returned_total")
            .increment(events.len() as u64);
        Ok(Response::new(RetrieveEventsResponse {
            next_cursor,
            events,
        }))
    }

    async fn subscribe_events(
        &self,
        request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        let request = request.into_inner();
        let (sender, receiver) = mpsc::channel(256);
        sender
            .send(Ok(SubscribeEventsResponse { event: None }))
            .await
            .map_err(|_| Status::internal("subscription setup failed"))?;
        self.state.event_subscriptions.lock().await.insert(
            Self::next_subscription_id().await,
            EventSubscription {
                keys: request.keys,
                sender,
            },
        );
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

#[derive(Clone, Copy)]
struct EntityMetaRow {
    created_at: u64,
    updated_at: u64,
    executed_at: u64,
    deleted: bool,
}

struct EntityModelRow {
    world_address: String,
    table_id: String,
    entity_id: String,
    row_json: String,
}

struct SnapshotJsonSerializer;

fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

fn sqlite_url(path: &str) -> Result<String> {
    if path == ":memory:" || path == "sqlite::memory:" {
        return Ok("sqlite::memory:".to_string());
    }
    if path.starts_with("sqlite:") {
        return Ok(path.to_string());
    }
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{path}"))
        .or_else(|_| Ok::<_, sqlx::Error>(SqliteConnectOptions::new().filename(path)))?;
    if let Some(parent) = options
        .get_filename()
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(options.to_url_lossy().to_string())
}

impl CairoTypeSerialization for SnapshotJsonSerializer {
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
        if type_def == &TypeDef::None {
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry("variant", name)?;
            map
        } else {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry("variant", name)?;
            map.serialize_entry(&format!("_{name}"), &type_def.to_de_se(data, self))?;
            map
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
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("is_ok", &is_ok)?;
        if is_ok {
            map.serialize_entry("Ok", &result.ok.to_de_se(data, self))?;
        } else {
            map.serialize_entry("Err", &result.err.to_de_se(data, self))?;
        }
        map.end()
    }
}

fn model_from_table(table: &ManagedTable) -> types::Model {
    let (namespace, name) = split_table_name(&table.table.name);
    types::Model {
        selector: table.table.id.to_bytes_be().to_vec(),
        namespace,
        name,
        packed_size: table.table.columns.len() as u32,
        unpacked_size: table.table.columns.len() as u32,
        class_hash: Vec::new(),
        layout: Vec::new(),
        schema: serde_json::to_vec(&table.table).unwrap_or_default(),
        contract_address: Vec::new(),
        use_legacy_store: table.table.legacy,
        world_address: table.world_address.to_bytes_be().to_vec(),
    }
}

fn split_table_name(name: &str) -> (String, String) {
    match name.split_once('-') {
        Some((namespace, model)) => (namespace.to_string(), model.to_string()),
        None => (String::new(), name.to_string()),
    }
}

fn row_to_model_struct(table: &DojoTable, row: &Map<String, Value>) -> Result<types::Struct> {
    let children = table
        .key_fields
        .iter()
        .chain(table.value_fields.iter())
        .filter_map(|column_id| table.columns.get(column_id))
        .map(|column| {
            let value = row.get(&column.name).cloned().unwrap_or(Value::Null);
            Ok(types::Member {
                name: column.name.clone(),
                ty: Some(type_to_proto_value(&column.type_def, value)?),
                key: column.attributes.has_attribute("key"),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(types::Struct {
        name: table.name.clone(),
        children,
    })
}

fn record_to_json_map(
    table: &DojoTable,
    columns: &[Felt],
    record: &Record,
) -> Result<Map<String, Value>> {
    let schema_table = table.to_schema();
    let columns_by_id = schema_table
        .columns
        .iter()
        .cloned()
        .map(|column| (column.id, column))
        .collect::<HashMap<_, _>>();
    let schema_columns = columns
        .iter()
        .map(|column_id| {
            columns_by_id
                .get(column_id)
                .ok_or_else(|| anyhow!("column {column_id:#x} not found in table {}", table.name))
        })
        .collect::<Result<Vec<&ColumnDef>>>()?;
    let schema = torii_introspect::tables::RecordSchema::new(&schema_table.primary, schema_columns);

    let mut bytes = Vec::new();
    let mut serializer = JsonSerializer::new(&mut bytes);
    schema.parse_records_with_metadata(
        std::slice::from_ref(record),
        &(),
        &mut serializer,
        &SnapshotJsonSerializer,
    )?;

    let value = serde_json::from_slice::<Vec<Value>>(&bytes)?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("record serializer returned no rows"))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("record serializer must return an object"))
}

fn build_entity_from_snapshot(
    key: &(String, String),
    meta: &HashMap<String, EntityMetaRow>,
    rows: Option<&Vec<(String, String)>>,
    models: &HashMap<String, ManagedTable>,
) -> Result<Option<EntityAggregate>> {
    let Some(rows) = rows else {
        return Ok(None);
    };

    let mut aggregate = EntityAggregate {
        world_address: felt_from_hex(&key.0)?.to_bytes_be().to_vec(),
        hashed_keys: felt_from_hex(&key.1)?.to_bytes_be().to_vec(),
        ..EntityAggregate::default()
    };

    for (table_id, row_json) in rows {
        let Some(table_meta) = meta.get(table_id) else {
            continue;
        };
        if table_meta.deleted {
            continue;
        }
        let Some(table) = models.get(table_id) else {
            continue;
        };

        let value: Value = serde_json::from_str(row_json)?;
        let object = value
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow!("entity snapshot row must be an object"))?;
        aggregate
            .models
            .push(row_to_model_struct(&table.table, &object)?);
        if aggregate.created_at == 0 || table_meta.created_at < aggregate.created_at {
            aggregate.created_at = table_meta.created_at;
        }
        aggregate.updated_at = aggregate.updated_at.max(table_meta.updated_at);
        aggregate.executed_at = aggregate.executed_at.max(table_meta.executed_at);
    }

    if aggregate.models.is_empty() {
        return Ok(None);
    }
    Ok(Some(aggregate))
}

fn any_row_to_json_map(row: &sqlx::any::AnyRow, table: &DojoTable) -> Result<Map<String, Value>> {
    let mut map = Map::new();
    let primary_name = &table.primary.name;
    map.insert(
        primary_name.clone(),
        read_row_value(row, primary_name, &TypeDef::from(&table.primary.type_def))?,
    );
    for column in table.columns.values() {
        map.insert(
            column.name.clone(),
            read_row_value(row, &column.name, &column.type_def)?,
        );
    }
    Ok(map)
}

fn read_row_value(row: &sqlx::any::AnyRow, name: &str, type_def: &TypeDef) -> Result<Value> {
    if is_integer_type(type_def) {
        let value = row.try_get::<Option<i64>, _>(name)?;
        return Ok(value.map_or(Value::Null, |value| Value::Number(value.into())));
    }

    let value = row.try_get::<Option<String>, _>(name)?;
    let Some(value) = value else {
        return Ok(Value::Null);
    };
    if is_json_text_type(type_def) {
        Ok(serde_json::from_str(&value).unwrap_or(Value::String(value)))
    } else {
        Ok(Value::String(value))
    }
}

fn is_integer_type(type_def: &TypeDef) -> bool {
    matches!(
        type_def,
        TypeDef::Bool
            | TypeDef::I8
            | TypeDef::I16
            | TypeDef::I32
            | TypeDef::I64
            | TypeDef::U8
            | TypeDef::U16
            | TypeDef::U32
    )
}

fn is_json_text_type(type_def: &TypeDef) -> bool {
    matches!(
        type_def,
        TypeDef::Tuple(_)
            | TypeDef::Array(_)
            | TypeDef::FixedArray(_)
            | TypeDef::Struct(_)
            | TypeDef::Enum(_)
            | TypeDef::Option(_)
            | TypeDef::Nullable(_)
            | TypeDef::Result(_)
            | TypeDef::Felt252Dict(_)
    )
}

fn type_to_proto_value(type_def: &TypeDef, value: Value) -> Result<types::Ty> {
    use types::ty::TyType;

    let ty_type = match type_def {
        TypeDef::Bool => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::Bool(
                value.as_bool().unwrap_or_default(),
            )),
        }),
        TypeDef::I8 | TypeDef::I16 | TypeDef::I32 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::I32(
                value.as_i64().unwrap_or_default() as i32,
            )),
        }),
        TypeDef::I64 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::I64(
                value.as_i64().unwrap_or_default(),
            )),
        }),
        TypeDef::U8 | TypeDef::U16 | TypeDef::U32 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::U32(
                value.as_u64().unwrap_or_default() as u32,
            )),
        }),
        TypeDef::U64 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::U64(value_as_u64(&value))),
        }),
        TypeDef::Felt252
        | TypeDef::ClassHash
        | TypeDef::ContractAddress
        | TypeDef::StorageAddress
        | TypeDef::StorageBaseAddress
        | TypeDef::EthAddress => {
            let bytes = value_as_felt_bytes(&value)?;
            let primitive = match type_def {
                TypeDef::ClassHash => types::primitive::PrimitiveType::ClassHash(bytes),
                TypeDef::ContractAddress
                | TypeDef::StorageAddress
                | TypeDef::StorageBaseAddress => {
                    types::primitive::PrimitiveType::ContractAddress(bytes)
                }
                TypeDef::EthAddress => types::primitive::PrimitiveType::EthAddress(bytes),
                _ => types::primitive::PrimitiveType::Felt252(bytes),
            };
            TyType::Primitive(types::Primitive {
                primitive_type: Some(primitive),
            })
        }
        TypeDef::U128 | TypeDef::I128 | TypeDef::U256 | TypeDef::U512 => {
            TyType::Primitive(types::Primitive {
                primitive_type: Some(types::primitive::PrimitiveType::U256(value_as_bytes(
                    &value,
                ))),
            })
        }
        TypeDef::Utf8String
        | TypeDef::ShortUtf8
        | TypeDef::ByteArray
        | TypeDef::ByteArrayEncoded(_)
        | TypeDef::Bytes31
        | TypeDef::Bytes31Encoded(_) => TyType::Bytearray(value_as_string(&value)),
        TypeDef::Struct(def) => {
            let object = value.as_object().cloned().unwrap_or_default();
            TyType::Struct(types::Struct {
                name: def.name.clone(),
                children: def
                    .members
                    .iter()
                    .map(|member| {
                        Ok(types::Member {
                            name: member.name.clone(),
                            ty: Some(type_to_proto_value(
                                &member.type_def,
                                object.get(&member.name).cloned().unwrap_or(Value::Null),
                            )?),
                            key: member.attributes.has_attribute("key"),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        TypeDef::Tuple(def) => TyType::Tuple(types::Array {
            children: value
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .zip(def.elements.iter())
                .map(|(item, inner)| type_to_proto_value(inner, item))
                .collect::<Result<Vec<_>>>()?,
        }),
        TypeDef::Array(def) => TyType::Array(types::Array {
            children: value
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|item| type_to_proto_value(&def.type_def, item))
                .collect::<Result<Vec<_>>>()?,
        }),
        TypeDef::FixedArray(def) => TyType::FixedSizeArray(types::FixedSizeArray {
            children: value
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|item| type_to_proto_value(&def.type_def, item))
                .collect::<Result<Vec<_>>>()?,
            size: def.size,
        }),
        TypeDef::Enum(def) => {
            let object = value.as_object().cloned().unwrap_or_default();
            let variant_name = object
                .get("option")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let (option, options) = def
                .order
                .iter()
                .enumerate()
                .filter_map(|(index, selector)| {
                    def.variants.get(selector).map(|variant| (index, variant))
                })
                .map(|(index, variant)| {
                    let ty = object
                        .get(&variant.name)
                        .cloned()
                        .map(|value| type_to_proto_value(&variant.type_def, value))
                        .transpose()?
                        .unwrap_or_default();
                    Ok::<_, anyhow::Error>((
                        if variant.name == variant_name {
                            index as u32
                        } else {
                            u32::MAX
                        },
                        types::EnumOption {
                            name: variant.name.clone(),
                            ty: Some(ty),
                        },
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .fold(
                    (0, Vec::new()),
                    |(selected, mut options), (index, option)| {
                        let selected = if index == u32::MAX { selected } else { index };
                        options.push(option);
                        (selected, options)
                    },
                );
            TyType::Enum(types::Enum {
                name: def.name.clone(),
                option,
                options,
            })
        }
        TypeDef::Option(def) => {
            if value.is_null() {
                TyType::Bytearray(String::new())
            } else {
                return type_to_proto_value(&def.type_def, value);
            }
        }
        TypeDef::Nullable(def) => {
            if value.is_null() {
                TyType::Bytearray(String::new())
            } else {
                return type_to_proto_value(&def.type_def, value);
            }
        }
        TypeDef::Result(def) => {
            let object = value.as_object().cloned().unwrap_or_default();
            if let Some(ok) = object.get("Ok").cloned() {
                return type_to_proto_value(&def.ok, ok);
            }
            if let Some(err) = object.get("Err").cloned() {
                return type_to_proto_value(&def.err, err);
            }
            TyType::Bytearray(value_as_string(&value))
        }
        TypeDef::None | TypeDef::Felt252Dict(_) | TypeDef::Ref(_) | TypeDef::Custom(_) => {
            TyType::Bytearray(value_as_string(&value))
        }
    };

    Ok(types::Ty {
        ty_type: Some(ty_type),
    })
}

fn value_as_u64(value: &Value) -> u64 {
    value
        .as_u64()
        .or_else(|| value.as_i64().map(|value| value as u64))
        .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
        .unwrap_or_default()
}

fn value_as_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn value_as_bytes(value: &Value) -> Vec<u8> {
    value.as_str().map_or_else(
        || value_as_string(value).into_bytes(),
        |value| value.as_bytes().to_vec(),
    )
}

fn value_as_felt_bytes(value: &Value) -> Result<Vec<u8>> {
    if value.is_null() {
        return Ok(Felt::ZERO.to_bytes_be().to_vec());
    }

    let string = value_as_string(value);
    let trimmed = string.trim();
    if trimmed.is_empty() {
        return Ok(Felt::ZERO.to_bytes_be().to_vec());
    }

    Ok(felt_from_hex(trimmed)?.to_bytes_be().to_vec())
}

fn value_to_primary_felt(value: &Value, type_def: &PrimaryTypeDef) -> Result<Felt> {
    match type_def {
        PrimaryTypeDef::Bool => Ok(Felt::from(value.as_bool().unwrap_or_default() as u64)),
        PrimaryTypeDef::U8
        | PrimaryTypeDef::U16
        | PrimaryTypeDef::U32
        | PrimaryTypeDef::U64
        | PrimaryTypeDef::U128
        | PrimaryTypeDef::I8
        | PrimaryTypeDef::I16
        | PrimaryTypeDef::I32
        | PrimaryTypeDef::I64
        | PrimaryTypeDef::I128 => Ok(Felt::from(value_as_u64(value))),
        _ => felt_from_hex(&value_as_string(value)),
    }
}

fn member_pushdown_from_clause(
    clause: Option<&types::Clause>,
    models: &HashMap<String, ManagedTable>,
) -> Option<MemberPushdown> {
    let ClauseType::Member(member) = clause?.clause_type.as_ref()? else {
        return None;
    };
    if member.member.contains('.') {
        return None;
    }
    if !member
        .member
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return None;
    }

    let expected = scalar_from_member_value(member.value.as_ref()?)?;
    if matches!(expected, ScalarValue::Bytes(_)) {
        return None;
    }

    let table = models
        .values()
        .find(|table| table.table.name == member.model)?;
    if !table
        .table
        .columns
        .values()
        .any(|column| column.name == member.member)
    {
        return None;
    }

    Some(MemberPushdown {
        table_id: felt_hex(table.table.id),
        member: member.member.clone(),
        operator: member.operator,
        expected,
    })
}

fn sql_operator(operator: i32) -> Option<&'static str> {
    match operator {
        value if value == ComparisonOperator::Eq as i32 => Some("="),
        value if value == ComparisonOperator::Neq as i32 => Some("!="),
        value if value == ComparisonOperator::Gt as i32 => Some(">"),
        value if value == ComparisonOperator::Gte as i32 => Some(">="),
        value if value == ComparisonOperator::Lt as i32 => Some("<"),
        value if value == ComparisonOperator::Lte as i32 => Some("<="),
        _ => None,
    }
}

fn append_member_pushdown_sql(
    builder: &mut QueryBuilder<'_, Any>,
    backend: DbBackend,
    pushdown: &MemberPushdown,
) {
    let Some(operator) = sql_operator(pushdown.operator) else {
        return;
    };

    builder.push(" AND EXISTS (SELECT 1 FROM torii_ecs_entity_models em WHERE em.kind = m.kind");
    builder.push(
        " AND em.world_address = m.world_address AND em.table_id = m.table_id AND em.entity_id = m.entity_id",
    );
    builder.push(" AND em.table_id = ");
    builder.push_bind(pushdown.table_id.clone());

    match (&pushdown.expected, backend) {
        (ScalarValue::Bool(value), DbBackend::Sqlite) => {
            builder.push(" AND CAST(json_extract(em.row_json, '$.");
            builder.push(pushdown.member.as_str());
            builder.push("') AS INTEGER) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(i64::from(*value));
        }
        (ScalarValue::Bool(value), DbBackend::Postgres) => {
            builder.push(" AND CASE WHEN lower((em.row_json::jsonb ->> '");
            builder.push(pushdown.member.as_str());
            builder.push("')) = 'true' THEN 1 ELSE 0 END ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(i64::from(*value));
        }
        (ScalarValue::Signed(value), DbBackend::Sqlite) => {
            builder.push(" AND CAST(json_extract(em.row_json, '$.");
            builder.push(pushdown.member.as_str());
            builder.push("') AS INTEGER) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(*value);
        }
        (ScalarValue::Signed(value), DbBackend::Postgres) => {
            builder.push(" AND CAST((em.row_json::jsonb ->> '");
            builder.push(pushdown.member.as_str());
            builder.push("') AS BIGINT) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(*value);
        }
        (ScalarValue::Unsigned(value), DbBackend::Sqlite) => {
            builder.push(" AND CAST(json_extract(em.row_json, '$.");
            builder.push(pushdown.member.as_str());
            builder.push("') AS INTEGER) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(*value as i64);
        }
        (ScalarValue::Unsigned(value), DbBackend::Postgres) => {
            builder.push(" AND CAST((em.row_json::jsonb ->> '");
            builder.push(pushdown.member.as_str());
            builder.push("') AS BIGINT) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(*value as i64);
        }
        (ScalarValue::Text(value), DbBackend::Sqlite) => {
            builder.push(" AND CAST(json_extract(em.row_json, '$.");
            builder.push(pushdown.member.as_str());
            builder.push("') AS TEXT) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(value.clone());
        }
        (ScalarValue::Text(value), DbBackend::Postgres) => {
            builder.push(" AND (em.row_json::jsonb ->> '");
            builder.push(pushdown.member.as_str());
            builder.push("') ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(value.clone());
        }
        (ScalarValue::Bytes(_), _) => {}
    }

    builder.push(")");
}

fn entity_matches_clause(entity: &types::Entity, clause: &types::Clause) -> bool {
    match &clause.clause_type {
        Some(ClauseType::HashedKeys(hashed)) => {
            hashed.hashed_keys.is_empty() || hashed.hashed_keys.contains(&entity.hashed_keys)
        }
        Some(ClauseType::Keys(keys)) => {
            if !keys.models.is_empty()
                && !entity
                    .models
                    .iter()
                    .any(|model| keys.models.contains(&model.name))
            {
                return false;
            }
            let entity_keys = entity_key_values(entity);
            if keys.pattern_matching == PatternMatching::FixedLen as i32
                && entity_keys.len() != keys.keys.len()
            {
                return false;
            }
            entity_keys.iter().enumerate().all(|(index, key)| {
                keys.keys
                    .get(index)
                    .is_none_or(|expected| expected.is_empty() || expected == key)
            })
        }
        Some(ClauseType::Member(member)) => entity.models.iter().any(|model| {
            model.name == member.model
                && model_member_matches(
                    model,
                    &member.member,
                    member.operator,
                    member.value.as_ref(),
                )
        }),
        Some(ClauseType::Composite(composite)) => match composite.operator {
            value if value == LogicalOperator::And as i32 => composite
                .clauses
                .iter()
                .all(|clause| entity_matches_clause(entity, clause)),
            _ => composite
                .clauses
                .iter()
                .any(|clause| entity_matches_clause(entity, clause)),
        },
        None => true,
    }
}

fn match_keys(keys: &[Vec<u8>], clauses: &[types::KeysClause]) -> bool {
    clauses.is_empty()
        || clauses.iter().any(|clause| {
            if clause.pattern_matching == PatternMatching::FixedLen as i32
                && keys.len() != clause.keys.len()
            {
                return false;
            }
            keys.iter().enumerate().all(|(index, key)| {
                clause
                    .keys
                    .get(index)
                    .is_none_or(|expected| expected.is_empty() || expected == key)
            })
        })
}

fn model_member_matches(
    model: &types::Struct,
    path: &str,
    operator: i32,
    value: Option<&types::MemberValue>,
) -> bool {
    let mut current = find_member_value(model, path);
    let Some(current) = current.take() else {
        return false;
    };
    let Some(value) = value else {
        return false;
    };
    compare_member_value(current, operator, value)
}

fn find_member_value<'a>(model: &'a types::Struct, path: &str) -> Option<&'a types::Ty> {
    let mut members = &model.children;
    let parts = path.split('.').collect::<Vec<_>>();
    let mut current = None;
    for (index, part) in parts.iter().enumerate() {
        let member = members.iter().find(|member| member.name == *part)?;
        current = member.ty.as_ref();
        if index + 1 == parts.len() {
            return current;
        }
        let ty = current?.ty_type.as_ref()?;
        match ty {
            types::ty::TyType::Struct(value) => {
                members = &value.children;
            }
            _ => return None,
        }
    }
    current
}

fn compare_member_value(current: &types::Ty, operator: i32, expected: &types::MemberValue) -> bool {
    let current_value = scalar_from_ty(current);
    let expected_value = scalar_from_member_value(expected);
    match (current_value, expected_value) {
        (Some(current), Some(expected)) => compare_scalars(&current, operator, &expected),
        _ => false,
    }
}

fn scalar_from_ty(value: &types::Ty) -> Option<ScalarValue> {
    match value.ty_type.as_ref()? {
        types::ty::TyType::Primitive(primitive) => match primitive.primitive_type.as_ref()? {
            types::primitive::PrimitiveType::Bool(value) => Some(ScalarValue::Bool(*value)),
            types::primitive::PrimitiveType::I32(value) => Some(ScalarValue::Signed(*value as i64)),
            types::primitive::PrimitiveType::I64(value) => Some(ScalarValue::Signed(*value)),
            types::primitive::PrimitiveType::U32(value) => {
                Some(ScalarValue::Unsigned(*value as u64))
            }
            types::primitive::PrimitiveType::U64(value) => Some(ScalarValue::Unsigned(*value)),
            types::primitive::PrimitiveType::Felt252(value)
            | types::primitive::PrimitiveType::ClassHash(value)
            | types::primitive::PrimitiveType::ContractAddress(value)
            | types::primitive::PrimitiveType::EthAddress(value)
            | types::primitive::PrimitiveType::U256(value)
            | types::primitive::PrimitiveType::U128(value)
            | types::primitive::PrimitiveType::I128(value) => {
                Some(ScalarValue::Bytes(value.clone()))
            }
            _ => None,
        },
        types::ty::TyType::Bytearray(value) => Some(ScalarValue::Text(value.clone())),
        _ => None,
    }
}

fn scalar_from_member_value(value: &types::MemberValue) -> Option<ScalarValue> {
    match value.value_type.as_ref()? {
        ValueType::Primitive(primitive) => scalar_from_ty(&types::Ty {
            ty_type: Some(types::ty::TyType::Primitive(primitive.clone())),
        }),
        ValueType::String(value) => Some(ScalarValue::Text(value.clone())),
        ValueType::List(_) => None,
    }
}

#[derive(Clone)]
enum ScalarValue {
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Text(String),
    Bytes(Vec<u8>),
}

struct MemberPushdown {
    table_id: String,
    member: String,
    operator: i32,
    expected: ScalarValue,
}

fn compare_scalars(current: &ScalarValue, operator: i32, expected: &ScalarValue) -> bool {
    match (current, expected) {
        (ScalarValue::Bool(current), ScalarValue::Bool(expected)) => match operator {
            value if value == ComparisonOperator::Eq as i32 => current == expected,
            value if value == ComparisonOperator::Neq as i32 => current != expected,
            _ => false,
        },
        (ScalarValue::Signed(current), ScalarValue::Signed(expected)) => {
            compare_ord(current, operator, expected)
        }
        (ScalarValue::Unsigned(current), ScalarValue::Unsigned(expected)) => {
            compare_ord(current, operator, expected)
        }
        (ScalarValue::Text(current), ScalarValue::Text(expected)) => {
            compare_ord(current, operator, expected)
        }
        (ScalarValue::Bytes(current), ScalarValue::Bytes(expected)) => {
            compare_ord(current, operator, expected)
        }
        _ => false,
    }
}

fn compare_ord<T: PartialEq + PartialOrd>(current: &T, operator: i32, expected: &T) -> bool {
    match operator {
        value if value == ComparisonOperator::Eq as i32 => current == expected,
        value if value == ComparisonOperator::Neq as i32 => current != expected,
        value if value == ComparisonOperator::Gt as i32 => current > expected,
        value if value == ComparisonOperator::Gte as i32 => current >= expected,
        value if value == ComparisonOperator::Lt as i32 => current < expected,
        value if value == ComparisonOperator::Lte as i32 => current <= expected,
        _ => false,
    }
}

fn entity_key_values(entity: &types::Entity) -> Vec<Vec<u8>> {
    entity
        .models
        .iter()
        .find_map(|model| {
            let keys = model
                .children
                .iter()
                .filter(|member| member.key)
                .filter_map(|member| {
                    scalar_from_ty(member.ty.as_ref()?).map(|value| match value {
                        ScalarValue::Bool(value) => vec![u8::from(value)],
                        ScalarValue::Signed(value) => value.to_be_bytes().to_vec(),
                        ScalarValue::Unsigned(value) => value.to_be_bytes().to_vec(),
                        ScalarValue::Text(value) => value.into_bytes(),
                        ScalarValue::Bytes(value) => value,
                    })
                })
                .collect::<Vec<_>>();
            (!keys.is_empty()).then_some(keys)
        })
        .unwrap_or_default()
}

fn contract_matches_query(contract: &types::Contract, query: &types::ContractQuery) -> bool {
    (query.contract_addresses.is_empty()
        || query
            .contract_addresses
            .contains(&contract.contract_address))
        && (query.contract_types.is_empty()
            || query.contract_types.contains(&contract.contract_type))
}

fn felt_hex(value: Felt) -> String {
    format!("{value:#x}")
}

fn felt_from_hex(value: &str) -> Result<Felt> {
    Felt::from_str(value).map_err(|err| anyhow!("invalid felt {value}: {err}"))
}

fn felt_from_bytes(value: &[u8]) -> Result<Felt> {
    Ok(Felt::from_bytes_be_slice(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use introspect_types::{ColumnDef, PrimaryDef, PrimaryTypeDef, TypeDef};
    use tokio::time::{timeout, Duration};
    use tokio_stream::StreamExt;

    fn test_db_path(name: &str) -> String {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        format!("sqlite:file:torii-ecs-sink-{name}-{nonce}?mode=memory&cache=shared")
    }

    fn member_bool_clause(model: &str, member: &str, value: bool) -> types::Clause {
        types::Clause {
            clause_type: Some(ClauseType::Member(types::MemberClause {
                model: model.to_string(),
                member: member.to_string(),
                operator: ComparisonOperator::Eq as i32,
                value: Some(types::MemberValue {
                    value_type: Some(ValueType::Primitive(types::Primitive {
                        primitive_type: Some(types::primitive::PrimitiveType::Bool(value)),
                    })),
                }),
            })),
        }
    }

    async fn seed_entity(
        service: &EcsService,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
        table_name: &str,
        entity_id: Felt,
        open: bool,
    ) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dojo_tables (
                owner BLOB NOT NULL,
                id BLOB NOT NULL,
                name TEXT NOT NULL,
                attributes TEXT NOT NULL,
                keys_json TEXT NOT NULL,
                values_json TEXT NOT NULL,
                legacy INTEGER NOT NULL,
                created_at INTEGER,
                updated_at INTEGER,
                created_block INTEGER,
                updated_block INTEGER,
                created_tx BLOB,
                updated_tx BLOB,
                PRIMARY KEY(owner, id)
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create dojo_tables");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dojo_columns (
                owner BLOB NOT NULL,
                table_id BLOB NOT NULL,
                id BLOB NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY(owner, table_id, id)
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create dojo_columns");

        let table = CreateTable {
            id: table_id,
            name: table_name.to_string(),
            attributes: vec![],
            primary: PrimaryDef {
                name: "entity_id".to_string(),
                attributes: vec![],
                type_def: PrimaryTypeDef::Felt252,
            },
            columns: vec![ColumnDef {
                id: Felt::from(1_u64),
                name: "open".to_string(),
                attributes: vec![],
                type_def: TypeDef::Bool,
            }],
        };

        service.cache_created_table(world_address, &table).await;
        service
            .record_table_kind(world_address, table_id, kind)
            .await
            .expect("record table kind");
        service
            .upsert_entity_meta(kind, world_address, table_id, entity_id, 1, false)
            .await
            .expect("upsert entity meta");

        let row_json = serde_json::json!({ "open": open }).to_string();
        sqlx::query(
            "INSERT INTO torii_ecs_entity_models (
                kind, world_address, table_id, entity_id, row_json, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                row_json = excluded.row_json,
                updated_at = excluded.updated_at",
        )
        .bind(kind.as_str())
        .bind(felt_hex(world_address))
        .bind(felt_hex(table_id))
        .bind(felt_hex(entity_id))
        .bind(row_json)
        .bind(1_i64)
        .execute(&service.state.pool)
        .await
        .expect("insert entity model");
    }

    #[tokio::test]
    async fn subscribe_entities_update_flow() {
        let db_path = test_db_path("entities-sub");
        let service = EcsService::new(&db_path, Some(1))
            .await
            .expect("service init");
        let world = Felt::from(10_u64);
        let table = Felt::from(20_u64);
        let entity = Felt::from(30_u64);
        seed_entity(
            &service,
            TableKind::Entity,
            world,
            table,
            "test-Lobby",
            entity,
            true,
        )
        .await;

        let response = service
            .subscribe_entities(Request::new(SubscribeEntitiesRequest {
                clause: Some(member_bool_clause("test-Lobby", "open", true)),
                world_addresses: vec![world.to_bytes_be().to_vec()],
            }))
            .await
            .expect("subscribe entities");
        let mut stream = response.into_inner();

        let setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        let subscription_id = setup.subscription_id;
        assert!(setup.entity.is_none());

        service
            .publish_entity_update(TableKind::Entity, world, entity)
            .await
            .expect("publish entity update");
        let matched = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("entity timeout")
            .expect("entity frame")
            .expect("entity ok");
        assert_eq!(matched.subscription_id, subscription_id);
        let matched_entity = matched.entity.expect("entity payload");
        assert_eq!(matched_entity.world_address, world.to_bytes_be().to_vec());

        service
            .update_entities_subscription(Request::new(UpdateEntitiesSubscriptionRequest {
                subscription_id,
                clause: Some(member_bool_clause("test-Lobby", "open", false)),
                world_addresses: vec![world.to_bytes_be().to_vec()],
            }))
            .await
            .expect("update entities subscription");

        service
            .publish_entity_update(TableKind::Entity, world, entity)
            .await
            .expect("publish filtered entity update");
        let filtered = timeout(Duration::from_millis(150), stream.next()).await;
        assert!(filtered.is_err(), "entity should be filtered out");
    }

    #[tokio::test]
    async fn subscribe_event_messages_update_flow() {
        let db_path = test_db_path("event-messages-sub");
        let service = EcsService::new(&db_path, Some(1))
            .await
            .expect("service init");
        let world = Felt::from(101_u64);
        let table = Felt::from(201_u64);
        let entity = Felt::from(301_u64);
        seed_entity(
            &service,
            TableKind::EventMessage,
            world,
            table,
            "test-EventMessage",
            entity,
            true,
        )
        .await;

        let response = service
            .subscribe_event_messages(Request::new(SubscribeEntitiesRequest {
                clause: Some(member_bool_clause("test-EventMessage", "open", true)),
                world_addresses: vec![world.to_bytes_be().to_vec()],
            }))
            .await
            .expect("subscribe event messages");
        let mut stream = response.into_inner();

        let setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        let subscription_id = setup.subscription_id;
        assert!(setup.entity.is_none());

        service
            .publish_entity_update(TableKind::EventMessage, world, entity)
            .await
            .expect("publish event message update");
        let matched = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("event message timeout")
            .expect("event message frame")
            .expect("event message ok");
        assert_eq!(matched.subscription_id, subscription_id);
        assert!(matched.entity.is_some());

        service
            .update_event_messages_subscription(Request::new(UpdateEntitiesSubscriptionRequest {
                subscription_id,
                clause: Some(member_bool_clause("test-EventMessage", "open", false)),
                world_addresses: vec![world.to_bytes_be().to_vec()],
            }))
            .await
            .expect("update event messages subscription");

        service
            .publish_entity_update(TableKind::EventMessage, world, entity)
            .await
            .expect("publish filtered event message update");
        let filtered = timeout(Duration::from_millis(150), stream.next()).await;
        assert!(filtered.is_err(), "event message should be filtered out");
    }

    #[tokio::test]
    async fn subscribe_events_keys_filter() {
        let db_path = test_db_path("events-sub");
        let service = EcsService::new(&db_path, Some(1))
            .await
            .expect("service init");
        let world = Felt::from(1_u64);
        let tx1 = Felt::from(2_u64);
        let tx2 = Felt::from(3_u64);
        let key_match = Felt::from(444_u64);

        let response = service
            .subscribe_events(Request::new(SubscribeEventsRequest {
                keys: vec![types::KeysClause {
                    keys: vec![key_match.to_bytes_be().to_vec()],
                    pattern_matching: PatternMatching::VariableLen as i32,
                    models: vec![],
                }],
            }))
            .await
            .expect("subscribe events");
        let mut stream = response.into_inner();

        let setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        assert!(setup.event.is_none());

        service
            .store_event(
                world,
                tx1,
                1,
                1,
                &[Felt::from(999_u64)],
                &[Felt::from(7_u64)],
                0,
            )
            .await
            .expect("store non matching event");
        let no_match = timeout(Duration::from_millis(150), stream.next()).await;
        assert!(
            no_match.is_err(),
            "non matching event should be filtered out"
        );

        service
            .store_event(world, tx2, 1, 1, &[key_match, Felt::from(123_u64)], &[], 1)
            .await
            .expect("store matching event");
        let matched = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("matched timeout")
            .expect("matched frame")
            .expect("matched ok");
        let event = matched.event.expect("event payload");
        assert_eq!(event.transaction_hash, tx2.to_bytes_be().to_vec());
        assert_eq!(
            event.keys.first().cloned(),
            Some(key_match.to_bytes_be().to_vec())
        );
    }

    #[tokio::test]
    async fn subscribe_contracts_query_filter() {
        let db_path = test_db_path("contracts-sub");
        let service = EcsService::new(&db_path, Some(1))
            .await
            .expect("service init");
        let world_contract = Felt::from(1111_u64);
        let other_contract = Felt::from(2222_u64);

        let response = service
            .subscribe_contracts(Request::new(SubscribeContractsRequest {
                query: Some(types::ContractQuery {
                    contract_addresses: vec![world_contract.to_bytes_be().to_vec()],
                    contract_types: vec![ContractType::World as i32],
                }),
            }))
            .await
            .expect("subscribe contracts");
        let mut stream = response.into_inner();

        service
            .record_contract_progress(other_contract, ContractType::World, 7, 77, None)
            .await
            .expect("record other contract progress");
        let no_match = timeout(Duration::from_millis(150), stream.next()).await;
        assert!(
            no_match.is_err(),
            "non matching contract should be filtered out"
        );

        service
            .record_contract_progress(world_contract, ContractType::World, 9, 99, None)
            .await
            .expect("record world contract progress");
        let matched = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("matched timeout")
            .expect("matched frame")
            .expect("matched ok");
        let contract = matched.contract.expect("contract payload");
        assert_eq!(
            contract.contract_address,
            world_contract.to_bytes_be().to_vec()
        );
        assert_eq!(contract.contract_type, ContractType::World as i32);
    }

    #[tokio::test]
    async fn retrieve_entities_without_filters_uses_bounded_query() {
        let db_path = test_db_path("retrieve-entities");
        let service = EcsService::new(&db_path, Some(1))
            .await
            .expect("service init");
        let world = Felt::from(900_u64);

        seed_entity(
            &service,
            TableKind::Entity,
            world,
            Felt::from(901_u64),
            "test-ModelA",
            Felt::from(1001_u64),
            true,
        )
        .await;
        seed_entity(
            &service,
            TableKind::Entity,
            world,
            Felt::from(902_u64),
            "test-ModelB",
            Felt::from(1001_u64),
            true,
        )
        .await;

        let response = service
            .retrieve_entities(Request::new(RetrieveEntitiesRequest {
                query: Some(types::Query {
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 10,
                        direction: PaginationDirection::Forward as i32,
                        order_by: vec![],
                    }),
                    world_addresses: vec![],
                    models: vec![],
                    clause: None,
                    no_hashed_keys: false,
                    historical: false,
                }),
            }))
            .await
            .expect("retrieve entities")
            .into_inner();

        assert_eq!(response.entities.len(), 1);
        assert!(!response.next_cursor.is_empty() || response.entities.len() < 10);
    }
}
