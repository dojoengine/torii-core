use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde_json::Value;
use sqlx::{
    any::AnyPoolOptions, sqlite::SqliteConnectOptions, Any, ConnectOptions, Pool, QueryBuilder, Row,
};
use starknet::core::types::{Felt, U256};
use tonic::{Request, Response, Status};
use torii_common::{blob_to_u256, u256_to_blob};
use torii_erc721::{storage::OwnershipCursor, Erc721Storage};

use crate::proto::arcade::{
    arcade_server::Arcade, Collection, Edition, Game, GetPlayerInventoryRequest,
    GetPlayerInventoryResponse, InventoryItem, ListCollectionsRequest, ListCollectionsResponse,
    ListEditionsRequest, ListEditionsResponse, ListGamesRequest, ListGamesResponse,
    ListListingsRequest, ListListingsResponse, ListSalesRequest, ListSalesResponse,
    MarketplaceListing, MarketplaceSale,
};

const GAME_TABLE: &str = "ARCADE-Game";
const EDITION_TABLE: &str = "ARCADE-Edition";
const COLLECTION_TABLE: &str = "ARCADE-Collection";
const ORDER_TABLE: &str = "ARCADE-Order";
const LISTING_TABLE: &str = "ARCADE-Listing";
const SALE_TABLE: &str = "ARCADE-Sale";

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
pub struct ArcadeService {
    state: Arc<ArcadeState>,
}

struct ArcadeState {
    pool: Pool<Any>,
    backend: DbBackend,
    erc721: Arc<Erc721Storage>,
}

#[derive(Clone)]
struct CollectionRecord {
    collection_id: String,
    uuid: Option<String>,
    contract_address: String,
}

struct ListingRecord {
    order_id: u64,
    entity_id: String,
    collection_id: String,
    token_id: String,
    currency: String,
    owner: String,
    price: String,
    quantity: String,
    expiration: u64,
    status: u32,
    category: u32,
    royalties: bool,
    time: u64,
}

struct SaleRecord {
    order_id: u64,
    entity_id: String,
    collection_id: String,
    token_id: String,
    currency: String,
    seller: String,
    buyer: String,
    price: String,
    quantity: String,
    expiration: u64,
    status: u32,
    category: u32,
    royalties: bool,
    time: u64,
}

impl ArcadeService {
    pub async fn new(
        database_url: &str,
        erc721_database_url: &str,
        max_connections: Option<u32>,
    ) -> Result<Self> {
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
            state: Arc::new(ArcadeState {
                pool,
                backend,
                erc721: Arc::new(Erc721Storage::new(erc721_database_url).await?),
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

        for statement in projection_schema_sql() {
            sqlx::query(statement).execute(&self.state.pool).await?;
        }

        self.bootstrap_from_source().await?;
        Ok(())
    }

    pub async fn bootstrap_from_source(&self) -> Result<()> {
        self.bootstrap_games().await?;
        self.bootstrap_editions().await?;
        self.bootstrap_collections().await?;
        self.bootstrap_listings().await?;
        self.bootstrap_sales().await?;
        Ok(())
    }

    pub async fn refresh_game(&self, entity_id: Felt) -> Result<()> {
        if !self.source_table_exists(GAME_TABLE).await? {
            return Ok(());
        }
        let sql = select_by_entity_sql(self.state.backend, GAME_TABLE);
        if let Some(row) = sqlx::query(&sql)
            .bind(felt_hex(entity_id))
            .fetch_optional(&self.state.pool)
            .await?
        {
            self.upsert_game_row(&row).await?;
        }
        Ok(())
    }

    pub async fn refresh_edition(&self, entity_id: Felt) -> Result<()> {
        if !self.source_table_exists(EDITION_TABLE).await? {
            return Ok(());
        }
        let sql = select_by_entity_sql(self.state.backend, EDITION_TABLE);
        if let Some(row) = sqlx::query(&sql)
            .bind(felt_hex(entity_id))
            .fetch_optional(&self.state.pool)
            .await?
        {
            self.upsert_edition_row(&row).await?;
        }
        Ok(())
    }

    pub async fn refresh_collection(&self, entity_id: Felt) -> Result<()> {
        if !self.source_table_exists(COLLECTION_TABLE).await? {
            return Ok(());
        }
        let sql = select_by_entity_sql(self.state.backend, COLLECTION_TABLE);
        if let Some(row) = sqlx::query(&sql)
            .bind(felt_hex(entity_id))
            .fetch_optional(&self.state.pool)
            .await?
        {
            self.upsert_collection_row(&row).await?;
        }
        Ok(())
    }

    pub async fn refresh_listing(&self, entity_id: Felt) -> Result<()> {
        let Some(listing_table) = self.listing_source_table().await? else {
            return Ok(());
        };
        let sql = select_by_entity_sql(self.state.backend, listing_table);
        if let Some(row) = sqlx::query(&sql)
            .bind(felt_hex(entity_id))
            .fetch_optional(&self.state.pool)
            .await?
        {
            self.upsert_listing_row(&row).await?;
        }
        Ok(())
    }

    pub async fn refresh_sale(&self, entity_id: Felt) -> Result<()> {
        if !self.source_table_exists(SALE_TABLE).await? {
            return Ok(());
        }
        let sql = select_by_entity_sql(self.state.backend, SALE_TABLE);
        if let Some(row) = sqlx::query(&sql)
            .bind(felt_hex(entity_id))
            .fetch_optional(&self.state.pool)
            .await?
        {
            self.upsert_sale_row(&row).await?;
        }
        Ok(())
    }

    pub async fn delete_game(&self, entity_id: Felt) -> Result<()> {
        self.delete_by_entity("torii_arcade_games", entity_id).await
    }

    pub async fn delete_edition(&self, entity_id: Felt) -> Result<()> {
        self.delete_by_entity("torii_arcade_editions", entity_id)
            .await
    }

    pub async fn delete_collection(&self, entity_id: Felt) -> Result<()> {
        self.delete_by_entity("torii_arcade_collections", entity_id)
            .await
    }

    pub async fn delete_listing(&self, entity_id: Felt) -> Result<()> {
        self.delete_by_entity("torii_arcade_listings", entity_id)
            .await
    }

    pub async fn delete_sale(&self, entity_id: Felt) -> Result<()> {
        self.delete_by_entity("torii_arcade_sales", entity_id).await
    }

    async fn delete_by_entity(&self, table: &str, entity_id: Felt) -> Result<()> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => format!("DELETE FROM {table} WHERE entity_id = ?1"),
            DbBackend::Postgres => format!("DELETE FROM {table} WHERE entity_id = $1"),
        };
        sqlx::query(&sql)
            .bind(felt_hex(entity_id))
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn bootstrap_games(&self) -> Result<()> {
        if !self.source_table_exists(GAME_TABLE).await? {
            return Ok(());
        }
        let rows = sqlx::query(r#"SELECT * FROM "ARCADE-Game""#)
            .fetch_all(&self.state.pool)
            .await?;
        for row in rows {
            self.upsert_game_row(&row).await?;
        }
        Ok(())
    }

    async fn bootstrap_editions(&self) -> Result<()> {
        if !self.source_table_exists(EDITION_TABLE).await? {
            return Ok(());
        }
        let rows = sqlx::query(r#"SELECT * FROM "ARCADE-Edition""#)
            .fetch_all(&self.state.pool)
            .await?;
        for row in rows {
            self.upsert_edition_row(&row).await?;
        }
        Ok(())
    }

    async fn bootstrap_collections(&self) -> Result<()> {
        if !self.source_table_exists(COLLECTION_TABLE).await? {
            return Ok(());
        }
        let rows = sqlx::query(r#"SELECT * FROM "ARCADE-Collection""#)
            .fetch_all(&self.state.pool)
            .await?;
        for row in rows {
            self.upsert_collection_row(&row).await?;
        }
        Ok(())
    }

    async fn bootstrap_listings(&self) -> Result<()> {
        let Some(listing_table) = self.listing_source_table().await? else {
            return Ok(());
        };
        let rows = sqlx::query(&format!(r#"SELECT * FROM "{listing_table}""#))
            .fetch_all(&self.state.pool)
            .await?;
        for row in rows {
            self.upsert_listing_row(&row).await?;
        }
        Ok(())
    }

    async fn bootstrap_sales(&self) -> Result<()> {
        if !self.source_table_exists(SALE_TABLE).await? {
            return Ok(());
        }
        let rows = sqlx::query(r#"SELECT * FROM "ARCADE-Sale""#)
            .fetch_all(&self.state.pool)
            .await?;
        for row in rows {
            self.upsert_sale_row(&row).await?;
        }
        Ok(())
    }

    async fn source_table_exists(&self, table_name: &str) -> Result<bool> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)"
            }
            DbBackend::Postgres => {
                "SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = $1
                )"
            }
        };
        Ok(sqlx::query_scalar(sql)
            .bind(table_name)
            .fetch_one(&self.state.pool)
            .await
            .unwrap_or(false))
    }

    pub async fn load_tracked_table_names_by_id(&self) -> Result<HashMap<String, String>> {
        if !self.source_table_exists("dojo_tables").await? {
            return Ok(HashMap::new());
        }

        let mut builder: QueryBuilder<Any> =
            QueryBuilder::new("SELECT id, name FROM dojo_tables WHERE name IN (");
        let mut separated = builder.separated(", ");
        for table_name in [
            GAME_TABLE,
            EDITION_TABLE,
            COLLECTION_TABLE,
            ORDER_TABLE,
            LISTING_TABLE,
            SALE_TABLE,
        ] {
            separated.push_bind(table_name);
        }
        separated.push_unseparated(")");

        let rows = builder.build().fetch_all(&self.state.pool).await?;
        let mut names_by_id = HashMap::with_capacity(rows.len());
        for row in rows {
            names_by_id.insert(row_felt_hex(&row, "id")?, row_string(&row, "name")?);
        }

        Ok(names_by_id)
    }

    async fn listing_source_table(&self) -> Result<Option<&'static str>> {
        for table_name in [ORDER_TABLE, LISTING_TABLE] {
            if self.source_table_exists(table_name).await? {
                return Ok(Some(table_name));
            }
        }

        Ok(None)
    }

    async fn upsert_game_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_arcade_games (
                    game_id, entity_id, name, description, published, whitelisted, color, image,
                    external_url, animation_url, youtube_url, attributes_json, properties_json, socials_json, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, strftime('%s','now'))
                 ON CONFLICT(game_id) DO UPDATE SET
                    entity_id = excluded.entity_id,
                    name = excluded.name,
                    description = excluded.description,
                    published = excluded.published,
                    whitelisted = excluded.whitelisted,
                    color = excluded.color,
                    image = excluded.image,
                    external_url = excluded.external_url,
                    animation_url = excluded.animation_url,
                    youtube_url = excluded.youtube_url,
                    attributes_json = excluded.attributes_json,
                    properties_json = excluded.properties_json,
                    socials_json = excluded.socials_json,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_arcade_games (
                    game_id, entity_id, name, description, published, whitelisted, color, image,
                    external_url, animation_url, youtube_url, attributes_json, properties_json, socials_json, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(game_id) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    published = EXCLUDED.published,
                    whitelisted = EXCLUDED.whitelisted,
                    color = EXCLUDED.color,
                    image = EXCLUDED.image,
                    external_url = EXCLUDED.external_url,
                    animation_url = EXCLUDED.animation_url,
                    youtube_url = EXCLUDED.youtube_url,
                    attributes_json = EXCLUDED.attributes_json,
                    properties_json = EXCLUDED.properties_json,
                    socials_json = EXCLUDED.socials_json,
                    updated_at = EXCLUDED.updated_at"
            }
        };

        sqlx::query(sql)
            .bind(row_string(row, "id")?)
            .bind(row_string(row, "entity_id")?)
            .bind(row_opt_string(row, "name")?)
            .bind(row_opt_string(row, "description")?)
            .bind(row_bool_i64(row, "published")?)
            .bind(row_bool_i64(row, "whitelisted")?)
            .bind(row_opt_string(row, "color")?)
            .bind(row_opt_string(row, "image")?)
            .bind(row_opt_string(row, "external_url")?)
            .bind(row_opt_string(row, "animation_url")?)
            .bind(row_opt_string(row, "youtube_url")?)
            .bind(row_opt_string(row, "attributes")?)
            .bind(row_opt_string(row, "properties")?)
            .bind(row_opt_string(row, "socials")?)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn upsert_edition_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_arcade_editions (
                    edition_id, entity_id, game_id, world_address, namespace, name, description,
                    published, whitelisted, priority, config_json, color, image, external_url,
                    animation_url, youtube_url, attributes_json, properties_json, socials_json, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, strftime('%s','now'))
                 ON CONFLICT(edition_id) DO UPDATE SET
                    entity_id = excluded.entity_id,
                    game_id = excluded.game_id,
                    world_address = excluded.world_address,
                    namespace = excluded.namespace,
                    name = excluded.name,
                    description = excluded.description,
                    published = excluded.published,
                    whitelisted = excluded.whitelisted,
                    priority = excluded.priority,
                    config_json = excluded.config_json,
                    color = excluded.color,
                    image = excluded.image,
                    external_url = excluded.external_url,
                    animation_url = excluded.animation_url,
                    youtube_url = excluded.youtube_url,
                    attributes_json = excluded.attributes_json,
                    properties_json = excluded.properties_json,
                    socials_json = excluded.socials_json,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_arcade_editions (
                    edition_id, entity_id, game_id, world_address, namespace, name, description,
                    published, whitelisted, priority, config_json, color, image, external_url,
                    animation_url, youtube_url, attributes_json, properties_json, socials_json, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(edition_id) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    game_id = EXCLUDED.game_id,
                    world_address = EXCLUDED.world_address,
                    namespace = EXCLUDED.namespace,
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    published = EXCLUDED.published,
                    whitelisted = EXCLUDED.whitelisted,
                    priority = EXCLUDED.priority,
                    config_json = EXCLUDED.config_json,
                    color = EXCLUDED.color,
                    image = EXCLUDED.image,
                    external_url = EXCLUDED.external_url,
                    animation_url = EXCLUDED.animation_url,
                    youtube_url = EXCLUDED.youtube_url,
                    attributes_json = EXCLUDED.attributes_json,
                    properties_json = EXCLUDED.properties_json,
                    socials_json = EXCLUDED.socials_json,
                    updated_at = EXCLUDED.updated_at"
            }
        };

        sqlx::query(sql)
            .bind(row_string(row, "id")?)
            .bind(row_string(row, "entity_id")?)
            .bind(row_string(row, "game_id")?)
            .bind(row_opt_string(row, "world_address")?)
            .bind(row_opt_string(row, "namespace")?)
            .bind(row_opt_string(row, "name")?)
            .bind(row_opt_string(row, "description")?)
            .bind(row_bool_i64(row, "published")?)
            .bind(row_bool_i64(row, "whitelisted")?)
            .bind(row_u64(row, "priority")? as i64)
            .bind(row_opt_string(row, "config")?)
            .bind(row_opt_string(row, "color")?)
            .bind(row_opt_string(row, "image")?)
            .bind(row_opt_string(row, "external_url")?)
            .bind(row_opt_string(row, "animation_url")?)
            .bind(row_opt_string(row, "youtube_url")?)
            .bind(row_opt_string(row, "attributes")?)
            .bind(row_opt_string(row, "properties")?)
            .bind(row_opt_string(row, "socials")?)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn upsert_collection_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_arcade_collections (
                    collection_id, entity_id, uuid, contract_address, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, strftime('%s','now'))
                 ON CONFLICT(collection_id) DO UPDATE SET
                    entity_id = excluded.entity_id,
                    uuid = excluded.uuid,
                    contract_address = excluded.contract_address,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_arcade_collections (
                    collection_id, entity_id, uuid, contract_address, updated_at
                 ) VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(collection_id) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    uuid = EXCLUDED.uuid,
                    contract_address = EXCLUDED.contract_address,
                    updated_at = EXCLUDED.updated_at"
            }
        };

        sqlx::query(sql)
            .bind(row_string(row, "id")?)
            .bind(row_string(row, "entity_id")?)
            .bind(row_opt_string(row, "uuid")?)
            .bind(row_string(row, "contract_address")?)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn upsert_listing_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        let parsed = parse_listing_row(row)?;
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_arcade_listings (
                    order_id, entity_id, collection_id, token_id, currency, owner, price, quantity,
                    expiration, status, category, royalties, time, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, strftime('%s','now'))
                 ON CONFLICT(order_id) DO UPDATE SET
                    entity_id = excluded.entity_id,
                    collection_id = excluded.collection_id,
                    token_id = excluded.token_id,
                    currency = excluded.currency,
                    owner = excluded.owner,
                    price = excluded.price,
                    quantity = excluded.quantity,
                    expiration = excluded.expiration,
                    status = excluded.status,
                    category = excluded.category,
                    royalties = excluded.royalties,
                    time = excluded.time,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_arcade_listings (
                    order_id, entity_id, collection_id, token_id, currency, owner, price, quantity,
                    expiration, status, category, royalties, time, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(order_id) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    collection_id = EXCLUDED.collection_id,
                    token_id = EXCLUDED.token_id,
                    currency = EXCLUDED.currency,
                    owner = EXCLUDED.owner,
                    price = EXCLUDED.price,
                    quantity = EXCLUDED.quantity,
                    expiration = EXCLUDED.expiration,
                    status = EXCLUDED.status,
                    category = EXCLUDED.category,
                    royalties = EXCLUDED.royalties,
                    time = EXCLUDED.time,
                    updated_at = EXCLUDED.updated_at"
            }
        };

        sqlx::query(sql)
            .bind(parsed.order_id as i64)
            .bind(parsed.entity_id)
            .bind(parsed.collection_id)
            .bind(parsed.token_id)
            .bind(parsed.currency)
            .bind(parsed.owner)
            .bind(parsed.price)
            .bind(parsed.quantity)
            .bind(parsed.expiration as i64)
            .bind(parsed.status as i64)
            .bind(parsed.category as i64)
            .bind(if parsed.royalties { 1_i64 } else { 0_i64 })
            .bind(parsed.time as i64)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn upsert_sale_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        let parsed = parse_sale_row(row)?;
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_arcade_sales (
                    order_id, entity_id, collection_id, token_id, currency, seller, buyer, price, quantity,
                    expiration, status, category, royalties, time, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, strftime('%s','now'))
                 ON CONFLICT(order_id) DO UPDATE SET
                    entity_id = excluded.entity_id,
                    collection_id = excluded.collection_id,
                    token_id = excluded.token_id,
                    currency = excluded.currency,
                    seller = excluded.seller,
                    buyer = excluded.buyer,
                    price = excluded.price,
                    quantity = excluded.quantity,
                    expiration = excluded.expiration,
                    status = excluded.status,
                    category = excluded.category,
                    royalties = excluded.royalties,
                    time = excluded.time,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_arcade_sales (
                    order_id, entity_id, collection_id, token_id, currency, seller, buyer, price, quantity,
                    expiration, status, category, royalties, time, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(order_id) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    collection_id = EXCLUDED.collection_id,
                    token_id = EXCLUDED.token_id,
                    currency = EXCLUDED.currency,
                    seller = EXCLUDED.seller,
                    buyer = EXCLUDED.buyer,
                    price = EXCLUDED.price,
                    quantity = EXCLUDED.quantity,
                    expiration = EXCLUDED.expiration,
                    status = EXCLUDED.status,
                    category = EXCLUDED.category,
                    royalties = EXCLUDED.royalties,
                    time = EXCLUDED.time,
                    updated_at = EXCLUDED.updated_at"
            }
        };

        sqlx::query(sql)
            .bind(parsed.order_id as i64)
            .bind(parsed.entity_id)
            .bind(parsed.collection_id)
            .bind(parsed.token_id)
            .bind(parsed.currency)
            .bind(parsed.seller)
            .bind(parsed.buyer)
            .bind(parsed.price)
            .bind(parsed.quantity)
            .bind(parsed.expiration as i64)
            .bind(parsed.status as i64)
            .bind(parsed.category as i64)
            .bind(if parsed.royalties { 1_i64 } else { 0_i64 })
            .bind(parsed.time as i64)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn load_collection_records(&self) -> Result<Vec<CollectionRecord>> {
        let rows = sqlx::query(
            "SELECT collection_id, uuid, contract_address FROM torii_arcade_collections ORDER BY collection_id ASC",
        )
        .fetch_all(&self.state.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(CollectionRecord {
                    collection_id: row.try_get("collection_id")?,
                    uuid: row.try_get("uuid").ok(),
                    contract_address: row.try_get("contract_address")?,
                })
            })
            .collect()
    }

    async fn load_collections_by_contract(&self) -> Result<HashMap<String, CollectionRecord>> {
        Ok(self
            .load_collection_records()
            .await?
            .into_iter()
            .map(|record| (record.contract_address.clone(), record))
            .collect())
    }

    async fn load_collections_by_id(&self) -> Result<HashMap<String, CollectionRecord>> {
        Ok(self
            .load_collection_records()
            .await?
            .into_iter()
            .map(|record| (record.collection_id.clone(), record))
            .collect())
    }
}

#[tonic::async_trait]
impl Arcade for ArcadeService {
    async fn list_games(
        &self,
        request: Request<ListGamesRequest>,
    ) -> Result<Response<ListGamesResponse>, Status> {
        let req = request.into_inner();
        let mut builder: QueryBuilder<Any> = QueryBuilder::new(
            "SELECT game_id, entity_id, name, description, published, whitelisted, color, image, external_url, animation_url, youtube_url, attributes_json, properties_json, socials_json FROM torii_arcade_games WHERE 1=1",
        );
        if !req.include_unpublished {
            builder.push(" AND published = ").push_bind(1_i64);
        }
        if !req.include_unwhitelisted {
            builder.push(" AND whitelisted = ").push_bind(1_i64);
        }
        builder.push(" ORDER BY name ASC");

        let rows = builder
            .build()
            .fetch_all(&self.state.pool)
            .await
            .map_err(internal_status)?;
        let games = rows
            .into_iter()
            .map(|row| Ok(game_from_row(&row)?))
            .collect::<Result<Vec<_>>>()
            .map_err(internal_status)?;

        Ok(Response::new(ListGamesResponse { games }))
    }

    async fn list_editions(
        &self,
        request: Request<ListEditionsRequest>,
    ) -> Result<Response<ListEditionsResponse>, Status> {
        let req = request.into_inner();
        let mut builder: QueryBuilder<Any> = QueryBuilder::new(
            "SELECT edition_id, entity_id, game_id, world_address, namespace, name, description, published, whitelisted, priority, config_json, color, image, external_url, animation_url, youtube_url, attributes_json, properties_json, socials_json FROM torii_arcade_editions WHERE 1=1",
        );
        if !req.game_id.is_empty() {
            let game_id = felt_hex(felt_from_bytes(&req.game_id).map_err(internal_status)?);
            builder.push(" AND game_id = ").push_bind(game_id);
        }
        if !req.include_unpublished {
            builder.push(" AND published = ").push_bind(1_i64);
        }
        if !req.include_unwhitelisted {
            builder.push(" AND whitelisted = ").push_bind(1_i64);
        }
        builder.push(" ORDER BY priority DESC, edition_id ASC");
        builder
            .push(" LIMIT ")
            .push_bind(i64::from(limit_or_default(req.limit, 100)));

        let rows = builder
            .build()
            .fetch_all(&self.state.pool)
            .await
            .map_err(internal_status)?;
        let editions = rows
            .into_iter()
            .map(|row| Ok(edition_from_row(&row)?))
            .collect::<Result<Vec<_>>>()
            .map_err(internal_status)?;

        Ok(Response::new(ListEditionsResponse { editions }))
    }

    async fn list_collections(
        &self,
        request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let req = request.into_inner();
        let limit = limit_or_default(req.limit, 500);
        let rows = QueryBuilder::<Any>::new(
            "SELECT collection_id, uuid, contract_address FROM torii_arcade_collections ORDER BY collection_id ASC LIMIT ",
        )
        .push_bind(i64::from(limit))
        .build()
        .fetch_all(&self.state.pool)
        .await
        .map_err(internal_status)?;

        let mut collections = Vec::with_capacity(rows.len());
        for row in rows {
            let contract_address = row_string(&row, "contract_address").map_err(internal_status)?;
            let token_felt = felt_from_hex(&contract_address).map_err(internal_status)?;
            let metadata = self
                .state
                .erc721
                .get_token_metadata(token_felt)
                .await
                .map_err(internal_status)?;
            let (name, symbol, _) = metadata.unwrap_or((None, None, None));
            collections.push(Collection {
                collection_id: felt_from_hex(
                    &row_string(&row, "collection_id").map_err(internal_status)?,
                )
                .map_err(internal_status)?
                .to_bytes_be()
                .to_vec(),
                uuid: row
                    .try_get::<String, _>("uuid")
                    .ok()
                    .and_then(|value| felt_from_hex(&value).ok())
                    .map(|felt| felt.to_bytes_be().to_vec())
                    .unwrap_or_default(),
                contract_address: token_felt.to_bytes_be().to_vec(),
                name: name.unwrap_or_default(),
                symbol: symbol.unwrap_or_default(),
            });
        }

        Ok(Response::new(ListCollectionsResponse { collections }))
    }

    async fn list_listings(
        &self,
        request: Request<ListListingsRequest>,
    ) -> Result<Response<ListListingsResponse>, Status> {
        let req = request.into_inner();
        let collections_by_id = self
            .load_collections_by_id()
            .await
            .map_err(internal_status)?;
        let collections_by_contract = self
            .load_collections_by_contract()
            .await
            .map_err(internal_status)?;
        let collection_id_filter = if req.collection_id.is_empty() {
            None
        } else {
            Some(felt_hex(
                felt_from_bytes(&req.collection_id).map_err(internal_status)?,
            ))
        };
        let collection_contract_filter = if req.collection_contract.is_empty() {
            None
        } else {
            let felt = felt_from_bytes(&req.collection_contract).map_err(internal_status)?;
            let hex = felt_hex(felt);
            collections_by_contract
                .get(&hex)
                .map(|record| record.collection_id.clone())
        };

        let mut builder: QueryBuilder<Any> = QueryBuilder::new(
            "SELECT order_id, entity_id, collection_id, token_id, currency, owner, price, quantity, expiration, status, category, royalties, time FROM torii_arcade_listings WHERE 1=1",
        );
        if let Some(collection_id) = collection_id_filter.or(collection_contract_filter) {
            builder
                .push(" AND collection_id = ")
                .push_bind(collection_id);
        }
        if !req.owner.is_empty() {
            builder.push(" AND owner = ").push_bind(felt_hex(
                felt_from_bytes(&req.owner).map_err(internal_status)?,
            ));
        }
        if !req.currency.is_empty() {
            builder.push(" AND currency = ").push_bind(felt_hex(
                felt_from_bytes(&req.currency).map_err(internal_status)?,
            ));
        }
        if !req.include_inactive {
            builder.push(" AND status = ").push_bind(1_i64);
        }
        if req.cursor_order_id > 0 {
            builder
                .push(" AND order_id < ")
                .push_bind(req.cursor_order_id as i64);
        }
        builder.push(" ORDER BY order_id DESC");
        let limit = limit_or_default(req.limit, 100);
        builder.push(" LIMIT ").push_bind((limit + 1) as i64);

        let rows = builder
            .build()
            .fetch_all(&self.state.pool)
            .await
            .map_err(internal_status)?;

        let mut listings = Vec::new();
        let mut next_cursor_order_id = 0_u64;
        for (index, row) in rows.into_iter().enumerate() {
            if index == limit as usize {
                next_cursor_order_id = row.try_get::<i64, _>("order_id").unwrap_or_default() as u64;
                break;
            }
            listings.push(
                listing_from_projection_row(&row, &collections_by_id, &self.state.erc721)
                    .await
                    .map_err(internal_status)?,
            );
        }

        Ok(Response::new(ListListingsResponse {
            listings,
            next_cursor_order_id,
        }))
    }

    async fn list_sales(
        &self,
        request: Request<ListSalesRequest>,
    ) -> Result<Response<ListSalesResponse>, Status> {
        let req = request.into_inner();
        let collections_by_id = self
            .load_collections_by_id()
            .await
            .map_err(internal_status)?;
        let collections_by_contract = self
            .load_collections_by_contract()
            .await
            .map_err(internal_status)?;
        let collection_id_filter = if req.collection_id.is_empty() {
            None
        } else {
            Some(felt_hex(
                felt_from_bytes(&req.collection_id).map_err(internal_status)?,
            ))
        };
        let collection_contract_filter = if req.collection_contract.is_empty() {
            None
        } else {
            let felt = felt_from_bytes(&req.collection_contract).map_err(internal_status)?;
            let hex = felt_hex(felt);
            collections_by_contract
                .get(&hex)
                .map(|record| record.collection_id.clone())
        };

        let mut builder: QueryBuilder<Any> = QueryBuilder::new(
            "SELECT order_id, entity_id, collection_id, token_id, currency, seller, buyer, price, quantity, expiration, status, category, royalties, time FROM torii_arcade_sales WHERE 1=1",
        );
        if let Some(collection_id) = collection_id_filter.or(collection_contract_filter) {
            builder
                .push(" AND collection_id = ")
                .push_bind(collection_id);
        }
        if !req.account.is_empty() {
            let account = felt_hex(felt_from_bytes(&req.account).map_err(internal_status)?);
            builder
                .push(" AND (seller = ")
                .push_bind(account.clone())
                .push(" OR buyer = ")
                .push_bind(account)
                .push(")");
        }
        if req.cursor_order_id > 0 {
            builder
                .push(" AND order_id < ")
                .push_bind(req.cursor_order_id as i64);
        }
        builder.push(" ORDER BY order_id DESC");
        let limit = limit_or_default(req.limit, 100);
        builder.push(" LIMIT ").push_bind((limit + 1) as i64);

        let rows = builder
            .build()
            .fetch_all(&self.state.pool)
            .await
            .map_err(internal_status)?;

        let mut sales = Vec::new();
        let mut next_cursor_order_id = 0_u64;
        for (index, row) in rows.into_iter().enumerate() {
            if index == limit as usize {
                next_cursor_order_id = row.try_get::<i64, _>("order_id").unwrap_or_default() as u64;
                break;
            }
            sales.push(
                sale_from_projection_row(&row, &collections_by_id, &self.state.erc721)
                    .await
                    .map_err(internal_status)?,
            );
        }

        Ok(Response::new(ListSalesResponse {
            sales,
            next_cursor_order_id,
        }))
    }

    async fn get_player_inventory(
        &self,
        request: Request<GetPlayerInventoryRequest>,
    ) -> Result<Response<GetPlayerInventoryResponse>, Status> {
        let req = request.into_inner();
        if req.owner.is_empty() {
            return Err(Status::invalid_argument("owner is required"));
        }
        let owner = felt_from_bytes(&req.owner).map_err(internal_status)?;
        let collections_by_contract = self
            .load_collections_by_contract()
            .await
            .map_err(internal_status)?;
        let token_filter = if req.collection_contract.is_empty() {
            collections_by_contract
                .keys()
                .map(|value| felt_from_hex(value))
                .collect::<Result<Vec<_>>>()
                .map_err(internal_status)?
        } else {
            vec![felt_from_bytes(&req.collection_contract).map_err(internal_status)?]
        };
        let cursor = if req.cursor_block_number > 0 || req.cursor_id > 0 {
            Some(OwnershipCursor {
                block_number: req.cursor_block_number,
                id: req.cursor_id,
            })
        } else {
            None
        };
        let limit = limit_or_default(req.limit, 100);
        let (ownership, next_cursor) = self
            .state
            .erc721
            .get_ownership_by_owner(owner, &token_filter, cursor, limit)
            .await
            .map_err(internal_status)?;

        let mut items = Vec::with_capacity(ownership.len());
        for item in ownership {
            let collection = collections_by_contract
                .get(&felt_hex(item.token))
                .cloned()
                .unwrap_or(CollectionRecord {
                    collection_id: felt_hex(item.token),
                    uuid: None,
                    contract_address: felt_hex(item.token),
                });
            let metadata = self
                .state
                .erc721
                .get_token_metadata(item.token)
                .await
                .map_err(internal_status)?;
            let (name, symbol, _) = metadata.unwrap_or((None, None, None));
            items.push(InventoryItem {
                collection: Some(Collection {
                    collection_id: felt_from_hex(&collection.collection_id)
                        .map_err(internal_status)?
                        .to_bytes_be()
                        .to_vec(),
                    uuid: collection
                        .uuid
                        .as_deref()
                        .and_then(|value| felt_from_hex(value).ok())
                        .map(|felt| felt.to_bytes_be().to_vec())
                        .unwrap_or_default(),
                    contract_address: felt_from_hex(&collection.contract_address)
                        .map_err(internal_status)?
                        .to_bytes_be()
                        .to_vec(),
                    name: name.unwrap_or_default(),
                    symbol: symbol.unwrap_or_default(),
                }),
                token_id: u256_to_bytes(item.token_id),
                owner: item.owner.to_bytes_be().to_vec(),
                last_block: item.block_number,
            });
        }

        Ok(Response::new(GetPlayerInventoryResponse {
            items,
            next_cursor_block_number: next_cursor.map_or(0, |cursor| cursor.block_number),
            next_cursor_id: next_cursor.map_or(0, |cursor| cursor.id),
        }))
    }
}

fn projection_schema_sql() -> &'static [&'static str] {
    &[
        "CREATE TABLE IF NOT EXISTS torii_arcade_games (
            game_id TEXT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            name TEXT,
            description TEXT,
            published BIGINT NOT NULL DEFAULT 0,
            whitelisted BIGINT NOT NULL DEFAULT 0,
            color TEXT,
            image TEXT,
            external_url TEXT,
            animation_url TEXT,
            youtube_url TEXT,
            attributes_json TEXT,
            properties_json TEXT,
            socials_json TEXT,
            updated_at BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS torii_arcade_games_visibility_idx
            ON torii_arcade_games(published, whitelisted)",
        "CREATE TABLE IF NOT EXISTS torii_arcade_editions (
            edition_id TEXT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            game_id TEXT NOT NULL,
            world_address TEXT,
            namespace TEXT,
            name TEXT,
            description TEXT,
            published BIGINT NOT NULL DEFAULT 0,
            whitelisted BIGINT NOT NULL DEFAULT 0,
            priority BIGINT NOT NULL DEFAULT 0,
            config_json TEXT,
            color TEXT,
            image TEXT,
            external_url TEXT,
            animation_url TEXT,
            youtube_url TEXT,
            attributes_json TEXT,
            properties_json TEXT,
            socials_json TEXT,
            updated_at BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS torii_arcade_editions_game_idx
            ON torii_arcade_editions(game_id, published, whitelisted, priority DESC)",
        "CREATE TABLE IF NOT EXISTS torii_arcade_collections (
            collection_id TEXT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            uuid TEXT,
            contract_address TEXT NOT NULL,
            updated_at BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS torii_arcade_collections_contract_idx
            ON torii_arcade_collections(contract_address)",
        "CREATE TABLE IF NOT EXISTS torii_arcade_listings (
            order_id BIGINT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            owner TEXT NOT NULL,
            price TEXT NOT NULL,
            quantity TEXT NOT NULL,
            expiration BIGINT NOT NULL,
            status BIGINT NOT NULL,
            category BIGINT NOT NULL,
            royalties BIGINT NOT NULL,
            time BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS torii_arcade_listings_market_idx
            ON torii_arcade_listings(collection_id, status, order_id DESC)",
        "CREATE INDEX IF NOT EXISTS torii_arcade_listings_owner_idx
            ON torii_arcade_listings(owner, order_id DESC)",
        "CREATE TABLE IF NOT EXISTS torii_arcade_sales (
            order_id BIGINT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            seller TEXT NOT NULL,
            buyer TEXT NOT NULL,
            price TEXT NOT NULL,
            quantity TEXT NOT NULL,
            expiration BIGINT NOT NULL,
            status BIGINT NOT NULL,
            category BIGINT NOT NULL,
            royalties BIGINT NOT NULL,
            time BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS torii_arcade_sales_market_idx
            ON torii_arcade_sales(collection_id, order_id DESC)",
        "CREATE INDEX IF NOT EXISTS torii_arcade_sales_account_idx
            ON torii_arcade_sales(seller, buyer, order_id DESC)",
    ]
}

async fn listing_from_projection_row(
    row: &sqlx::any::AnyRow,
    collections_by_id: &HashMap<String, CollectionRecord>,
    erc721: &Arc<Erc721Storage>,
) -> Result<MarketplaceListing> {
    let collection_id = row_string(row, "collection_id")?;
    let collection = collection_proto(
        collections_by_id.get(&collection_id).cloned(),
        erc721,
        &collection_id,
    )
    .await?;
    Ok(MarketplaceListing {
        order_id: row.try_get::<i64, _>("order_id")? as u64,
        entity_id: felt_from_hex(&row_string(row, "entity_id")?)?
            .to_bytes_be()
            .to_vec(),
        collection: Some(collection),
        token_id: decimal_to_bytes(&row_string(row, "token_id")?)?,
        currency: felt_from_hex(&row_string(row, "currency")?)?
            .to_bytes_be()
            .to_vec(),
        owner: felt_from_hex(&row_string(row, "owner")?)?
            .to_bytes_be()
            .to_vec(),
        price: row_string(row, "price")?,
        quantity: row_string(row, "quantity")?,
        expiration: row.try_get::<i64, _>("expiration")? as u64,
        status: row.try_get::<i64, _>("status")? as u32,
        category: row.try_get::<i64, _>("category")? as u32,
        royalties: row.try_get::<i64, _>("royalties")? != 0,
        time: row.try_get::<i64, _>("time")? as u64,
    })
}

async fn sale_from_projection_row(
    row: &sqlx::any::AnyRow,
    collections_by_id: &HashMap<String, CollectionRecord>,
    erc721: &Arc<Erc721Storage>,
) -> Result<MarketplaceSale> {
    let collection_id = row_string(row, "collection_id")?;
    let collection = collection_proto(
        collections_by_id.get(&collection_id).cloned(),
        erc721,
        &collection_id,
    )
    .await?;
    Ok(MarketplaceSale {
        order_id: row.try_get::<i64, _>("order_id")? as u64,
        entity_id: felt_from_hex(&row_string(row, "entity_id")?)?
            .to_bytes_be()
            .to_vec(),
        collection: Some(collection),
        token_id: decimal_to_bytes(&row_string(row, "token_id")?)?,
        currency: felt_from_hex(&row_string(row, "currency")?)?
            .to_bytes_be()
            .to_vec(),
        seller: felt_from_hex(&row_string(row, "seller")?)?
            .to_bytes_be()
            .to_vec(),
        buyer: felt_from_hex(&row_string(row, "buyer")?)?
            .to_bytes_be()
            .to_vec(),
        price: row_string(row, "price")?,
        quantity: row_string(row, "quantity")?,
        expiration: row.try_get::<i64, _>("expiration")? as u64,
        status: row.try_get::<i64, _>("status")? as u32,
        category: row.try_get::<i64, _>("category")? as u32,
        royalties: row.try_get::<i64, _>("royalties")? != 0,
        time: row.try_get::<i64, _>("time")? as u64,
    })
}

async fn collection_proto(
    record: Option<CollectionRecord>,
    erc721: &Arc<Erc721Storage>,
    fallback_collection_id: &str,
) -> Result<Collection> {
    let record = record.unwrap_or(CollectionRecord {
        collection_id: fallback_collection_id.to_string(),
        uuid: None,
        contract_address: fallback_collection_id.to_string(),
    });
    let token = felt_from_hex(&record.contract_address)?;
    let metadata = erc721.get_token_metadata(token).await?;
    let (name, symbol, _) = metadata.unwrap_or((None, None, None));
    Ok(Collection {
        collection_id: felt_from_hex(&record.collection_id)?.to_bytes_be().to_vec(),
        uuid: record
            .uuid
            .as_deref()
            .and_then(|value| felt_from_hex(value).ok())
            .map(|felt| felt.to_bytes_be().to_vec())
            .unwrap_or_default(),
        contract_address: token.to_bytes_be().to_vec(),
        name: name.unwrap_or_default(),
        symbol: symbol.unwrap_or_default(),
    })
}

fn game_from_row(row: &sqlx::any::AnyRow) -> Result<Game> {
    Ok(Game {
        game_id: felt_from_hex(&row_string(row, "game_id")?)?
            .to_bytes_be()
            .to_vec(),
        entity_id: felt_from_hex(&row_string(row, "entity_id")?)?
            .to_bytes_be()
            .to_vec(),
        name: row
            .try_get::<Option<String>, _>("name")?
            .unwrap_or_default(),
        description: row
            .try_get::<Option<String>, _>("description")?
            .unwrap_or_default(),
        published: row.try_get::<i64, _>("published")? != 0,
        whitelisted: row.try_get::<i64, _>("whitelisted")? != 0,
        color: row
            .try_get::<Option<String>, _>("color")?
            .unwrap_or_default(),
        image: row
            .try_get::<Option<String>, _>("image")?
            .unwrap_or_default(),
        external_url: row
            .try_get::<Option<String>, _>("external_url")?
            .unwrap_or_default(),
        animation_url: row
            .try_get::<Option<String>, _>("animation_url")?
            .unwrap_or_default(),
        youtube_url: row
            .try_get::<Option<String>, _>("youtube_url")?
            .unwrap_or_default(),
        attributes_json: row
            .try_get::<Option<String>, _>("attributes_json")?
            .unwrap_or_default(),
        properties_json: row
            .try_get::<Option<String>, _>("properties_json")?
            .unwrap_or_default(),
        socials_json: row
            .try_get::<Option<String>, _>("socials_json")?
            .unwrap_or_default(),
    })
}

fn edition_from_row(row: &sqlx::any::AnyRow) -> Result<Edition> {
    Ok(Edition {
        edition_id: felt_from_hex(&row_string(row, "edition_id")?)?
            .to_bytes_be()
            .to_vec(),
        entity_id: felt_from_hex(&row_string(row, "entity_id")?)?
            .to_bytes_be()
            .to_vec(),
        game_id: felt_from_hex(&row_string(row, "game_id")?)?
            .to_bytes_be()
            .to_vec(),
        world_address: row
            .try_get::<Option<String>, _>("world_address")?
            .as_deref()
            .and_then(|value| felt_from_hex(value).ok())
            .map(|felt| felt.to_bytes_be().to_vec())
            .unwrap_or_default(),
        namespace: row
            .try_get::<Option<String>, _>("namespace")?
            .as_deref()
            .and_then(|value| felt_from_hex(value).ok())
            .map(|felt| felt.to_bytes_be().to_vec())
            .unwrap_or_default(),
        name: row
            .try_get::<Option<String>, _>("name")?
            .unwrap_or_default(),
        description: row
            .try_get::<Option<String>, _>("description")?
            .unwrap_or_default(),
        published: row.try_get::<i64, _>("published")? != 0,
        whitelisted: row.try_get::<i64, _>("whitelisted")? != 0,
        priority: row.try_get::<i64, _>("priority")? as u32,
        config_json: row
            .try_get::<Option<String>, _>("config_json")?
            .unwrap_or_default(),
        color: row
            .try_get::<Option<String>, _>("color")?
            .unwrap_or_default(),
        image: row
            .try_get::<Option<String>, _>("image")?
            .unwrap_or_default(),
        external_url: row
            .try_get::<Option<String>, _>("external_url")?
            .unwrap_or_default(),
        animation_url: row
            .try_get::<Option<String>, _>("animation_url")?
            .unwrap_or_default(),
        youtube_url: row
            .try_get::<Option<String>, _>("youtube_url")?
            .unwrap_or_default(),
        attributes_json: row
            .try_get::<Option<String>, _>("attributes_json")?
            .unwrap_or_default(),
        properties_json: row
            .try_get::<Option<String>, _>("properties_json")?
            .unwrap_or_default(),
        socials_json: row
            .try_get::<Option<String>, _>("socials_json")?
            .unwrap_or_default(),
    })
}

fn parse_listing_row(row: &sqlx::any::AnyRow) -> Result<ListingRecord> {
    let entity_id = row_string(row, "entity_id")?;
    let order_value = serde_json::from_str::<Value>(&row_string(row, "order")?)?;
    Ok(ListingRecord {
        order_id: row_u64(row, "order_id")?,
        entity_id,
        collection_id: json_string(&order_value, "collection")?,
        token_id: json_string(&order_value, "token_id")?,
        currency: json_string(&order_value, "currency")?,
        owner: json_string(&order_value, "owner")?,
        price: json_number_string(&order_value, "price")?,
        quantity: json_number_string(&order_value, "quantity")?,
        expiration: json_u64(&order_value, "expiration")?,
        status: json_u64(&order_value, "status")? as u32,
        category: json_u64(&order_value, "category")? as u32,
        royalties: json_bool(&order_value, "royalties")?,
        time: row_u64(row, "time")?,
    })
}

fn parse_sale_row(row: &sqlx::any::AnyRow) -> Result<SaleRecord> {
    let entity_id = row_string(row, "entity_id")?;
    let order_value = serde_json::from_str::<Value>(&row_string(row, "order")?)?;
    Ok(SaleRecord {
        order_id: row_u64(row, "order_id")?,
        entity_id,
        collection_id: json_string(&order_value, "collection")?,
        token_id: json_string(&order_value, "token_id")?,
        currency: json_string(&order_value, "currency")?,
        seller: row_string(row, "from")?,
        buyer: row_string(row, "to")?,
        price: json_number_string(&order_value, "price")?,
        quantity: json_number_string(&order_value, "quantity")?,
        expiration: json_u64(&order_value, "expiration")?,
        status: json_u64(&order_value, "status")? as u32,
        category: json_u64(&order_value, "category")? as u32,
        royalties: json_bool(&order_value, "royalties")?,
        time: row_u64(row, "time")?,
    })
}

fn select_by_entity_sql(backend: DbBackend, table: &str) -> String {
    match backend {
        DbBackend::Sqlite => format!(r#"SELECT * FROM "{table}" WHERE entity_id = ?1"#),
        DbBackend::Postgres => format!(r#"SELECT * FROM "{table}" WHERE entity_id = $1"#),
    }
}

fn limit_or_default(limit: u32, default: u32) -> u32 {
    if limit == 0 {
        default
    } else {
        limit.min(500)
    }
}

fn row_string(row: &sqlx::any::AnyRow, column: &str) -> Result<String> {
    row.try_get::<String, _>(column)
        .map_err(|err| anyhow!("failed to read {column}: {err}"))
}

fn row_felt_hex(row: &sqlx::any::AnyRow, column: &str) -> Result<String> {
    if let Ok(bytes) = row.try_get::<Vec<u8>, _>(column) {
        return Ok(felt_hex(Felt::from_bytes_be_slice(&bytes)));
    }

    let value = row
        .try_get::<String, _>(column)
        .map_err(|err| anyhow!("failed to read {column} as felt bytes or string: {err}"))?;
    let felt = felt_from_hex(&value)
        .or_else(|_| Felt::from_hex(&value))
        .map_err(|err| anyhow!("failed to parse {column} as felt: {err}"))?;
    Ok(felt_hex(felt))
}

fn row_opt_string(row: &sqlx::any::AnyRow, column: &str) -> Result<Option<String>> {
    row.try_get::<Option<String>, _>(column)
        .map_err(|err| anyhow!("failed to read {column}: {err}"))
}

fn row_u64(row: &sqlx::any::AnyRow, column: &str) -> Result<u64> {
    if let Ok(value) = row.try_get::<i64, _>(column) {
        return Ok(value as u64);
    }
    if let Ok(value) = row.try_get::<String, _>(column) {
        return value
            .parse::<u64>()
            .map_err(|err| anyhow!("failed to parse {column}: {err}"));
    }
    Err(anyhow!("failed to read {column} as u64"))
}

fn row_bool_i64(row: &sqlx::any::AnyRow, column: &str) -> Result<i64> {
    if let Ok(value) = row.try_get::<bool, _>(column) {
        return Ok(i64::from(value));
    }
    if let Ok(value) = row.try_get::<i64, _>(column) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<String, _>(column) {
        return Ok(match value.as_str() {
            "true" | "1" => 1,
            _ => 0,
        });
    }
    Err(anyhow!("failed to read {column} as bool"))
}

fn json_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("missing string field {key}"))
}

fn json_number_string(value: &Value, key: &str) -> Result<String> {
    if let Some(raw) = value.get(key) {
        if let Some(string) = raw.as_str() {
            return Ok(string.to_string());
        }
        return Ok(raw.to_string());
    }
    Err(anyhow!("missing numeric field {key}"))
}

fn json_u64(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing u64 field {key}"))
}

fn json_bool(value: &Value, key: &str) -> Result<bool> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("missing bool field {key}"))
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

fn decimal_to_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow!("cannot parse empty decimal value"));
    }

    let mut bytes = [0_u8; 32];
    for digit in value.bytes() {
        if !digit.is_ascii_digit() {
            return Err(anyhow!("invalid decimal value {value}"));
        }

        let mut carry = u16::from(digit - b'0');
        for byte in bytes.iter_mut().rev() {
            let acc = u16::from(*byte) * 10 + carry;
            *byte = (acc & 0xff) as u8;
            carry = acc >> 8;
        }

        if carry != 0 {
            return Err(anyhow!("decimal value overflows 256 bits: {value}"));
        }
    }

    Ok(u256_to_blob(blob_to_u256(&bytes)))
}

fn u256_to_bytes(value: U256) -> Vec<u8> {
    u256_to_blob(value)
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

fn internal_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}
