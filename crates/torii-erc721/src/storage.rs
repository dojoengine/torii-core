//! SQLite storage for ERC721 NFT transfers, approvals, and ownership
//!
//! Uses binary (BLOB) storage for efficiency.
//! Tracks current NFT ownership state (who owns each token).

use anyhow::Result;
use rusqlite::{params, Connection};
use starknet::core::types::{Felt, U256};
use std::sync::{Arc, Mutex};
use tokio_postgres::{types::ToSql as PgToSql, Client, NoTls};
use torii_common::{
    blob_to_felt, blob_to_u256, felt_to_blob, u256_to_blob, TokenUriResult, TokenUriStore,
};

/// Storage for ERC721 NFT data
pub struct Erc721Storage {
    backend: StorageBackend,
    conn: Arc<Mutex<Connection>>,
    pg_conn: Option<Arc<tokio::sync::Mutex<Client>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageBackend {
    Sqlite,
    Postgres,
}

/// NFT transfer data for batch insertion
pub struct NftTransferData {
    pub id: Option<i64>,
    pub token: Felt,
    pub token_id: U256,
    pub from: Felt,
    pub to: Felt,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// NFT ownership data
pub struct NftOwnershipData {
    pub id: Option<i64>,
    pub token: Felt,
    pub token_id: U256,
    pub owner: Felt,
    pub block_number: u64,
}

/// NFT approval data
pub struct NftApprovalData {
    pub id: Option<i64>,
    pub token: Felt,
    pub token_id: U256,
    pub owner: Felt,
    pub approved: Felt,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// Operator approval data
pub struct OperatorApprovalData {
    pub id: Option<i64>,
    pub token: Felt,
    pub owner: Felt,
    pub operator: Felt,
    pub approved: bool,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// Cursor for paginated transfer queries
#[derive(Debug, Clone, Copy)]
pub struct TransferCursor {
    pub block_number: u64,
    pub id: i64,
}

/// Cursor for paginated ownership queries
#[derive(Debug, Clone, Copy)]
pub struct OwnershipCursor {
    pub block_number: u64,
    pub id: i64,
}

impl Erc721Storage {
    /// Create or open the database
    pub async fn new(db_path: &str) -> Result<Self> {
        if db_path.starts_with("postgres://") || db_path.starts_with("postgresql://") {
            let (client, connection) = tokio_postgres::connect(db_path, NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!(target: "torii_erc721::storage", error = %e, "PostgreSQL connection task failed");
                }
            });
            client.batch_execute(
                r"
                CREATE SCHEMA IF NOT EXISTS erc721;

                CREATE TABLE IF NOT EXISTS erc721.nft_ownership (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    owner BYTEA NOT NULL,
                    block_number BIGINT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp BIGINT,
                    UNIQUE(token, token_id)
                );
                CREATE INDEX IF NOT EXISTS idx_nft_ownership_owner ON erc721.nft_ownership(owner);
                CREATE INDEX IF NOT EXISTS idx_nft_ownership_token ON erc721.nft_ownership(token);

                CREATE TABLE IF NOT EXISTS erc721.nft_transfers (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    from_addr BYTEA NOT NULL,
                    to_addr BYTEA NOT NULL,
                    block_number BIGINT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp BIGINT,
                    UNIQUE(token, tx_hash, token_id, from_addr, to_addr)
                );
                CREATE INDEX IF NOT EXISTS idx_nft_transfers_token ON erc721.nft_transfers(token);
                CREATE INDEX IF NOT EXISTS idx_nft_transfers_from ON erc721.nft_transfers(from_addr);
                CREATE INDEX IF NOT EXISTS idx_nft_transfers_to ON erc721.nft_transfers(to_addr);
                CREATE INDEX IF NOT EXISTS idx_nft_transfers_block ON erc721.nft_transfers(block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_nft_transfers_token_id ON erc721.nft_transfers(token, token_id);

                CREATE TABLE IF NOT EXISTS erc721.nft_wallet_activity (
                    id BIGSERIAL PRIMARY KEY,
                    wallet_address BYTEA NOT NULL,
                    token BYTEA NOT NULL,
                    transfer_id BIGINT NOT NULL REFERENCES erc721.nft_transfers(id),
                    direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                    block_number BIGINT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_nft_wallet_activity_wallet_block ON erc721.nft_wallet_activity(wallet_address, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_nft_wallet_activity_wallet_token ON erc721.nft_wallet_activity(wallet_address, token, block_number DESC);

                CREATE TABLE IF NOT EXISTS erc721.nft_approvals (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    owner BYTEA NOT NULL,
                    approved BYTEA NOT NULL,
                    block_number BIGINT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp BIGINT
                );

                CREATE TABLE IF NOT EXISTS erc721.nft_operators (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    owner BYTEA NOT NULL,
                    operator BYTEA NOT NULL,
                    approved BIGINT NOT NULL,
                    block_number BIGINT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp BIGINT,
                    UNIQUE(token, owner, operator)
                );

                CREATE TABLE IF NOT EXISTS erc721.token_metadata (
                    token BYTEA PRIMARY KEY,
                    name TEXT,
                    symbol TEXT,
                    total_supply BYTEA
                );

                CREATE TABLE IF NOT EXISTS erc721.token_uris (
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    uri TEXT,
                    metadata_json TEXT,
                    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT),
                    PRIMARY KEY (token, token_id)
                );
                CREATE INDEX IF NOT EXISTS idx_token_uris_token ON erc721.token_uris(token);

                CREATE TABLE IF NOT EXISTS erc721.token_attributes (
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (token, token_id, key)
                );
                CREATE INDEX IF NOT EXISTS idx_token_attributes_token ON erc721.token_attributes(token);
                CREATE INDEX IF NOT EXISTS idx_token_attributes_key ON erc721.token_attributes(key);
                CREATE INDEX IF NOT EXISTS idx_token_attributes_key_value ON erc721.token_attributes(key, value);
                ",
            ).await?;

            tracing::info!(target: "torii_erc721::storage", "PostgreSQL storage initialized");
            return Ok(Self {
                backend: StorageBackend::Postgres,
                conn: Arc::new(Mutex::new(Connection::open_in_memory()?)),
                pg_conn: Some(Arc::new(tokio::sync::Mutex::new(client))),
            });
        }

        let conn = Connection::open(db_path)?;

        // Enable WAL mode + Performance PRAGMAs
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;
             PRAGMA cache_size=-64000;
             PRAGMA temp_store=MEMORY;
             PRAGMA mmap_size=268435456;
             PRAGMA wal_autocheckpoint=10000;
             PRAGMA page_size=4096;
             PRAGMA busy_timeout=5000;",
        )?;

        tracing::info!(target: "torii_erc721::storage", "SQLite configured: WAL mode, 64MB cache, 256MB mmap, NORMAL sync");

        // NFT ownership (current state) - one owner per NFT
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nft_ownership (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER,
                UNIQUE(token, token_id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_ownership_owner ON nft_ownership(owner)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_ownership_token ON nft_ownership(token)",
            [],
        )?;

        // Transfer history
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nft_transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER,
                UNIQUE(token, tx_hash, token_id, from_addr, to_addr)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_token ON nft_transfers(token)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_from ON nft_transfers(from_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_to ON nft_transfers(to_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_block ON nft_transfers(block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_token_id ON nft_transfers(token, token_id)",
            [],
        )?;

        // Wallet activity table for efficient OR queries
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nft_wallet_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address BLOB NOT NULL,
                token BLOB NOT NULL,
                transfer_id INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                block_number INTEGER NOT NULL,
                FOREIGN KEY (transfer_id) REFERENCES nft_transfers(id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_wallet_activity_wallet_block
             ON nft_wallet_activity(wallet_address, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_wallet_activity_wallet_token
             ON nft_wallet_activity(wallet_address, token, block_number DESC)",
            [],
        )?;

        // Approvals (single token)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nft_approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL,
                approved BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER
            )",
            [],
        )?;

        // Operator approvals (all tokens)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nft_operators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                owner BLOB NOT NULL,
                operator BLOB NOT NULL,
                approved INTEGER NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER,
                UNIQUE(token, owner, operator)
            )",
            [],
        )?;

        // Token metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_metadata (
                token BLOB PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                total_supply BLOB
            )",
            [],
        )?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS token_uris (
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                uri TEXT,
                metadata_json TEXT,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (token, token_id)
            );
            CREATE INDEX IF NOT EXISTS idx_token_uris_token ON token_uris(token);

            CREATE TABLE IF NOT EXISTS token_attributes (
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (token, token_id, key),
                FOREIGN KEY (token, token_id) REFERENCES token_uris(token, token_id)
            );
            CREATE INDEX IF NOT EXISTS idx_token_attributes_token ON token_attributes(token);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_key ON token_attributes(key);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_key_value ON token_attributes(key, value);",
        )?;

        tracing::info!(target: "torii_erc721::storage", db_path = %db_path, "ERC721 database initialized");

        Ok(Self {
            backend: StorageBackend::Sqlite,
            conn: Arc::new(Mutex::new(conn)),
            pg_conn: None,
        })
    }

    /// Insert multiple transfers and update ownership in a single transaction
    pub async fn insert_transfers_batch(&self, transfers: &[NftTransferData]) -> Result<usize> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_insert_transfers_batch(transfers).await;
        }
        if transfers.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut transfer_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO nft_transfers (token, token_id, from_addr, to_addr, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;

            let mut ownership_stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO nft_ownership (token, token_id, owner, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, COALESCE(?6, strftime('%s', 'now')))",
            )?;
            let mut wallet_both_stmt = tx.prepare_cached(
                "INSERT INTO nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'both', ?4)",
            )?;
            let mut wallet_sent_stmt = tx.prepare_cached(
                "INSERT INTO nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'sent', ?4)",
            )?;
            let mut wallet_received_stmt = tx.prepare_cached(
                "INSERT INTO nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'received', ?4)",
            )?;

            for transfer in transfers {
                let token_blob = felt_to_blob(transfer.token);
                let token_id_blob = u256_to_blob(transfer.token_id);
                let from_blob = felt_to_blob(transfer.from);
                let to_blob = felt_to_blob(transfer.to);
                let tx_hash_blob = felt_to_blob(transfer.tx_hash);

                // Insert transfer
                let rows = transfer_stmt.execute(params![
                    &token_blob,
                    &token_id_blob,
                    &from_blob,
                    &to_blob,
                    transfer.block_number,
                    &tx_hash_blob,
                    transfer.timestamp,
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let transfer_id = tx.last_insert_rowid();

                    // Update ownership (only if to is not zero address)
                    if transfer.to != Felt::ZERO {
                        ownership_stmt.execute(params![
                            &token_blob,
                            &token_id_blob,
                            &to_blob,
                            transfer.block_number,
                            &tx_hash_blob,
                            transfer.timestamp,
                        ])?;
                    }

                    // Insert wallet activity records
                    if transfer.from != Felt::ZERO
                        && transfer.to != Felt::ZERO
                        && transfer.from == transfer.to
                    {
                        wallet_both_stmt.execute(params![
                            &from_blob,
                            &token_blob,
                            transfer_id,
                            transfer.block_number
                        ])?;
                    } else {
                        if transfer.from != Felt::ZERO {
                            wallet_sent_stmt.execute(params![
                                &from_blob,
                                &token_blob,
                                transfer_id,
                                transfer.block_number
                            ])?;
                        }
                        if transfer.to != Felt::ZERO {
                            wallet_received_stmt.execute(params![
                                &to_blob,
                                &token_blob,
                                transfer_id,
                                transfer.block_number
                            ])?;
                        }
                    }
                }
            }
        }

        tx.commit()?;
        Ok(inserted)
    }

    /// Insert operator approvals in a single transaction
    pub async fn insert_operator_approvals_batch(
        &self,
        approvals: &[OperatorApprovalData],
    ) -> Result<usize> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_insert_operator_approvals_batch(approvals).await;
        }
        if approvals.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO nft_operators (token, owner, operator, approved, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;

            for approval in approvals {
                let token_blob = felt_to_blob(approval.token);
                let owner_blob = felt_to_blob(approval.owner);
                let operator_blob = felt_to_blob(approval.operator);
                let tx_hash_blob = felt_to_blob(approval.tx_hash);

                stmt.execute(params![
                    &token_blob,
                    &owner_blob,
                    &operator_blob,
                    approval.approved as i32,
                    approval.block_number,
                    &tx_hash_blob,
                    approval.timestamp,
                ])?;

                inserted += 1;
            }
        }

        tx.commit()?;
        Ok(inserted)
    }

    /// Get filtered transfers with cursor-based pagination
    pub async fn get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        tokens: &[Felt],
        token_ids: &[U256],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<NftTransferData>, Option<TransferCursor>)> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_get_transfers_filtered(
                    wallet, from, to, tokens, token_ids, block_from, block_to, cursor, limit,
                )
                .await;
        }
        let conn = self.conn.lock().unwrap();

        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.token_id, t.from_addr, t.to_addr, t.block_number, t.tx_hash, t.timestamp
                 FROM nft_wallet_activity wa
                 JOIN nft_transfers t ON wa.transfer_id = t.id
                 WHERE wa.wallet_address = ?",
            );
            params_vec.push(Box::new(felt_to_blob(wallet_addr)));

            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND wa.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        } else {
            query.push_str(
                "SELECT t.id, t.token, t.token_id, t.from_addr, t.to_addr, t.block_number, t.tx_hash, t.timestamp
                 FROM nft_transfers t
                 WHERE 1=1",
            );

            if let Some(from_addr) = from {
                query.push_str(" AND t.from_addr = ?");
                params_vec.push(Box::new(felt_to_blob(from_addr)));
            }

            if let Some(to_addr) = to {
                query.push_str(" AND t.to_addr = ?");
                params_vec.push(Box::new(felt_to_blob(to_addr)));
            }

            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND t.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        }

        if !token_ids.is_empty() {
            let placeholders: Vec<&str> = token_ids.iter().map(|_| "?").collect();
            query.push_str(&format!(" AND t.token_id IN ({})", placeholders.join(",")));
            for tid in token_ids {
                params_vec.push(Box::new(u256_to_blob(*tid)));
            }
        }

        if let Some(block_min) = block_from {
            query.push_str(" AND t.block_number >= ?");
            params_vec.push(Box::new(block_min as i64));
        }

        if let Some(block_max) = block_to {
            query.push_str(" AND t.block_number <= ?");
            params_vec.push(Box::new(block_max as i64));
        }

        if let Some(c) = cursor {
            query.push_str(" AND (t.block_number < ? OR (t.block_number = ? AND t.id < ?))");
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.id));
        }

        query.push_str(" ORDER BY t.block_number DESC, t.id DESC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let token_id_bytes: Vec<u8> = row.get(2)?;
            let from_bytes: Vec<u8> = row.get(3)?;
            let to_bytes: Vec<u8> = row.get(4)?;
            let block_number: i64 = row.get(5)?;
            let tx_hash_bytes: Vec<u8> = row.get(6)?;
            let timestamp: Option<i64> = row.get(7)?;

            Ok(NftTransferData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                token_id: blob_to_u256(&token_id_bytes),
                from: blob_to_felt(&from_bytes),
                to: blob_to_felt(&to_bytes),
                block_number: block_number as u64,
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp,
            })
        })?;

        let transfers: Vec<NftTransferData> = rows.collect::<Result<_, _>>()?;

        let next_cursor = if transfers.len() == limit as usize {
            transfers.last().map(|t| TransferCursor {
                block_number: t.block_number,
                id: t.id.unwrap(),
            })
        } else {
            None
        };

        Ok((transfers, next_cursor))
    }

    /// Get current owner of a specific NFT
    pub async fn get_owner(&self, token: Felt, token_id: U256) -> Result<Option<Felt>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_owner(token, token_id).await;
        }
        let conn = self.conn.lock().unwrap();

        let result: Result<Vec<u8>, _> = conn.query_row(
            "SELECT owner FROM nft_ownership WHERE token = ? AND token_id = ?",
            params![felt_to_blob(token), u256_to_blob(token_id)],
            |row| row.get(0),
        );

        match result {
            Ok(owner_bytes) => Ok(Some(blob_to_felt(&owner_bytes))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get ownership records filtered by owner
    pub async fn get_ownership_by_owner(
        &self,
        owner: Felt,
        tokens: &[Felt],
        cursor: Option<OwnershipCursor>,
        limit: u32,
    ) -> Result<(Vec<NftOwnershipData>, Option<OwnershipCursor>)> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_get_ownership_by_owner(owner, tokens, cursor, limit)
                .await;
        }
        let conn = self.conn.lock().unwrap();

        let mut query = String::from(
            "SELECT id, token, token_id, owner, block_number FROM nft_ownership WHERE owner = ?",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(felt_to_blob(owner))];

        if !tokens.is_empty() {
            let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
            query.push_str(&format!(" AND token IN ({})", placeholders.join(",")));
            for token in tokens {
                params_vec.push(Box::new(felt_to_blob(*token)));
            }
        }

        if let Some(c) = cursor {
            query.push_str(" AND (block_number < ? OR (block_number = ? AND id < ?))");
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.id));
        }

        query.push_str(" ORDER BY block_number DESC, id DESC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let token_id_bytes: Vec<u8> = row.get(2)?;
            let owner_bytes: Vec<u8> = row.get(3)?;
            let block_number: i64 = row.get(4)?;

            Ok(NftOwnershipData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                token_id: blob_to_u256(&token_id_bytes),
                owner: blob_to_felt(&owner_bytes),
                block_number: block_number as u64,
            })
        })?;

        let ownership: Vec<NftOwnershipData> = rows.collect::<Result<_, _>>()?;

        let next_cursor = if ownership.len() == limit as usize {
            ownership.last().map(|o| OwnershipCursor {
                block_number: o.block_number,
                id: o.id.unwrap(),
            })
        } else {
            None
        };

        Ok((ownership, next_cursor))
    }

    /// Get transfer count
    pub async fn get_transfer_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_transfer_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM nft_transfers", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get unique token contract count
    pub async fn get_token_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT token) FROM nft_transfers",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get unique NFT count
    pub async fn get_nft_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_nft_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM nft_ownership", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get latest block number indexed
    pub async fn get_latest_block(&self) -> Result<Option<u64>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_latest_block().await;
        }
        let conn = self.conn.lock().unwrap();
        let block: Option<i64> = conn
            .query_row("SELECT MAX(block_number) FROM nft_transfers", [], |row| {
                row.get(0)
            })
            .ok();
        Ok(block.map(|b| b as u64))
    }

    // ===== Token Metadata Methods =====

    /// Check if metadata exists for a token
    pub async fn has_token_metadata(&self, token: Felt) -> Result<bool> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_has_token_metadata(token).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM token_metadata WHERE token = ?",
            params![&token_blob],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Insert or update token metadata
    pub async fn upsert_token_metadata(
        &self,
        token: Felt,
        name: Option<&str>,
        symbol: Option<&str>,
        total_supply: Option<U256>,
    ) -> Result<()> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_upsert_token_metadata(token, name, symbol, total_supply)
                .await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let supply_blob = total_supply.map(u256_to_blob);
        conn.execute(
            "INSERT INTO token_metadata (token, name, symbol, total_supply)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(token) DO UPDATE SET
                 name = COALESCE(excluded.name, token_metadata.name),
                 symbol = COALESCE(excluded.symbol, token_metadata.symbol),
                 total_supply = COALESCE(excluded.total_supply, token_metadata.total_supply)",
            params![&token_blob, name, symbol, supply_blob],
        )?;
        Ok(())
    }

    /// Get token metadata
    pub async fn get_token_metadata(
        &self,
        token: Felt,
    ) -> Result<Option<(Option<String>, Option<String>, Option<U256>)>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_metadata(token).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let result = conn
            .query_row(
                "SELECT name, symbol, total_supply FROM token_metadata WHERE token = ?",
                params![&token_blob],
                |row| {
                    let name: Option<String> = row.get(0)?;
                    let symbol: Option<String> = row.get(1)?;
                    let supply_bytes: Option<Vec<u8>> = row.get(2)?;
                    Ok((name, symbol, supply_bytes.map(|b| blob_to_u256(&b))))
                },
            )
            .ok();
        Ok(result)
    }

    /// Get all token metadata
    pub async fn get_all_token_metadata(
        &self,
    ) -> Result<Vec<(Felt, Option<String>, Option<String>, Option<U256>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT token, name, symbol, total_supply FROM token_metadata")?;
        let rows = stmt.query_map([], |row| {
            let token_bytes: Vec<u8> = row.get(0)?;
            let name: Option<String> = row.get(1)?;
            let symbol: Option<String> = row.get(2)?;
            let supply_bytes: Option<Vec<u8>> = row.get(3)?;
            Ok((
                blob_to_felt(&token_bytes),
                name,
                symbol,
                supply_bytes.map(|b| blob_to_u256(&b)),
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Get token metadata with cursor-based pagination.
    ///
    /// Returns at most `limit` rows and an optional next cursor token.
    pub async fn get_token_metadata_paginated(
        &self,
        cursor: Option<Felt>,
        limit: u32,
    ) -> Result<(
        Vec<(Felt, Option<String>, Option<String>, Option<U256>)>,
        Option<Felt>,
    )> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_metadata_paginated(cursor, limit).await;
        }
        let conn = self.conn.lock().unwrap();
        let fetch_limit = limit.clamp(1, 1000) as usize + 1;

        let mut out = if let Some(cursor_token) = cursor {
            let cursor_blob = felt_to_blob(cursor_token);
            let mut stmt = conn.prepare(
                "SELECT token, name, symbol, total_supply
                 FROM token_metadata
                 WHERE token > ?1
                 ORDER BY token ASC
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![&cursor_blob, fetch_limit as i64], |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let symbol: Option<String> = row.get(2)?;
                let supply_bytes: Option<Vec<u8>> = row.get(3)?;
                Ok((
                    blob_to_felt(&token_bytes),
                    name,
                    symbol,
                    supply_bytes.map(|b| blob_to_u256(&b)),
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let mut stmt = conn.prepare(
                "SELECT token, name, symbol, total_supply
                 FROM token_metadata
                 ORDER BY token ASC
                 LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![fetch_limit as i64], |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let symbol: Option<String> = row.get(2)?;
                let supply_bytes: Option<Vec<u8>> = row.get(3)?;
                Ok((
                    blob_to_felt(&token_bytes),
                    name,
                    symbol,
                    supply_bytes.map(|b| blob_to_u256(&b)),
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        let capped = limit.clamp(1, 1000) as usize;
        let next_cursor = if out.len() > capped {
            let next = out[capped].0;
            out.truncate(capped);
            Some(next)
        } else {
            None
        };

        Ok((out, next_cursor))
    }

    /// Returns true if a token URI row exists for `(token, token_id)`.
    pub async fn has_token_uri(&self, token: Felt, token_id: U256) -> Result<bool> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_has_token_uri(token, token_id).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let token_id_blob = u256_to_blob(token_id);
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM token_uris WHERE token = ?1 AND token_id = ?2",
            params![&token_blob, &token_id_blob],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Returns all token URI rows for a given contract.
    pub async fn get_token_uris_by_contract(
        &self,
        token: Felt,
    ) -> Result<Vec<(U256, Option<String>, Option<String>)>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_uris_by_contract(token).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let mut stmt = conn.prepare(
            "SELECT token_id, uri, metadata_json
             FROM token_uris
             WHERE token = ?1
             ORDER BY token_id ASC",
        )?;
        let rows = stmt.query_map(params![&token_blob], |row| {
            let token_id_bytes: Vec<u8> = row.get(0)?;
            let uri: Option<String> = row.get(1)?;
            let metadata_json: Option<String> = row.get(2)?;
            Ok((blob_to_u256(&token_id_bytes), uri, metadata_json))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    async fn pg_client(&self) -> Result<tokio::sync::MutexGuard<'_, Client>> {
        let conn = self
            .pg_conn
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL connection not initialized"))?;
        Ok(conn.lock().await)
    }

    fn pg_next_param(
        params: &mut Vec<Box<dyn PgToSql + Sync + Send>>,
        value: impl PgToSql + Sync + Send + 'static,
    ) -> String {
        params.push(Box::new(value));
        format!("${}", params.len())
    }

    async fn pg_insert_transfers_batch(&self, transfers: &[NftTransferData]) -> Result<usize> {
        if transfers.is_empty() {
            return Ok(0);
        }
        let mut client = self.pg_client().await?;
        let tx = client.transaction().await?;
        let mut inserted = 0usize;

        for transfer in transfers {
            let token_blob = felt_to_blob(transfer.token);
            let token_id_blob = u256_to_blob(transfer.token_id);
            let from_blob = felt_to_blob(transfer.from);
            let to_blob = felt_to_blob(transfer.to);
            let tx_hash_blob = felt_to_blob(transfer.tx_hash);
            let ts = transfer
                .timestamp
                .unwrap_or_else(|| chrono::Utc::now().timestamp());

            let row = tx.query_opt(
                "INSERT INTO erc721.nft_transfers (token, token_id, from_addr, to_addr, block_number, tx_hash, timestamp)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (token, tx_hash, token_id, from_addr, to_addr) DO NOTHING
                 RETURNING id",
                &[&token_blob, &token_id_blob, &from_blob, &to_blob, &(transfer.block_number as i64), &tx_hash_blob, &ts],
            ).await?;

            if let Some(row) = row {
                inserted += 1;
                let transfer_id: i64 = row.get(0);
                if transfer.to != Felt::ZERO {
                    tx.execute(
                        "INSERT INTO erc721.nft_ownership (token, token_id, owner, block_number, tx_hash, timestamp)
                         VALUES ($1, $2, $3, $4, $5, $6)
                         ON CONFLICT (token, token_id) DO UPDATE SET owner = EXCLUDED.owner, block_number = EXCLUDED.block_number, tx_hash = EXCLUDED.tx_hash, timestamp = EXCLUDED.timestamp",
                        &[&token_blob, &token_id_blob, &to_blob, &(transfer.block_number as i64), &tx_hash_blob, &ts],
                    ).await?;
                }

                if transfer.from != Felt::ZERO
                    && transfer.to != Felt::ZERO
                    && transfer.from == transfer.to
                {
                    tx.execute(
                        "INSERT INTO erc721.nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                         VALUES ($1, $2, $3, 'both', $4)",
                        &[&from_blob, &token_blob, &transfer_id, &(transfer.block_number as i64)],
                    ).await?;
                } else {
                    if transfer.from != Felt::ZERO {
                        tx.execute(
                            "INSERT INTO erc721.nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                             VALUES ($1, $2, $3, 'sent', $4)",
                            &[&from_blob, &token_blob, &transfer_id, &(transfer.block_number as i64)],
                        ).await?;
                    }
                    if transfer.to != Felt::ZERO {
                        tx.execute(
                            "INSERT INTO erc721.nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                             VALUES ($1, $2, $3, 'received', $4)",
                            &[&to_blob, &token_blob, &transfer_id, &(transfer.block_number as i64)],
                        ).await?;
                    }
                }
            }
        }

        tx.commit().await?;
        Ok(inserted)
    }

    async fn pg_insert_operator_approvals_batch(
        &self,
        approvals: &[OperatorApprovalData],
    ) -> Result<usize> {
        if approvals.is_empty() {
            return Ok(0);
        }
        let mut client = self.pg_client().await?;
        let tx = client.transaction().await?;
        let mut inserted = 0usize;
        for approval in approvals {
            tx.execute(
                "INSERT INTO erc721.nft_operators (token, owner, operator, approved, block_number, tx_hash, timestamp)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (token, owner, operator) DO UPDATE SET approved = EXCLUDED.approved, block_number = EXCLUDED.block_number, tx_hash = EXCLUDED.tx_hash, timestamp = EXCLUDED.timestamp",
                &[
                    &felt_to_blob(approval.token),
                    &felt_to_blob(approval.owner),
                    &felt_to_blob(approval.operator),
                    &((approval.approved as i32) as i64),
                    &(approval.block_number as i64),
                    &felt_to_blob(approval.tx_hash),
                    &approval.timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp()),
                ],
            ).await?;
            inserted += 1;
        }
        tx.commit().await?;
        Ok(inserted)
    }

    #[allow(clippy::too_many_arguments)]
    async fn pg_get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        tokens: &[Felt],
        token_ids: &[U256],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<NftTransferData>, Option<TransferCursor>)> {
        let client = self.pg_client().await?;
        let mut query = String::new();
        let mut params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.token_id, t.from_addr, t.to_addr, t.block_number, t.tx_hash, t.timestamp
                 FROM erc721.nft_wallet_activity wa
                 JOIN erc721.nft_transfers t ON wa.transfer_id = t.id
                 WHERE wa.wallet_address = ",
            );
            query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(wallet_addr)));
            if !tokens.is_empty() {
                let list = tokens
                    .iter()
                    .map(|token| Self::pg_next_param(&mut params, felt_to_blob(*token)))
                    .collect::<Vec<_>>()
                    .join(",");
                query.push_str(&format!(" AND wa.token IN ({list})"));
            }
        } else {
            query.push_str(
                "SELECT t.id, t.token, t.token_id, t.from_addr, t.to_addr, t.block_number, t.tx_hash, t.timestamp
                 FROM erc721.nft_transfers t
                 WHERE 1=1",
            );
            if let Some(from_addr) = from {
                query.push_str(" AND t.from_addr = ");
                query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(from_addr)));
            }
            if let Some(to_addr) = to {
                query.push_str(" AND t.to_addr = ");
                query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(to_addr)));
            }
            if !tokens.is_empty() {
                let list = tokens
                    .iter()
                    .map(|token| Self::pg_next_param(&mut params, felt_to_blob(*token)))
                    .collect::<Vec<_>>()
                    .join(",");
                query.push_str(&format!(" AND t.token IN ({list})"));
            }
        }

        if !token_ids.is_empty() {
            let list = token_ids
                .iter()
                .map(|tid| Self::pg_next_param(&mut params, u256_to_blob(*tid)))
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&format!(" AND t.token_id IN ({list})"));
        }
        if let Some(block_min) = block_from {
            query.push_str(" AND t.block_number >= ");
            query.push_str(&Self::pg_next_param(&mut params, block_min as i64));
        }
        if let Some(block_max) = block_to {
            query.push_str(" AND t.block_number <= ");
            query.push_str(&Self::pg_next_param(&mut params, block_max as i64));
        }
        if let Some(c) = cursor {
            let p1 = Self::pg_next_param(&mut params, c.block_number as i64);
            let p2 = Self::pg_next_param(&mut params, c.block_number as i64);
            let p3 = Self::pg_next_param(&mut params, c.id);
            query.push_str(&format!(
                " AND (t.block_number < {p1} OR (t.block_number = {p2} AND t.id < {p3}))"
            ));
        }
        query.push_str(" ORDER BY t.block_number DESC, t.id DESC LIMIT ");
        query.push_str(&Self::pg_next_param(&mut params, limit as i64));

        let refs: Vec<&(dyn PgToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let rows = client.query(&query, &refs).await?;
        let transfers: Vec<NftTransferData> = rows
            .into_iter()
            .map(|row| NftTransferData {
                id: Some(row.get::<usize, i64>(0)),
                token: blob_to_felt(&row.get::<usize, Vec<u8>>(1)),
                token_id: blob_to_u256(&row.get::<usize, Vec<u8>>(2)),
                from: blob_to_felt(&row.get::<usize, Vec<u8>>(3)),
                to: blob_to_felt(&row.get::<usize, Vec<u8>>(4)),
                block_number: row.get::<usize, i64>(5) as u64,
                tx_hash: blob_to_felt(&row.get::<usize, Vec<u8>>(6)),
                timestamp: Some(row.get::<usize, i64>(7)),
            })
            .collect();
        let next_cursor = if transfers.len() == limit as usize {
            transfers.last().map(|t| TransferCursor {
                block_number: t.block_number,
                id: t.id.unwrap_or_default(),
            })
        } else {
            None
        };
        Ok((transfers, next_cursor))
    }

    async fn pg_get_owner(&self, token: Felt, token_id: U256) -> Result<Option<Felt>> {
        let client = self.pg_client().await?;
        let row = client
            .query_opt(
                "SELECT owner FROM erc721.nft_ownership WHERE token = $1 AND token_id = $2",
                &[&felt_to_blob(token), &u256_to_blob(token_id)],
            )
            .await?;
        Ok(row.map(|r| blob_to_felt(&r.get::<usize, Vec<u8>>(0))))
    }

    async fn pg_get_ownership_by_owner(
        &self,
        owner: Felt,
        tokens: &[Felt],
        cursor: Option<OwnershipCursor>,
        limit: u32,
    ) -> Result<(Vec<NftOwnershipData>, Option<OwnershipCursor>)> {
        let client = self.pg_client().await?;
        let mut query = String::from(
            "SELECT id, token, token_id, owner, block_number FROM erc721.nft_ownership WHERE owner = ",
        );
        let mut params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();
        query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(owner)));
        if !tokens.is_empty() {
            let list = tokens
                .iter()
                .map(|token| Self::pg_next_param(&mut params, felt_to_blob(*token)))
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&format!(" AND token IN ({list})"));
        }
        if let Some(c) = cursor {
            let p1 = Self::pg_next_param(&mut params, c.block_number as i64);
            let p2 = Self::pg_next_param(&mut params, c.block_number as i64);
            let p3 = Self::pg_next_param(&mut params, c.id);
            query.push_str(&format!(
                " AND (block_number < {p1} OR (block_number = {p2} AND id < {p3}))"
            ));
        }
        query.push_str(" ORDER BY block_number DESC, id DESC LIMIT ");
        query.push_str(&Self::pg_next_param(&mut params, limit as i64));
        let refs: Vec<&(dyn PgToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let rows = client.query(&query, &refs).await?;
        let ownership: Vec<NftOwnershipData> = rows
            .into_iter()
            .map(|row| NftOwnershipData {
                id: Some(row.get::<usize, i64>(0)),
                token: blob_to_felt(&row.get::<usize, Vec<u8>>(1)),
                token_id: blob_to_u256(&row.get::<usize, Vec<u8>>(2)),
                owner: blob_to_felt(&row.get::<usize, Vec<u8>>(3)),
                block_number: row.get::<usize, i64>(4) as u64,
            })
            .collect();
        let next_cursor = if ownership.len() == limit as usize {
            ownership.last().map(|o| OwnershipCursor {
                block_number: o.block_number,
                id: o.id.unwrap_or_default(),
            })
        } else {
            None
        };
        Ok((ownership, next_cursor))
    }

    async fn pg_get_transfer_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT COUNT(*) FROM erc721.nft_transfers", &[])
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_token_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(DISTINCT token) FROM erc721.nft_transfers",
                &[],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_nft_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT COUNT(*) FROM erc721.nft_ownership", &[])
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_latest_block(&self) -> Result<Option<u64>> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT MAX(block_number) FROM erc721.nft_transfers", &[])
            .await?;
        let v: Option<i64> = row.get(0);
        Ok(v.map(|x| x as u64))
    }

    async fn pg_has_token_metadata(&self, token: Felt) -> Result<bool> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM erc721.token_metadata WHERE token = $1",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) > 0)
    }

    async fn pg_upsert_token_metadata(
        &self,
        token: Felt,
        name: Option<&str>,
        symbol: Option<&str>,
        total_supply: Option<U256>,
    ) -> Result<()> {
        let client = self.pg_client().await?;
        client.execute(
            "INSERT INTO erc721.token_metadata (token, name, symbol, total_supply)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (token) DO UPDATE SET
                 name = COALESCE(EXCLUDED.name, erc721.token_metadata.name),
                 symbol = COALESCE(EXCLUDED.symbol, erc721.token_metadata.symbol),
                 total_supply = COALESCE(EXCLUDED.total_supply, erc721.token_metadata.total_supply)",
            &[&felt_to_blob(token), &name, &symbol, &total_supply.map(u256_to_blob)],
        ).await?;
        Ok(())
    }

    async fn pg_get_token_metadata(
        &self,
        token: Felt,
    ) -> Result<Option<(Option<String>, Option<String>, Option<U256>)>> {
        let client = self.pg_client().await?;
        let row = client
            .query_opt(
                "SELECT name, symbol, total_supply FROM erc721.token_metadata WHERE token = $1",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(row.map(|r| {
            let supply: Option<Vec<u8>> = r.get(2);
            (r.get(0), r.get(1), supply.map(|b| blob_to_u256(&b)))
        }))
    }

    async fn pg_get_token_metadata_paginated(
        &self,
        cursor: Option<Felt>,
        limit: u32,
    ) -> Result<(
        Vec<(Felt, Option<String>, Option<String>, Option<U256>)>,
        Option<Felt>,
    )> {
        let client = self.pg_client().await?;
        let fetch_limit = limit.clamp(1, 1000) as i64 + 1;
        let rows = if let Some(cursor_token) = cursor {
            client
                .query(
                    "SELECT token, name, symbol, total_supply
                 FROM erc721.token_metadata
                 WHERE token > $1
                 ORDER BY token ASC
                 LIMIT $2",
                    &[&felt_to_blob(cursor_token), &fetch_limit],
                )
                .await?
        } else {
            client
                .query(
                    "SELECT token, name, symbol, total_supply
                 FROM erc721.token_metadata
                 ORDER BY token ASC
                 LIMIT $1",
                    &[&fetch_limit],
                )
                .await?
        };
        let mut out: Vec<(Felt, Option<String>, Option<String>, Option<U256>)> = rows
            .into_iter()
            .map(|row| {
                let supply: Option<Vec<u8>> = row.get(3);
                (
                    blob_to_felt(&row.get::<usize, Vec<u8>>(0)),
                    row.get(1),
                    row.get(2),
                    supply.map(|b| blob_to_u256(&b)),
                )
            })
            .collect();
        let capped = limit.clamp(1, 1000) as usize;
        let next_cursor = if out.len() > capped {
            let next = out[capped].0;
            out.truncate(capped);
            Some(next)
        } else {
            None
        };
        Ok((out, next_cursor))
    }

    async fn pg_has_token_uri(&self, token: Felt, token_id: U256) -> Result<bool> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM erc721.token_uris WHERE token = $1 AND token_id = $2",
                &[&felt_to_blob(token), &u256_to_blob(token_id)],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) > 0)
    }

    async fn pg_get_token_uris_by_contract(
        &self,
        token: Felt,
    ) -> Result<Vec<(U256, Option<String>, Option<String>)>> {
        let client = self.pg_client().await?;
        let rows = client
            .query(
                "SELECT token_id, uri, metadata_json
             FROM erc721.token_uris
             WHERE token = $1
             ORDER BY token_id ASC",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    blob_to_u256(&row.get::<usize, Vec<u8>>(0)),
                    row.get(1),
                    row.get(2),
                )
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl TokenUriStore for Erc721Storage {
    async fn store_token_uri(&self, result: &TokenUriResult) -> Result<()> {
        if self.backend == StorageBackend::Postgres {
            let mut client = self.pg_client().await?;
            let tx = client.transaction().await?;

            let token_blob = felt_to_blob(result.contract);
            let token_id_blob = u256_to_blob(result.token_id);

            tx.execute(
                "INSERT INTO erc721.token_uris (token, token_id, uri, metadata_json, updated_at)
                 VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(token, token_id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = EXCLUDED.updated_at",
                &[
                    &token_blob,
                    &token_id_blob,
                    &result.uri.as_deref(),
                    &result.metadata_json.as_deref(),
                ],
            )
            .await?;

            tx.execute(
                "DELETE FROM erc721.token_attributes WHERE token = $1 AND token_id = $2",
                &[&token_blob, &token_id_blob],
            )
            .await?;

            if let Some(metadata_json) = &result.metadata_json {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(metadata_json) {
                    if let Some(attrs) = value.get("attributes").and_then(|a| a.as_array()) {
                        for attr in attrs {
                            let key = attr
                                .get("trait_type")
                                .or_else(|| attr.get("key"))
                                .and_then(|v| v.as_str());
                            let value = attr.get("value").and_then(|v| {
                                v.as_str().map(ToOwned::to_owned).or_else(|| {
                                    if v.is_null() {
                                        None
                                    } else {
                                        Some(v.to_string())
                                    }
                                })
                            });

                            if let (Some(key), Some(value)) = (key, value) {
                                tx.execute(
                                    "INSERT INTO erc721.token_attributes (token, token_id, key, value)
                                     VALUES ($1, $2, $3, $4)
                                     ON CONFLICT (token, token_id, key) DO UPDATE SET value = EXCLUDED.value",
                                    &[&token_blob, &token_id_blob, &key, &value],
                                ).await?;
                            }
                        }
                    }
                }
            }

            tx.commit().await?;
            return Ok(());
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let token_blob = felt_to_blob(result.contract);
        let token_id_blob = u256_to_blob(result.token_id);

        tx.execute(
            "INSERT INTO token_uris (token, token_id, uri, metadata_json, updated_at)
             VALUES (?1, ?2, ?3, ?4, strftime('%s', 'now'))
             ON CONFLICT(token, token_id) DO UPDATE SET
                uri = excluded.uri,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at",
            params![
                &token_blob,
                &token_id_blob,
                result.uri.as_deref(),
                result.metadata_json.as_deref()
            ],
        )?;

        tx.execute(
            "DELETE FROM token_attributes WHERE token = ?1 AND token_id = ?2",
            params![&token_blob, &token_id_blob],
        )?;

        if let Some(metadata_json) = &result.metadata_json {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(metadata_json) {
                if let Some(attrs) = value.get("attributes").and_then(|a| a.as_array()) {
                    let mut attr_stmt = tx.prepare_cached(
                        "INSERT OR REPLACE INTO token_attributes (token, token_id, key, value)
                         VALUES (?1, ?2, ?3, ?4)",
                    )?;

                    for attr in attrs {
                        let key = attr
                            .get("trait_type")
                            .or_else(|| attr.get("key"))
                            .and_then(|v| v.as_str());
                        let value = attr.get("value").and_then(|v| {
                            v.as_str().map(ToOwned::to_owned).or_else(|| {
                                if v.is_null() {
                                    None
                                } else {
                                    Some(v.to_string())
                                }
                            })
                        });

                        if let (Some(key), Some(value)) = (key, value) {
                            attr_stmt.execute(params![&token_blob, &token_id_blob, key, value])?;
                        }
                    }
                }
            }
        }

        tx.commit()?;
        Ok(())
    }
}
