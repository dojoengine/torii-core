//! SQLite storage for ERC1155 token transfers
//!
//! Like ERC20, we only track transfer history - NOT balances.
//! Clients should query the chain for actual balances to avoid inaccuracies.

use anyhow::Result;
use rusqlite::{params, Connection};
use starknet::core::types::{Felt, U256};
use std::sync::{Arc, Mutex};

/// Storage for ERC1155 token data
pub struct Erc1155Storage {
    conn: Arc<Mutex<Connection>>,
}

/// Token transfer data for batch insertion
pub struct TokenTransferData {
    pub id: Option<i64>,
    pub token: Felt,
    pub operator: Felt,
    pub from: Felt,
    pub to: Felt,
    pub token_id: U256,
    pub amount: U256,
    pub is_batch: bool,
    pub batch_index: u32,
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

/// Convert Felt to 32-byte BLOB for storage (big-endian)
fn felt_to_blob(felt: Felt) -> Vec<u8> {
    felt.to_bytes_be().to_vec()
}

/// Convert BLOB back to Felt (big-endian)
fn blob_to_felt(bytes: &[u8]) -> Felt {
    let mut arr = [0u8; 32];
    let len = bytes.len().min(32);
    arr[32 - len..].copy_from_slice(&bytes[..len]);
    Felt::from_bytes_be(&arr)
}

/// Convert U256 to variable-length BLOB for storage (big-endian, compact)
fn u256_to_blob(value: U256) -> Vec<u8> {
    let high = value.high();
    let low = value.low();

    if high == 0 {
        if low == 0 {
            return vec![0u8];
        }
        let bytes = low.to_be_bytes();
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(15);
        return bytes[start..].to_vec();
    }

    let mut result = Vec::with_capacity(32);
    let high_bytes = high.to_be_bytes();
    let high_start = high_bytes.iter().position(|&b| b != 0).unwrap_or(15);
    result.extend_from_slice(&high_bytes[high_start..]);
    result.extend_from_slice(&low.to_be_bytes());
    result
}

/// Convert BLOB back to U256 (big-endian)
fn blob_to_u256(bytes: &[u8]) -> U256 {
    let len = bytes.len();

    if len == 0 {
        return U256::from(0u64);
    }

    if len <= 16 {
        let mut low_bytes = [0u8; 16];
        low_bytes[16 - len..].copy_from_slice(bytes);
        let low = u128::from_be_bytes(low_bytes);
        U256::from_words(low, 0)
    } else {
        let high_len = len - 16;
        let mut high_bytes = [0u8; 16];
        high_bytes[16 - high_len..].copy_from_slice(&bytes[..high_len]);
        let high = u128::from_be_bytes(high_bytes);

        let mut low_bytes = [0u8; 16];
        low_bytes.copy_from_slice(&bytes[high_len..]);
        let low = u128::from_be_bytes(low_bytes);

        U256::from_words(low, high)
    }
}

impl Erc1155Storage {
    /// Create or open the database
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        // Enable WAL mode + Performance PRAGMAs
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;
             PRAGMA cache_size=-64000;
             PRAGMA temp_store=MEMORY;
             PRAGMA mmap_size=268435456;
             PRAGMA page_size=4096;
             PRAGMA busy_timeout=5000;",
        )?;

        tracing::info!("SQLite configured: WAL mode, 64MB cache, 256MB mmap, NORMAL sync");

        // Token transfers table (both single and batch transfers)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                operator BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                token_id BLOB NOT NULL,
                amount BLOB NOT NULL,
                is_batch INTEGER NOT NULL DEFAULT 0,
                batch_index INTEGER NOT NULL DEFAULT 0,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER,
                UNIQUE(token, tx_hash, token_id, from_addr, to_addr, batch_index)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transfers_token ON token_transfers(token)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transfers_from ON token_transfers(from_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transfers_to ON token_transfers(to_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transfers_block ON token_transfers(block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transfers_token_id ON token_transfers(token, token_id)",
            [],
        )?;

        // Wallet activity table for efficient OR queries
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_wallet_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address BLOB NOT NULL,
                token BLOB NOT NULL,
                transfer_id INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                block_number INTEGER NOT NULL,
                FOREIGN KEY (transfer_id) REFERENCES token_transfers(id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_wallet_activity_wallet_block
             ON token_wallet_activity(wallet_address, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_wallet_activity_wallet_token
             ON token_wallet_activity(wallet_address, token, block_number DESC)",
            [],
        )?;

        // Operator approvals
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_operators (
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

        // URI metadata
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_uris (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                uri TEXT NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER,
                UNIQUE(token, token_id)
            )",
            [],
        )?;

        tracing::info!("ERC1155 database initialized at {}", db_path);

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Insert multiple transfers in a single transaction
    pub fn insert_transfers_batch(&self, transfers: &[TokenTransferData]) -> Result<usize> {
        if transfers.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO token_transfers (token, operator, from_addr, to_addr, token_id, amount, is_batch, batch_index, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, COALESCE(?11, strftime('%s', 'now')))",
            )?;

            for transfer in transfers {
                let token_blob = felt_to_blob(transfer.token);
                let operator_blob = felt_to_blob(transfer.operator);
                let from_blob = felt_to_blob(transfer.from);
                let to_blob = felt_to_blob(transfer.to);
                let token_id_blob = u256_to_blob(transfer.token_id);
                let amount_blob = u256_to_blob(transfer.amount);
                let tx_hash_blob = felt_to_blob(transfer.tx_hash);

                let rows = stmt.execute(params![
                    &token_blob,
                    &operator_blob,
                    &from_blob,
                    &to_blob,
                    &token_id_blob,
                    &amount_blob,
                    transfer.is_batch as i32,
                    transfer.batch_index,
                    transfer.block_number,
                    &tx_hash_blob,
                    transfer.timestamp,
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let transfer_id = tx.last_insert_rowid();

                    // Insert wallet activity records
                    if transfer.from != Felt::ZERO
                        && transfer.to != Felt::ZERO
                        && transfer.from == transfer.to
                    {
                        tx.execute(
                            "INSERT INTO token_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                             VALUES (?, ?, ?, 'both', ?)",
                            params![&from_blob, &token_blob, transfer_id, transfer.block_number],
                        )?;
                    } else {
                        if transfer.from != Felt::ZERO {
                            tx.execute(
                                "INSERT INTO token_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                                 VALUES (?, ?, ?, 'sent', ?)",
                                params![&from_blob, &token_blob, transfer_id, transfer.block_number],
                            )?;
                        }
                        if transfer.to != Felt::ZERO {
                            tx.execute(
                                "INSERT INTO token_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                                 VALUES (?, ?, ?, 'received', ?)",
                                params![&to_blob, &token_blob, transfer_id, transfer.block_number],
                            )?;
                        }
                    }
                }
            }
        }

        tx.commit()?;
        Ok(inserted)
    }

    /// Insert operator approvals in a single transaction
    pub fn insert_operator_approvals_batch(&self, approvals: &[OperatorApprovalData]) -> Result<usize> {
        if approvals.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO token_operators (token, owner, operator, approved, block_number, tx_hash, timestamp)
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
    pub fn get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        operator: Option<Felt>,
        tokens: &[Felt],
        token_ids: &[U256],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<TokenTransferData>, Option<TransferCursor>)> {
        let conn = self.conn.lock().unwrap();

        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.operator, t.from_addr, t.to_addr, t.token_id, t.amount, t.is_batch, t.batch_index, t.block_number, t.tx_hash, t.timestamp
                 FROM token_wallet_activity wa
                 JOIN token_transfers t ON wa.transfer_id = t.id
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
                "SELECT t.id, t.token, t.operator, t.from_addr, t.to_addr, t.token_id, t.amount, t.is_batch, t.batch_index, t.block_number, t.tx_hash, t.timestamp
                 FROM token_transfers t
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

            if let Some(op_addr) = operator {
                query.push_str(" AND t.operator = ?");
                params_vec.push(Box::new(felt_to_blob(op_addr)));
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
        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let operator_bytes: Vec<u8> = row.get(2)?;
            let from_bytes: Vec<u8> = row.get(3)?;
            let to_bytes: Vec<u8> = row.get(4)?;
            let token_id_bytes: Vec<u8> = row.get(5)?;
            let amount_bytes: Vec<u8> = row.get(6)?;
            let is_batch: i32 = row.get(7)?;
            let batch_index: i32 = row.get(8)?;
            let block_number: i64 = row.get(9)?;
            let tx_hash_bytes: Vec<u8> = row.get(10)?;
            let timestamp: Option<i64> = row.get(11)?;

            Ok(TokenTransferData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                operator: blob_to_felt(&operator_bytes),
                from: blob_to_felt(&from_bytes),
                to: blob_to_felt(&to_bytes),
                token_id: blob_to_u256(&token_id_bytes),
                amount: blob_to_u256(&amount_bytes),
                is_batch: is_batch != 0,
                batch_index: batch_index as u32,
                block_number: block_number as u64,
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp,
            })
        })?;

        let transfers: Vec<TokenTransferData> = rows.collect::<Result<_, _>>()?;

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

    /// Get transfer count
    pub fn get_transfer_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM token_transfers", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get unique token contract count
    pub fn get_token_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT token) FROM token_transfers",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get unique token ID count
    pub fn get_token_id_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT token || token_id) FROM token_transfers",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get latest block number indexed
    pub fn get_latest_block(&self) -> Result<Option<u64>> {
        let conn = self.conn.lock().unwrap();
        let block: Option<i64> = conn
            .query_row(
                "SELECT MAX(block_number) FROM token_transfers",
                [],
                |row| row.get(0),
            )
            .ok();
        Ok(block.map(|b| b as u64))
    }
}
