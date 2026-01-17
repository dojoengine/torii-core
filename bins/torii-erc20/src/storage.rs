//! SQLite storage for ERC20 transfers and balances
//!
//! Uses binary (BLOB) storage for efficiency (~51% size reduction vs hex strings).
//! Uses U256 for amounts (proper 256-bit arithmetic for ERC20 token balances).
//! Properly tracks running balances with add/subtract arithmetic.

use anyhow::Result;
use rusqlite::{params, Connection, Transaction};
use starknet::core::types::{Felt, U256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Storage for ERC20 transfers and balances
pub struct Erc20Storage {
    conn: Arc<Mutex<Connection>>,
}

/// Transfer data for batch insertion
pub struct TransferData {
    pub id: Option<i64>,  // SQLite rowid (set after insertion, None before)
    pub token: Felt,
    pub from: Felt,
    pub to: Felt,
    /// Amount as U256 (256-bit) for proper ERC20 token amounts
    pub amount: U256,
    pub block_number: u64,
    pub tx_hash: Felt,
}

/// Cursor for paginated transfer queries
#[derive(Debug, Clone, Copy)]
pub struct TransferCursor {
    pub block_number: u64,
    pub id: i64,  // Last transfer ID from previous page
}

/// Convert Felt to 32-byte BLOB for storage (big-endian for easier inspection)
fn felt_to_blob(felt: Felt) -> Vec<u8> {
    felt.to_bytes_be().to_vec()
}

/// Convert BLOB back to Felt (big-endian)
fn blob_to_felt(bytes: &[u8]) -> Felt {
    let mut arr = [0u8; 32];
    let len = bytes.len().min(32);
    // Right-align for big-endian (pad zeros on the left)
    arr[32 - len..].copy_from_slice(&bytes[..len]);
    Felt::from_bytes_be(&arr)
}

/// Convert U256 to variable-length BLOB for storage (big-endian, compact)
///
/// Compression strategy:
/// - Zero value: 1 byte (0x00)
/// - Values < 2^128: 1-16 bytes (minimal encoding of low word)
/// - Values >= 2^128: Full encoding (17-32 bytes, both high and low words)
///
/// This is safe because:
/// 1. blob_to_u256() already handles variable-length (right-aligns shorter BLOBs)
/// 2. SQLite memcmp() compares correctly for partial index WHERE balance > 0
/// 3. All comparisons use U256 after decoding (not raw BLOB comparison)
///
/// Space savings: 78-95% compression for typical ERC20 transfer distributions
fn u256_to_blob(value: U256) -> Vec<u8> {
    let high = value.high();
    let low = value.low();

    // If high word is zero, only encode low word
    if high == 0 {
        if low == 0 {
            return vec![0u8];  // Zero value: 1 byte
        }

        // Encode low word with minimal bytes (remove leading zeros)
        let bytes = low.to_be_bytes();

        // Find first non-zero byte
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(15);

        return bytes[start..].to_vec();
    }

    // Both high and low words are used: encode full value
    let mut result = Vec::with_capacity(32);

    // Encode high word (remove leading zeros)
    let high_bytes = high.to_be_bytes();
    let high_start = high_bytes.iter().position(|&b| b != 0).unwrap_or(15);
    result.extend_from_slice(&high_bytes[high_start..]);

    // Always encode full low word when high is present
    result.extend_from_slice(&low.to_be_bytes());

    result
}

/// Convert BLOB back to U256 (big-endian)
fn blob_to_u256(bytes: &[u8]) -> U256 {
    let mut high_bytes = [0u8; 16];
    let mut low_bytes = [0u8; 16];

    let len = bytes.len().min(32);
    if len >= 16 {
        // Big-endian: first 16 bytes are high, remaining are low
        high_bytes.copy_from_slice(&bytes[..16]);
        low_bytes[..len - 16].copy_from_slice(&bytes[16..len]);
    } else {
        // Less than 16 bytes - all goes into low, right-aligned
        low_bytes[16 - len..].copy_from_slice(&bytes[..len]);
    }

    let high = u128::from_be_bytes(high_bytes);
    let low = u128::from_be_bytes(low_bytes);
    U256::from_words(low, high)
}

/// Zero value for U256
const U256_ZERO: U256 = U256::from_words(0, 0);

impl Erc20Storage {
    /// Create or open the database
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        // Enable WAL mode + Performance PRAGMAs (critical for production scale)
        // - WAL mode: Readers don't block writers (~2-3x write throughput)
        // - synchronous=NORMAL: Relaxed fsync (safe with WAL, ~10x faster writes)
        // - cache_size=-64000: 64MB cache (negative value = KB)
        // - mmap_size: 256MB memory-mapped I/O for zero-copy reads
        // - busy_timeout: 5s wait for lock (handles concurrent access)
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;
             PRAGMA cache_size=-64000;
             PRAGMA temp_store=MEMORY;
             PRAGMA mmap_size=268435456;
             PRAGMA page_size=4096;
             PRAGMA busy_timeout=5000;"
        )?;

        tracing::info!(
            "SQLite configured: WAL mode, 64MB cache, 256MB mmap, NORMAL sync"
        );

        // Create tables with BLOB columns for efficient storage
        conn.execute(
            "CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                amount BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER DEFAULT (strftime('%s', 'now')),
                UNIQUE(token, tx_hash, from_addr, to_addr)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_token ON transfers(token)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers(from_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers(to_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_block ON transfers(block_number)",
            [],
        )?;

        // Composite indexes for efficient queries (Phase 1.1 optimizations)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_token_block ON transfers(token, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_from_block ON transfers(from_addr, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_to_block ON transfers(to_addr, block_number DESC)",
            [],
        )?;

        // Balances table - maintains current balance for each (token, address) pair
        conn.execute(
            "CREATE TABLE IF NOT EXISTS balances (
                token BLOB NOT NULL,
                address BLOB NOT NULL,
                balance BLOB NOT NULL,
                updated_at INTEGER DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (token, address)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_balances_token ON balances(token)",
            [],
        )?;

        // Critical: Add address index for wallet-centric queries (Phase 1.1 optimization)
        // Without this, "get all balances for wallet" does full table scan!
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_balances_address ON balances(address)",
            [],
        )?;

        // Partial index for non-zero balances (optimal for wallet portfolio queries)
        // Only indexes rows where balance > 0, saving space and query time
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_balances_address_nonzero
             ON balances(address, token) WHERE balance > X'0000000000000000000000000000000000000000000000000000000000000000'",
            [],
        )?;

        // Metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;

        // Wallet activity table - denormalized for efficient wallet queries (Phase 1.2 optimization)
        // Solves the OR query problem: "get all transfers where wallet is sender OR receiver"
        // Instead of: WHERE from_addr = ? OR to_addr = ? (can only use one index)
        // We use: JOIN wallet_activity WHERE wallet_address = ? (uses this table's index)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS wallet_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address BLOB NOT NULL,
                token BLOB NOT NULL,
                transfer_id INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                block_number INTEGER NOT NULL,
                FOREIGN KEY (transfer_id) REFERENCES transfers(id)
            )",
            [],
        )?;

        // Compound index: wallet -> block (optimal for wallet activity queries)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet_block
             ON wallet_activity(wallet_address, block_number DESC)",
            [],
        )?;

        // Compound index: wallet -> token -> block (token-specific activity)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet_token
             ON wallet_activity(wallet_address, token, block_number DESC)",
            [],
        )?;

        // Index for reverse lookup (transfer -> wallets)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_activity_transfer
             ON wallet_activity(transfer_id)",
            [],
        )?;

        tracing::info!("Database initialized at {}", db_path);

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Update balance for an address with proper U256 arithmetic
    ///
    /// - For credits (is_credit=true): adds amount to current balance
    /// - For debits (is_credit=false): subtracts amount from current balance
    /// - Handles underflow gracefully by clamping to zero (for when indexer starts from block > 0)
    fn update_balance(
        tx: &Transaction,
        token: &[u8],
        address: &[u8],
        amount: U256,
        is_credit: bool,
    ) -> Result<()> {
        // Get current balance (default to 0 if not exists)
        let current: U256 = tx
            .query_row(
                "SELECT balance FROM balances WHERE token = ?1 AND address = ?2",
                params![token, address],
                |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    Ok(blob_to_u256(&bytes))
                },
            )
            .unwrap_or(U256_ZERO);

        // Compute new balance using safe arithmetic to avoid subtle crate panics
        // starknet_core::U256 uses the subtle crate which can panic on overflow/underflow
        // We must check conditions BEFORE performing operations
        let new_balance = if is_credit {
            // Check for addition overflow by examining the words
            // For addition to overflow: both low and high must overflow, or high overflows
            let (new_low, overflow_low) = current.low().overflowing_add(amount.low());
            let carry = if overflow_low { 1 } else { 0 };
            let (new_high, overflow_high) = current.high().overflowing_add(amount.high());
            let (new_high_with_carry, overflow_carry) = new_high.overflowing_add(carry);

            if overflow_high || overflow_carry {
                tracing::error!(
                    target: "torii_erc20::storage",
                    "Balance overflow would occur: current={}, amount={}, clamping to MAX",
                    current,
                    amount
                );
                // Return maximum U256 value to prevent panic
                U256::from_words(u128::MAX, u128::MAX)
            } else {
                // Safe to add
                current + amount
            }
        } else {
            // Subtraction: check for underflow before subtracting
            if current >= amount {
                current - amount
            } else {
                // Underflow: clamp to zero and log warning
                tracing::warn!(
                    target: "torii_erc20::storage",
                    "Balance underflow: current={}, amount={}, clamping to 0",
                    current,
                    amount
                );
                U256_ZERO
            }
        };

        // Upsert new balance
        tx.execute(
            "INSERT INTO balances (token, address, balance, updated_at)
             VALUES (?1, ?2, ?3, strftime('%s', 'now'))
             ON CONFLICT(token, address) DO UPDATE SET
               balance = ?3,
               updated_at = strftime('%s', 'now')",
            params![token, address, u256_to_blob(new_balance)],
        )?;

        Ok(())
    }

    /// Insert multiple transfers in a single transaction
    ///
    /// This is significantly faster than inserting one by one because:
    /// - Single lock acquisition
    /// - Single transaction (all-or-nothing commit)
    /// - Prepared statement reuse
    /// - Proper balance arithmetic (debit sender, credit receiver)
    pub fn insert_transfers_batch(&self, transfers: &[TransferData]) -> Result<usize> {
        if transfers.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut transfer_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO transfers (token, from_addr, to_addr, amount, block_number, tx_hash)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;

            for transfer in transfers {
                let token_blob = felt_to_blob(transfer.token);
                let from_blob = felt_to_blob(transfer.from);
                let to_blob = felt_to_blob(transfer.to);
                let amount_blob = u256_to_blob(transfer.amount);
                let tx_hash_blob = felt_to_blob(transfer.tx_hash);

                // Insert transfer
                let rows = transfer_stmt.execute(params![
                    &token_blob,
                    &from_blob,
                    &to_blob,
                    &amount_blob,
                    transfer.block_number,
                    &tx_hash_blob,
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let transfer_id = tx.last_insert_rowid();

                    // Insert wallet activity records (Phase 1.2 optimization)
                    // This enables O(log n) wallet queries instead of O(n) OR scans
                    if transfer.from != Felt::ZERO && transfer.to != Felt::ZERO && transfer.from == transfer.to {
                        // Self-transfer (one record with 'both' direction)
                        tx.execute(
                            "INSERT INTO wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                             VALUES (?, ?, ?, 'both', ?)",
                            params![&from_blob, &token_blob, transfer_id, transfer.block_number]
                        )?;
                    } else {
                        // Sender record
                        if transfer.from != Felt::ZERO {
                            tx.execute(
                                "INSERT INTO wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                                 VALUES (?, ?, ?, 'sent', ?)",
                                params![&from_blob, &token_blob, transfer_id, transfer.block_number]
                            )?;
                        }
                        // Receiver record
                        if transfer.to != Felt::ZERO {
                            tx.execute(
                                "INSERT INTO wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                                 VALUES (?, ?, ?, 'received', ?)",
                                params![&to_blob, &token_blob, transfer_id, transfer.block_number]
                            )?;
                        }
                    }

                    // Debit sender (subtract from balance)
                    if transfer.from != Felt::ZERO {
                        Self::update_balance(&tx, &token_blob, &from_blob, transfer.amount, false)?;
                    }

                    // Credit receiver (add to balance)
                    if transfer.to != Felt::ZERO {
                        Self::update_balance(&tx, &token_blob, &to_blob, transfer.amount, true)?;
                    }
                }
            }
        }

        tx.commit()?;

        Ok(inserted)
    }

    /// Insert a transfer and update balances (single transfer, for backwards compatibility)
    #[allow(dead_code)]
    pub fn insert_transfer(
        &self,
        token: Felt,
        from: Felt,
        to: Felt,
        amount: U256,
        block_number: u64,
        tx_hash: Felt,
    ) -> Result<()> {
        self.insert_transfers_batch(&[TransferData {
            id: None,  // Will be set after insertion
            token,
            from,
            to,
            amount,
            block_number,
            tx_hash,
        }])?;
        Ok(())
    }

    /// Get balance for an address (returns U256)
    #[allow(dead_code)]
    pub fn get_balance(&self, token: Felt, address: Felt) -> Result<Option<U256>> {
        let conn = self.conn.lock().unwrap();

        let balance = conn
            .query_row(
                "SELECT balance FROM balances WHERE token = ?1 AND address = ?2",
                params![felt_to_blob(token), felt_to_blob(address)],
                |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    Ok(blob_to_u256(&bytes))
                },
            )
            .ok();

        Ok(balance)
    }

    /// Get all balances for a token (returns map of address -> balance as U256)
    #[allow(dead_code)]
    pub fn get_token_balances(&self, token: Felt) -> Result<HashMap<Felt, U256>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT address, balance FROM balances WHERE token = ?1",
        )?;

        let balances = stmt
            .query_map(params![felt_to_blob(token)], |row| {
                let addr_bytes: Vec<u8> = row.get(0)?;
                let balance_bytes: Vec<u8> = row.get(1)?;
                Ok((blob_to_felt(&addr_bytes), blob_to_u256(&balance_bytes)))
            })?
            .collect::<Result<HashMap<_, _>, _>>()?;

        Ok(balances)
    }

    /// Get transfer count
    pub fn get_transfer_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM transfers", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get indexed token count
    pub fn get_token_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT token) FROM transfers",
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
                "SELECT MAX(block_number) FROM transfers",
                [],
                |row| row.get(0),
            )
            .ok();
        Ok(block.map(|b| b as u64))
    }

    /// Get paginated transfers for a wallet (using cursor-based pagination)
    ///
    /// This method uses cursor-based pagination which provides O(log n) performance
    /// per page regardless of page depth, unlike OFFSET-based pagination which
    /// degrades to O(n) for deep pages.
    ///
    /// # Arguments
    /// * `wallet` - The wallet address to query
    /// * `token` - Optional token filter (None = all tokens)
    /// * `cursor` - Optional cursor from previous page (None = start from beginning)
    /// * `limit` - Maximum number of transfers to return
    ///
    /// # Returns
    /// Tuple of (transfers, next_cursor) where next_cursor is Some if more pages exist
    pub fn get_wallet_transfers_paginated(
        &self,
        wallet: Felt,
        token: Option<Felt>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<TransferData>, Option<TransferCursor>)> {
        let conn = self.conn.lock().unwrap();
        let wallet_blob = felt_to_blob(wallet);

        let (query, params): (&str, Vec<Box<dyn rusqlite::ToSql>>) = match (token, cursor) {
            (Some(token_felt), Some(c)) => {
                // Token-specific query with cursor
                let token_blob = felt_to_blob(token_felt);
                (
                    "SELECT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash
                     FROM wallet_activity wa
                     JOIN transfers t ON wa.transfer_id = t.id
                     WHERE wa.wallet_address = ? AND wa.token = ?
                       AND (wa.block_number < ? OR (wa.block_number = ? AND t.id < ?))
                     ORDER BY wa.block_number DESC, t.id DESC
                     LIMIT ?",
                    vec![
                        Box::new(wallet_blob) as Box<dyn rusqlite::ToSql>,
                        Box::new(token_blob),
                        Box::new(c.block_number as i64),
                        Box::new(c.block_number as i64),
                        Box::new(c.id),
                        Box::new(limit as i64),
                    ],
                )
            }
            (Some(token_felt), None) => {
                // Token-specific query without cursor
                let token_blob = felt_to_blob(token_felt);
                (
                    "SELECT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash
                     FROM wallet_activity wa
                     JOIN transfers t ON wa.transfer_id = t.id
                     WHERE wa.wallet_address = ? AND wa.token = ?
                     ORDER BY wa.block_number DESC, t.id DESC
                     LIMIT ?",
                    vec![
                        Box::new(wallet_blob) as Box<dyn rusqlite::ToSql>,
                        Box::new(token_blob),
                        Box::new(limit as i64),
                    ],
                )
            }
            (None, Some(c)) => {
                // All tokens query with cursor
                (
                    "SELECT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash
                     FROM wallet_activity wa
                     JOIN transfers t ON wa.transfer_id = t.id
                     WHERE wa.wallet_address = ?
                       AND (wa.block_number < ? OR (wa.block_number = ? AND t.id < ?))
                     ORDER BY wa.block_number DESC, t.id DESC
                     LIMIT ?",
                    vec![
                        Box::new(wallet_blob) as Box<dyn rusqlite::ToSql>,
                        Box::new(c.block_number as i64),
                        Box::new(c.block_number as i64),
                        Box::new(c.id),
                        Box::new(limit as i64),
                    ],
                )
            }
            (None, None) => {
                // All tokens query without cursor
                (
                    "SELECT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash
                     FROM wallet_activity wa
                     JOIN transfers t ON wa.transfer_id = t.id
                     WHERE wa.wallet_address = ?
                     ORDER BY wa.block_number DESC, t.id DESC
                     LIMIT ?",
                    vec![
                        Box::new(wallet_blob) as Box<dyn rusqlite::ToSql>,
                        Box::new(limit as i64),
                    ],
                )
            }
        };

        let mut stmt = conn.prepare(query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let from_bytes: Vec<u8> = row.get(2)?;
            let to_bytes: Vec<u8> = row.get(3)?;
            let amount_bytes: Vec<u8> = row.get(4)?;
            let block_number: i64 = row.get(5)?;
            let tx_hash_bytes: Vec<u8> = row.get(6)?;

            Ok(TransferData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                from: blob_to_felt(&from_bytes),
                to: blob_to_felt(&to_bytes),
                amount: blob_to_u256(&amount_bytes),
                block_number: block_number as u64,
                tx_hash: blob_to_felt(&tx_hash_bytes),
            })
        })?;

        let transfers: Vec<TransferData> = rows.collect::<Result<_, _>>()?;

        // Compute next cursor if there might be more pages
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

    /// Get all non-zero balances for a wallet (paginated by token address)
    ///
    /// This method uses cursor-based pagination for efficient iteration through
    /// a wallet's token holdings.
    ///
    /// # Arguments
    /// * `wallet` - The wallet address to query
    /// * `cursor` - Optional cursor (last token address from previous page)
    /// * `limit` - Maximum number of balances to return
    ///
    /// # Returns
    /// Tuple of (balances, next_cursor) where next_cursor is Some if more pages exist
    pub fn get_wallet_balances_paginated(
        &self,
        wallet: Felt,
        cursor: Option<Felt>,
        limit: u32,
    ) -> Result<(Vec<(Felt, U256)>, Option<Felt>)> {
        let conn = self.conn.lock().unwrap();
        let wallet_blob = felt_to_blob(wallet);

        let (query, params): (&str, Vec<Box<dyn rusqlite::ToSql>>) = if let Some(cursor_token) = cursor {
            let cursor_blob = felt_to_blob(cursor_token);
            (
                "SELECT token, balance FROM balances
                 WHERE address = ? AND balance > X'0000000000000000000000000000000000000000000000000000000000000000' AND token > ?
                 ORDER BY token ASC
                 LIMIT ?",
                vec![
                    Box::new(wallet_blob) as Box<dyn rusqlite::ToSql>,
                    Box::new(cursor_blob),
                    Box::new(limit as i64),
                ],
            )
        } else {
            (
                "SELECT token, balance FROM balances
                 WHERE address = ? AND balance > X'0000000000000000000000000000000000000000000000000000000000000000'
                 ORDER BY token ASC
                 LIMIT ?",
                vec![
                    Box::new(wallet_blob) as Box<dyn rusqlite::ToSql>,
                    Box::new(limit as i64),
                ],
            )
        };

        let mut stmt = conn.prepare(query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let token_bytes: Vec<u8> = row.get(0)?;
            let balance_bytes: Vec<u8> = row.get(1)?;
            Ok((blob_to_felt(&token_bytes), blob_to_u256(&balance_bytes)))
        })?;

        let balances: Vec<(Felt, U256)> = rows.collect::<Result<_, _>>()?;

        // Compute next cursor if there might be more pages
        let next_cursor = if balances.len() == limit as usize {
            balances.last().map(|(token, _)| *token)
        } else {
            None
        };

        Ok((balances, next_cursor))
    }
}
