//! SQLite storage for ERC20 transfers and approvals
//!
//! Uses binary (BLOB) storage for efficiency (~51% size reduction vs hex strings).
//! Uses U256 for amounts (proper 256-bit arithmetic for ERC20 token amounts).
//!
//! Note: Balance tracking was intentionally removed because balances computed from
//! transfers are inherently incorrect - some addresses have initial balances without
//! transfer history (genesis allocations, airdrops). Clients should fetch actual
//! balances from the chain when needed.

use anyhow::Result;
use rusqlite::{params, Connection};
use starknet::core::types::{Felt, U256};
use std::sync::{Arc, Mutex};
use torii_common::{blob_to_felt, blob_to_u256, felt_to_blob, u256_to_blob};

/// Direction filter for transfer queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransferDirection {
    /// All transfers (sent and received)
    #[default]
    All,
    /// Only sent transfers (wallet is sender)
    Sent,
    /// Only received transfers (wallet is receiver)
    Received,
}

/// Storage for ERC20 transfers and approvals
pub struct Erc20Storage {
    conn: Arc<Mutex<Connection>>,
}

/// Transfer data for batch insertion
pub struct TransferData {
    pub id: Option<i64>,
    pub token: Felt,
    pub from: Felt,
    pub to: Felt,
    /// Amount as U256 (256-bit) for proper ERC20 token amounts
    pub amount: U256,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// Approval data for batch insertion
pub struct ApprovalData {
    pub id: Option<i64>,
    pub token: Felt,
    pub owner: Felt,
    pub spender: Felt,
    /// Amount as U256 (256-bit) for proper ERC20 token amounts
    pub amount: U256,
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

/// Cursor for paginated approval queries
#[derive(Debug, Clone, Copy)]
pub struct ApprovalCursor {
    pub block_number: u64,
    pub id: i64,
}

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
             PRAGMA busy_timeout=5000;",
        )?;

        tracing::info!(target: "torii_erc20::storage", "SQLite configured: WAL mode, 64MB cache, 256MB mmap, NORMAL sync");

        // Create transfers table with BLOB columns for efficient storage
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

        // Composite indexes for efficient queries
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

        // Create approvals table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                owner BLOB NOT NULL,
                spender BLOB NOT NULL,
                amount BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER DEFAULT (strftime('%s', 'now')),
                UNIQUE(token, tx_hash, owner, spender)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_owner ON approvals(owner, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_spender ON approvals(spender, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_token ON approvals(token, block_number DESC)",
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

        // Wallet activity table - denormalized for efficient wallet queries
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

        // Approval activity table - similar pattern for approvals
        conn.execute(
            "CREATE TABLE IF NOT EXISTS approval_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_address BLOB NOT NULL,
                token BLOB NOT NULL,
                approval_id INTEGER NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('owner', 'spender', 'both')),
                block_number INTEGER NOT NULL,
                FOREIGN KEY (approval_id) REFERENCES approvals(id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approval_activity_account_block
             ON approval_activity(account_address, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approval_activity_account_token
             ON approval_activity(account_address, token, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approval_activity_approval
             ON approval_activity(approval_id)",
            [],
        )?;

        tracing::info!(target: "torii_erc20::storage", db_path = %db_path, "Database initialized");

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Insert multiple transfers in a single transaction
    ///
    /// This is significantly faster than inserting one by one because:
    /// - Single lock acquisition
    /// - Single transaction (all-or-nothing commit)
    /// - Prepared statement reuse
    pub fn insert_transfers_batch(&self, transfers: &[TransferData]) -> Result<usize> {
        if transfers.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut transfer_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO transfers (token, from_addr, to_addr, amount, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
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
                    transfer.timestamp,
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let transfer_id = tx.last_insert_rowid();

                    // Insert wallet activity records
                    // This enables O(log n) wallet queries instead of O(n) OR scans
                    if transfer.from != Felt::ZERO
                        && transfer.to != Felt::ZERO
                        && transfer.from == transfer.to
                    {
                        // Self-transfer (one record with 'both' direction)
                        tx.execute(
                            "INSERT INTO wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                             VALUES (?, ?, ?, 'both', ?)",
                            params![&from_blob, &token_blob, transfer_id, transfer.block_number],
                        )?;
                    } else {
                        // Sender record
                        if transfer.from != Felt::ZERO {
                            tx.execute(
                                "INSERT INTO wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                                 VALUES (?, ?, ?, 'sent', ?)",
                                params![&from_blob, &token_blob, transfer_id, transfer.block_number],
                            )?;
                        }
                        // Receiver record
                        if transfer.to != Felt::ZERO {
                            tx.execute(
                                "INSERT INTO wallet_activity (wallet_address, token, transfer_id, direction, block_number)
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

    /// Insert multiple approvals in a single transaction
    pub fn insert_approvals_batch(&self, approvals: &[ApprovalData]) -> Result<usize> {
        if approvals.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut approval_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO approvals (token, owner, spender, amount, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;

            for approval in approvals {
                let token_blob = felt_to_blob(approval.token);
                let owner_blob = felt_to_blob(approval.owner);
                let spender_blob = felt_to_blob(approval.spender);
                let amount_blob = u256_to_blob(approval.amount);
                let tx_hash_blob = felt_to_blob(approval.tx_hash);

                // Insert approval
                let rows = approval_stmt.execute(params![
                    &token_blob,
                    &owner_blob,
                    &spender_blob,
                    &amount_blob,
                    approval.block_number,
                    &tx_hash_blob,
                    approval.timestamp,
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let approval_id = tx.last_insert_rowid();

                    // Insert approval activity records
                    if approval.owner != Felt::ZERO
                        && approval.spender != Felt::ZERO
                        && approval.owner == approval.spender
                    {
                        // Self-approval (one record with 'both' role)
                        tx.execute(
                            "INSERT INTO approval_activity (account_address, token, approval_id, role, block_number)
                             VALUES (?, ?, ?, 'both', ?)",
                            params![
                                &owner_blob,
                                &token_blob,
                                approval_id,
                                approval.block_number
                            ],
                        )?;
                    } else {
                        // Owner record
                        if approval.owner != Felt::ZERO {
                            tx.execute(
                                "INSERT INTO approval_activity (account_address, token, approval_id, role, block_number)
                                 VALUES (?, ?, ?, 'owner', ?)",
                                params![
                                    &owner_blob,
                                    &token_blob,
                                    approval_id,
                                    approval.block_number
                                ],
                            )?;
                        }
                        // Spender record
                        if approval.spender != Felt::ZERO {
                            tx.execute(
                                "INSERT INTO approval_activity (account_address, token, approval_id, role, block_number)
                                 VALUES (?, ?, ?, 'spender', ?)",
                                params![
                                    &spender_blob,
                                    &token_blob,
                                    approval_id,
                                    approval.block_number
                                ],
                            )?;
                        }
                    }
                }
            }
        }

        tx.commit()?;

        Ok(inserted)
    }

    /// Get filtered transfers with cursor-based pagination
    ///
    /// Supports:
    /// - `wallet`: Matches from OR to (uses wallet_activity table for efficient OR queries)
    /// - `from`: Exact from address match
    /// - `to`: Exact to address match
    /// - `tokens`: Token whitelist (empty = all tokens)
    /// - `direction`: Filter by sent/received/all (only applies with wallet)
    /// - `block_from`/`block_to`: Block range filter
    /// - `cursor`: Cursor from previous page
    /// - `limit`: Maximum results
    pub fn get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        tokens: &[Felt],
        direction: TransferDirection,
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<TransferData>, Option<TransferCursor>)> {
        let conn = self.conn.lock().unwrap();

        // Build dynamic query based on filters
        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            // Use wallet_activity table for efficient OR queries
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash, t.timestamp
                 FROM wallet_activity wa
                 JOIN transfers t ON wa.transfer_id = t.id
                 WHERE wa.wallet_address = ?",
            );
            params_vec.push(Box::new(felt_to_blob(wallet_addr)));

            // Apply direction filter
            match direction {
                TransferDirection::All => {}
                TransferDirection::Sent => {
                    query.push_str(" AND wa.direction IN ('sent', 'both')");
                }
                TransferDirection::Received => {
                    query.push_str(" AND wa.direction IN ('received', 'both')");
                }
            }

            // Token filter on wallet_activity for efficiency
            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND wa.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        } else {
            // Standard query without wallet optimization
            query.push_str(
                "SELECT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash, t.timestamp
                 FROM transfers t
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

        // Block range filters
        if let Some(block_min) = block_from {
            query.push_str(" AND t.block_number >= ?");
            params_vec.push(Box::new(block_min as i64));
        }

        if let Some(block_max) = block_to {
            query.push_str(" AND t.block_number <= ?");
            params_vec.push(Box::new(block_max as i64));
        }

        // Cursor-based pagination
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
            let from_bytes: Vec<u8> = row.get(2)?;
            let to_bytes: Vec<u8> = row.get(3)?;
            let amount_bytes: Vec<u8> = row.get(4)?;
            let block_number: i64 = row.get(5)?;
            let tx_hash_bytes: Vec<u8> = row.get(6)?;
            let timestamp: Option<i64> = row.get(7)?;

            Ok(TransferData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                from: blob_to_felt(&from_bytes),
                to: blob_to_felt(&to_bytes),
                amount: blob_to_u256(&amount_bytes),
                block_number: block_number as u64,
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp,
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

    /// Get filtered approvals with cursor-based pagination
    ///
    /// Supports:
    /// - `account`: Matches owner OR spender (uses approval_activity table)
    /// - `owner`: Exact owner address match
    /// - `spender`: Exact spender address match
    /// - `tokens`: Token whitelist (empty = all tokens)
    /// - `block_from`/`block_to`: Block range filter
    /// - `cursor`: Cursor from previous page
    /// - `limit`: Maximum results
    pub fn get_approvals_filtered(
        &self,
        account: Option<Felt>,
        owner: Option<Felt>,
        spender: Option<Felt>,
        tokens: &[Felt],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<ApprovalCursor>,
        limit: u32,
    ) -> Result<(Vec<ApprovalData>, Option<ApprovalCursor>)> {
        let conn = self.conn.lock().unwrap();

        // Build dynamic query based on filters
        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(account_addr) = account {
            // Use approval_activity table for efficient OR queries
            query.push_str(
                "SELECT DISTINCT a.id, a.token, a.owner, a.spender, a.amount, a.block_number, a.tx_hash, a.timestamp
                 FROM approval_activity aa
                 JOIN approvals a ON aa.approval_id = a.id
                 WHERE aa.account_address = ?",
            );
            params_vec.push(Box::new(felt_to_blob(account_addr)));

            // Token filter on approval_activity for efficiency
            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND aa.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        } else {
            // Standard query without account optimization
            query.push_str(
                "SELECT a.id, a.token, a.owner, a.spender, a.amount, a.block_number, a.tx_hash, a.timestamp
                 FROM approvals a
                 WHERE 1=1",
            );

            if let Some(owner_addr) = owner {
                query.push_str(" AND a.owner = ?");
                params_vec.push(Box::new(felt_to_blob(owner_addr)));
            }

            if let Some(spender_addr) = spender {
                query.push_str(" AND a.spender = ?");
                params_vec.push(Box::new(felt_to_blob(spender_addr)));
            }

            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND a.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        }

        // Block range filters
        if let Some(block_min) = block_from {
            query.push_str(" AND a.block_number >= ?");
            params_vec.push(Box::new(block_min as i64));
        }

        if let Some(block_max) = block_to {
            query.push_str(" AND a.block_number <= ?");
            params_vec.push(Box::new(block_max as i64));
        }

        // Cursor-based pagination
        if let Some(c) = cursor {
            query.push_str(" AND (a.block_number < ? OR (a.block_number = ? AND a.id < ?))");
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.id));
        }

        query.push_str(" ORDER BY a.block_number DESC, a.id DESC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let owner_bytes: Vec<u8> = row.get(2)?;
            let spender_bytes: Vec<u8> = row.get(3)?;
            let amount_bytes: Vec<u8> = row.get(4)?;
            let block_number: i64 = row.get(5)?;
            let tx_hash_bytes: Vec<u8> = row.get(6)?;
            let timestamp: Option<i64> = row.get(7)?;

            Ok(ApprovalData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                owner: blob_to_felt(&owner_bytes),
                spender: blob_to_felt(&spender_bytes),
                amount: blob_to_u256(&amount_bytes),
                block_number: block_number as u64,
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp,
            })
        })?;

        let approvals: Vec<ApprovalData> = rows.collect::<Result<_, _>>()?;

        // Compute next cursor if there might be more pages
        let next_cursor = if approvals.len() == limit as usize {
            approvals.last().map(|a| ApprovalCursor {
                block_number: a.block_number,
                id: a.id.unwrap(),
            })
        } else {
            None
        };

        Ok((approvals, next_cursor))
    }

    /// Get transfer count
    pub fn get_transfer_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM transfers", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get approval count
    pub fn get_approval_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM approvals", [], |row| row.get(0))?;
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
}
