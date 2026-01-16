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
    pub token: Felt,
    pub from: Felt,
    pub to: Felt,
    /// Amount as U256 (256-bit) for proper ERC20 token amounts
    pub amount: U256,
    pub block_number: u64,
    pub tx_hash: Felt,
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

/// Convert U256 to 32-byte BLOB for storage (big-endian for easier inspection)
fn u256_to_blob(value: U256) -> Vec<u8> {
    let mut bytes = vec![0u8; 32];
    // Big-endian: high 128 bits first, then low 128 bits
    bytes[..16].copy_from_slice(&value.high().to_be_bytes());
    bytes[16..].copy_from_slice(&value.low().to_be_bytes());
    bytes
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

        // Metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
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

        // Compute new balance
        let new_balance = if is_credit {
            current + amount
        } else {
            // Handle potential underflow gracefully (e.g., when starting from block > 0)
            if current >= amount {
                current - amount
            } else {
                // Underflow: clamp to zero and log warning
                tracing::warn!(
                    target: "torii_erc20::storage",
                    "Balance underflow: current={:#x}, amount={:#x}, clamping to 0",
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
}
