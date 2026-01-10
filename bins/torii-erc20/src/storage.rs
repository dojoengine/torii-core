//! SQLite storage for ERC20 transfers and balances

use anyhow::Result;
use rusqlite::{params, Connection};
use starknet::core::types::Felt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Storage for ERC20 transfers and balances
pub struct Erc20Storage {
    conn: Arc<Mutex<Connection>>,
}

impl Erc20Storage {
    /// Create or open the database
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        // Create tables
        conn.execute(
            "CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL,
                from_addr TEXT NOT NULL,
                to_addr TEXT NOT NULL,
                amount TEXT NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash TEXT NOT NULL,
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
                token TEXT NOT NULL,
                address TEXT NOT NULL,
                balance TEXT NOT NULL,
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

    /// Insert a transfer and update balances
    pub fn insert_transfer(
        &self,
        token: Felt,
        from: Felt,
        to: Felt,
        amount: Felt,
        block_number: u64,
        tx_hash: Felt,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        // Insert transfer (or ignore if duplicate)
        conn.execute(
            "INSERT OR IGNORE INTO transfers (token, from_addr, to_addr, amount, block_number, tx_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                format!("{:#x}", token),
                format!("{:#x}", from),
                format!("{:#x}", to),
                format!("{:#x}", amount),
                block_number,
                format!("{:#x}", tx_hash),
            ],
        )?;

        // Update sender balance (decrease)
        if from != Felt::ZERO {
            self.update_balance_internal(&conn, token, from, amount, false)?;
        }

        // Update receiver balance (increase)
        if to != Felt::ZERO {
            self.update_balance_internal(&conn, token, to, amount, true)?;
        }

        Ok(())
    }

    /// Update balance for an address
    fn update_balance_internal(
        &self,
        conn: &Connection,
        token: Felt,
        address: Felt,
        amount: Felt,
        is_increase: bool,
    ) -> Result<()> {
        let token_str = format!("{:#x}", token);
        let address_str = format!("{:#x}", address);

        // Get current balance
        let current_balance: Option<String> = conn
            .query_row(
                "SELECT balance FROM balances WHERE token = ?1 AND address = ?2",
                params![token_str, address_str],
                |row| row.get(0),
            )
            .ok();

        // Parse current balance (simple string-based arithmetic for now)
        // TODO: Proper big integer handling
        let _current = current_balance.unwrap_or_else(|| "0x0".to_string());

        // For now, just store the raw Felt string
        // In production, you'd want proper u256 arithmetic
        let new_balance_str = if is_increase {
            format!("{:#x}", amount) // Simplified: just store the amount
        } else {
            format!("{:#x}", amount)
        };

        // Upsert balance
        conn.execute(
            "INSERT INTO balances (token, address, balance, updated_at)
             VALUES (?1, ?2, ?3, strftime('%s', 'now'))
             ON CONFLICT(token, address) DO UPDATE SET
               balance = ?3,
               updated_at = strftime('%s', 'now')",
            params![token_str, address_str, new_balance_str],
        )?;

        Ok(())
    }

    /// Get balance for an address
    #[allow(dead_code)]
    pub fn get_balance(&self, token: Felt, address: Felt) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();

        let balance = conn
            .query_row(
                "SELECT balance FROM balances WHERE token = ?1 AND address = ?2",
                params![format!("{:#x}", token), format!("{:#x}", address)],
                |row| row.get(0),
            )
            .ok();

        Ok(balance)
    }

    /// Get all balances for a token
    #[allow(dead_code)]
    pub fn get_token_balances(&self, token: Felt) -> Result<HashMap<String, String>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT address, balance FROM balances WHERE token = ?1 ORDER BY balance DESC",
        )?;

        let balances = stmt
            .query_map(params![format!("{:#x}", token)], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
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
