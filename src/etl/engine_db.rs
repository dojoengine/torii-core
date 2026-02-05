//! Simplified Engine database for demo.
//!
//! Tracks basic state and statistics for the Torii engine.
//! This will be enhanced with actual Torii features in the future.

use anyhow::{Context, Result};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Pool, Row, Sqlite,
};
use starknet::core::types::Felt;
use std::path::Path;
use std::str::FromStr;

use crate::etl::decoder::DecoderId;

/// Embedded SQL schema
const SCHEMA_SQL: &str = include_str!("../../sql/engine_schema.sql");

/// Engine database configuration
#[derive(Debug, Clone)]
pub struct EngineDbConfig {
    pub path: String,
}

/// Engine database for tracking state
pub struct EngineDb {
    pool: Pool<Sqlite>,
}

impl EngineDb {
    /// Create a new engine database
    pub async fn new(config: EngineDbConfig) -> Result<Self> {
        // Handle special case for in-memory database
        let is_memory = config.path == ":memory:" || config.path == "sqlite::memory:";

        // Ensure parent directory exists (skip for in-memory)
        if !is_memory {
            if let Some(parent) = Path::new(&config.path).parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context(format!("Failed to create directory: {}", parent.display()))?;
            }
        }

        tracing::debug!(target: "torii::etl::engine_db", "Connecting to database: {}", config.path);

        // Create SQLite connection options
        let opts = SqliteConnectOptions::from_str(&config.path)?.create_if_missing(true);

        // Create connection pool
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .context("Failed to connect to engine database")?;

        let db = Self { pool };

        // Initialize schema
        db.init_schema().await?;

        Ok(db)
    }

    /// Initialize database with PRAGMAs and schema
    async fn init_schema(&self) -> Result<()> {
        // Apply PRAGMAs for performance
        self.apply_pragmas().await?;

        // Load tables from SQL file
        self.load_schema_from_sql().await?;

        tracing::info!(target: "torii::etl::engine_db", "Engine database schema initialized");

        Ok(())
    }

    /// Apply SQLite PRAGMAs for performance
    async fn apply_pragmas(&self) -> Result<()> {
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&self.pool)
            .await?;

        sqlx::query("PRAGMA synchronous=NORMAL")
            .execute(&self.pool)
            .await?;

        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&self.pool)
            .await?;

        tracing::debug!(target: "torii::etl::engine_db", "Applied SQLite PRAGMAs");

        Ok(())
    }

    /// Load schema from SQL file
    async fn load_schema_from_sql(&self) -> Result<()> {
        for statement in SCHEMA_SQL.split(';') {
            let statement = statement.trim();

            // Skip empty statements
            if statement.is_empty() {
                continue;
            }

            // Remove comment lines
            let sql_lines: Vec<&str> = statement
                .lines()
                .filter(|line| {
                    let trimmed = line.trim();
                    !trimmed.is_empty() && !trimmed.starts_with("--")
                })
                .collect();

            if sql_lines.is_empty() {
                continue;
            }

            let clean_sql = sql_lines.join("\n");

            tracing::debug!(
                target: "torii::etl::engine_db",
                "Executing SQL: {}",
                clean_sql.lines().next().unwrap_or("")
            );

            sqlx::query(&clean_sql)
                .execute(&self.pool)
                .await
                .context(format!(
                    "Failed to execute SQL: {}",
                    clean_sql.lines().next().unwrap_or("")
                ))?;
        }

        tracing::debug!(target: "torii::etl::engine_db", "Schema loaded successfully");
        Ok(())
    }

    /// Get the current head (block number and event count)
    pub async fn get_head(&self) -> Result<(u64, u64)> {
        let row = sqlx::query("SELECT block_number, event_count FROM head WHERE id = 'main'")
            .fetch_one(&self.pool)
            .await?;

        let block_number: i64 = row.get(0);
        let event_count: i64 = row.get(1);

        Ok((block_number as u64, event_count as u64))
    }

    /// Update the head (increment block and event count)
    pub async fn update_head(&self, block_number: u64, events_processed: u64) -> Result<()> {
        sqlx::query(
            "UPDATE head SET block_number = ?, event_count = event_count + ? WHERE id = 'main'",
        )
        .bind(block_number as i64)
        .bind(events_processed as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get a stat value
    pub async fn get_stat(&self, key: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT value FROM stats WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| r.get(0)))
    }

    /// Set a stat value
    pub async fn set_stat(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO stats (key, value) VALUES (?, ?)")
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Get engine statistics as a JSON-friendly struct
    pub async fn get_stats(&self) -> Result<EngineStats> {
        let (block_number, event_count) = self.get_head().await?;
        let start_time = self.get_stat("start_time").await?.unwrap_or_default();

        Ok(EngineStats {
            current_block: block_number,
            total_events: event_count,
            start_time,
        })
    }

    /// Get extractor state value
    ///
    /// # Arguments
    /// * `extractor_type` - Type of extractor (e.g., "block_range", "contract_events")
    /// * `state_key` - State key (e.g., "last_block", "contract:0x123...")
    ///
    /// # Returns
    /// The state value if it exists, None otherwise
    pub async fn get_extractor_state(
        &self,
        extractor_type: &str,
        state_key: &str,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT state_value FROM extractor_state WHERE extractor_type = ? AND state_key = ?",
        )
        .bind(extractor_type)
        .bind(state_key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.get(0)))
    }

    /// Set extractor state value
    ///
    /// # Arguments
    /// * `extractor_type` - Type of extractor (e.g., "block_range", "contract_events")
    /// * `state_key` - State key (e.g., "last_block", "contract:0x123...")
    /// * `state_value` - State value to store
    pub async fn set_extractor_state(
        &self,
        extractor_type: &str,
        state_key: &str,
        state_value: &str,
    ) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO extractor_state (extractor_type, state_key, state_value, updated_at)
            VALUES (?, ?, ?, strftime('%s', 'now'))
            ON CONFLICT(extractor_type, state_key)
            DO UPDATE SET state_value = excluded.state_value, updated_at = strftime('%s', 'now')
            ",
        )
        .bind(extractor_type)
        .bind(state_key)
        .bind(state_value)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Delete extractor state
    ///
    /// # Arguments
    /// * `extractor_type` - Type of extractor
    /// * `state_key` - State key to delete
    pub async fn delete_extractor_state(
        &self,
        extractor_type: &str,
        state_key: &str,
    ) -> Result<()> {
        sqlx::query("DELETE FROM extractor_state WHERE extractor_type = ? AND state_key = ?")
            .bind(extractor_type)
            .bind(state_key)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Get block timestamps from cache
    ///
    /// # Arguments
    /// * `block_numbers` - Block numbers to look up
    ///
    /// # Returns
    /// HashMap of block_number -> timestamp for blocks found in cache
    pub async fn get_block_timestamps(
        &self,
        block_numbers: &[u64],
    ) -> Result<std::collections::HashMap<u64, u64>> {
        if block_numbers.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        // Build query with IN clause
        let placeholders: Vec<String> = block_numbers.iter().map(|_| "?".to_string()).collect();
        let query = format!(
            "SELECT block_number, timestamp FROM block_timestamps WHERE block_number IN ({})",
            placeholders.join(", ")
        );

        let mut query_builder = sqlx::query(&query);
        for block_num in block_numbers {
            query_builder = query_builder.bind(*block_num as i64);
        }

        let rows = query_builder.fetch_all(&self.pool).await?;

        let mut result = std::collections::HashMap::new();
        for row in rows {
            let block_number: i64 = row.get(0);
            let timestamp: i64 = row.get(1);
            result.insert(block_number as u64, timestamp as u64);
        }

        Ok(result)
    }

    /// Insert block timestamps into cache
    ///
    /// # Arguments
    /// * `timestamps` - HashMap of block_number -> timestamp
    pub async fn insert_block_timestamps(
        &self,
        timestamps: &std::collections::HashMap<u64, u64>,
    ) -> Result<()> {
        if timestamps.is_empty() {
            return Ok(());
        }

        // Use INSERT OR IGNORE to avoid conflicts
        for (block_number, timestamp) in timestamps {
            sqlx::query(
                "INSERT OR IGNORE INTO block_timestamps (block_number, timestamp) VALUES (?, ?)",
            )
            .bind(*block_number as i64)
            .bind(*timestamp as i64)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    /// Get a single block timestamp from cache
    ///
    /// # Arguments
    /// * `block_number` - Block number to look up
    ///
    /// # Returns
    /// Timestamp if found in cache, None otherwise
    pub async fn get_block_timestamp(&self, block_number: u64) -> Result<Option<u64>> {
        let row = sqlx::query("SELECT timestamp FROM block_timestamps WHERE block_number = ?")
            .bind(block_number as i64)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| {
            let ts: i64 = r.get(0);
            ts as u64
        }))
    }

    // ===== Contract Decoder Persistence =====

    /// Get all contract decoder mappings from database.
    ///
    /// # Returns
    /// Vector of (contract_address, decoder_ids, identified_at_timestamp)
    pub async fn get_all_contract_decoders(&self) -> Result<Vec<(Felt, Vec<DecoderId>, i64)>> {
        let rows = sqlx::query(
            "SELECT contract_address, decoder_ids, identified_at FROM contract_decoders",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut results = Vec::new();
        for row in rows {
            let addr_hex: String = row.get(0);
            let decoder_ids_str: String = row.get(1);
            let identified_at: i64 = row.get(2);

            // Parse contract address
            let contract_address = Felt::from_hex(&addr_hex)
                .context(format!("Invalid contract address: {addr_hex}"))?;

            // Parse decoder IDs (comma-separated u64 values)
            let decoder_ids: Vec<DecoderId> = if decoder_ids_str.is_empty() {
                Vec::new()
            } else {
                decoder_ids_str
                    .split(',')
                    .filter_map(|s| s.trim().parse::<u64>().ok())
                    .map(DecoderId::from_u64)
                    .collect()
            };

            results.push((contract_address, decoder_ids, identified_at));
        }

        Ok(results)
    }

    /// Set decoder IDs for a contract.
    ///
    /// # Arguments
    /// * `contract` - Contract address
    /// * `decoder_ids` - List of decoder IDs (can be empty)
    pub async fn set_contract_decoders(
        &self,
        contract: Felt,
        decoder_ids: &[DecoderId],
    ) -> Result<()> {
        let addr_hex = format!("{contract:#x}");
        let decoder_ids_str: String = decoder_ids
            .iter()
            .map(|id| id.as_u64().to_string())
            .collect::<Vec<_>>()
            .join(",");

        sqlx::query(
            r"
            INSERT INTO contract_decoders (contract_address, decoder_ids, identified_at)
            VALUES (?, ?, strftime('%s', 'now'))
            ON CONFLICT(contract_address)
            DO UPDATE SET decoder_ids = excluded.decoder_ids, identified_at = strftime('%s', 'now')
            ",
        )
        .bind(&addr_hex)
        .bind(&decoder_ids_str)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get decoder IDs for a specific contract.
    ///
    /// # Arguments
    /// * `contract` - Contract address
    ///
    /// # Returns
    /// Some(decoder_ids) if found, None otherwise
    pub async fn get_contract_decoders(&self, contract: Felt) -> Result<Option<Vec<DecoderId>>> {
        let addr_hex = format!("{contract:#x}");

        let row =
            sqlx::query("SELECT decoder_ids FROM contract_decoders WHERE contract_address = ?")
                .bind(&addr_hex)
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some(r) => {
                let decoder_ids_str: String = r.get(0);
                let decoder_ids: Vec<DecoderId> = if decoder_ids_str.is_empty() {
                    Vec::new()
                } else {
                    decoder_ids_str
                        .split(',')
                        .filter_map(|s| s.trim().parse::<u64>().ok())
                        .map(DecoderId::from_u64)
                        .collect()
                };
                Ok(Some(decoder_ids))
            }
            None => Ok(None),
        }
    }
}

/// Engine statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct EngineStats {
    pub current_block: u64,
    pub total_events: u64,
    pub start_time: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_db_initialization() {
        let config = EngineDbConfig {
            path: ":memory:".to_string(),
        };

        let db = EngineDb::new(config).await.unwrap();
        let (block, events) = db.get_head().await.unwrap();

        assert_eq!(block, 0);
        assert_eq!(events, 0);
    }

    #[tokio::test]
    async fn test_update_head() {
        let config = EngineDbConfig {
            path: ":memory:".to_string(),
        };

        let db = EngineDb::new(config).await.unwrap();

        // Update head
        db.update_head(100, 50).await.unwrap();

        let (block, events) = db.get_head().await.unwrap();
        assert_eq!(block, 100);
        assert_eq!(events, 50);

        // Update again (events should accumulate)
        db.update_head(200, 30).await.unwrap();

        let (block, events) = db.get_head().await.unwrap();
        assert_eq!(block, 200);
        assert_eq!(events, 80); // 50 + 30
    }

    #[tokio::test]
    async fn test_stats() {
        let config = EngineDbConfig {
            path: ":memory:".to_string(),
        };

        let db = EngineDb::new(config).await.unwrap();

        // Set custom stat
        db.set_stat("last_sync", "2026-01-08").await.unwrap();

        // Get stat
        let value = db.get_stat("last_sync").await.unwrap();
        assert_eq!(value, Some("2026-01-08".to_string()));

        // Get missing stat
        let missing = db.get_stat("nonexistent").await.unwrap();
        assert_eq!(missing, None);
    }
}
