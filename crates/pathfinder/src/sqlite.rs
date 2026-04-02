use rusqlite::params;
use rusqlite::Connection;
use rusqlite::Error as SqliteError;
use rusqlite::Row;
use starknet::core::types::Felt;

pub type SqliteResult<T> = Result<T, SqliteError>;

#[derive(Debug)]
pub struct BlockEventsRow {
    pub block_number: u64,
    pub events: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct NumberAndHashRow(pub u64, pub Vec<u8>);

#[derive(Debug)]
pub struct BlockContextRow {
    pub number: u64,
    pub hash: Vec<u8>,
    pub parent_hash: Vec<u8>,
    pub timestamp: u64,
}

impl From<NumberAndHashRow> for (u64, Felt) {
    fn from(value: NumberAndHashRow) -> Self {
        (value.0 as u64, Felt::from_bytes_be_slice(&value.1))
    }
}

impl BlockEventsRow {
    pub fn from_row(row: &Row<'_>) -> SqliteResult<Self> {
        Ok(Self {
            block_number: row.get::<_, i64>(0)? as u64,
            events: row.get(1)?,
        })
    }
}

impl NumberAndHashRow {
    pub fn from_row(row: &Row<'_>) -> SqliteResult<Self> {
        Ok(Self(row.get::<_, i64>(0)? as u64, row.get(1)?))
    }
}

impl BlockContextRow {
    pub fn from_row(row: &Row<'_>) -> SqliteResult<Self> {
        Ok(Self {
            number: row.get::<_, i64>(0)? as u64,
            hash: row.get(1)?,
            parent_hash: row.get(2)?,
            timestamp: row.get::<_, i64>(3)? as u64,
        })
    }
}

pub trait SqliteExt {
    fn get_block_events_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<BlockEventsRow>>;
    fn get_block_tx_hash_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<NumberAndHashRow>>;
    fn get_block_hash_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<NumberAndHashRow>>;
    fn get_block_context_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<BlockContextRow>>;
}

impl SqliteExt for Connection {
    fn get_block_events_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<BlockEventsRow>> {
        let mut stmt = self.prepare_cached("SELECT block_number, events FROM transactions WHERE block_number BETWEEN ? AND ? ORDER BY block_number")?;
        let rows = stmt.query_map(params![from_block, to_block], BlockEventsRow::from_row)?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    fn get_block_tx_hash_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<NumberAndHashRow>> {
        let mut stmt = self.prepare_cached("SELECT block_number, hash FROM transaction_hashes WHERE block_number BETWEEN ? AND ? ORDER BY block_number, idx")?;
        let rows = stmt.query_map(params![from_block, to_block], NumberAndHashRow::from_row)?;
        rows.collect::<Result<Vec<_>, _>>()
    }
    fn get_block_hash_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<NumberAndHashRow>> {
        let mut stmt = self.prepare_cached(
            "SELECT number, hash FROM block_headers WHERE number BETWEEN ? AND ? ORDER BY number",
        )?;
        let rows = stmt.query_map(params![from_block, to_block], NumberAndHashRow::from_row)?;
        rows.collect::<Result<Vec<_>, _>>()
    }
    fn get_block_context_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<BlockContextRow>> {
        let mut stmt = self.prepare_cached(
            "SELECT number, hash, parent_hash, timestamp FROM block_headers WHERE number BETWEEN ? AND ? ORDER BY number",
        )?;
        let rows = stmt.query_map(params![from_block, to_block], BlockContextRow::from_row)?;
        rows.collect::<Result<Vec<_>, _>>()
    }
}
