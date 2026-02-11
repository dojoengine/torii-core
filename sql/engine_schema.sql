-- Torii Engine Database Schema (Simplified for Demo)
-- Tracks engine state and basic statistics

-- Head tracking (current processing state)
CREATE TABLE IF NOT EXISTS head (
    id TEXT PRIMARY KEY NOT NULL,    -- 'main' for primary head
    block_number INTEGER NOT NULL,    -- Current block number
    event_count INTEGER NOT NULL DEFAULT 0  -- Total events processed
);

-- Engine statistics
CREATE TABLE IF NOT EXISTS stats (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

-- Extractor state tracking (for cursor persistence)
CREATE TABLE IF NOT EXISTS extractor_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    extractor_type TEXT NOT NULL,    -- 'block_range', 'contract_events', etc.
    state_key TEXT NOT NULL,         -- 'last_block', 'contract:0x123...', etc.
    state_value TEXT NOT NULL,       -- Cursor value (block number, continuation token, etc.)
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    UNIQUE(extractor_type, state_key)
);

-- Contract identification cache (contract address -> decoder IDs)
CREATE TABLE IF NOT EXISTS contract_decoders (
    contract_address TEXT PRIMARY KEY NOT NULL,  -- Hex string of contract address
    decoder_ids TEXT NOT NULL,                   -- Comma-separated list of decoder IDs (u64)
    identified_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

-- Block timestamps cache (for event-based extraction)
CREATE TABLE IF NOT EXISTS block_timestamps (
    block_number INTEGER PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    block_hash BLOB
);

-- Initialize default values
INSERT OR IGNORE INTO head (id, block_number, event_count) VALUES ('main', 0, 0);
INSERT OR IGNORE INTO stats (key, value) VALUES ('start_time', strftime('%s', 'now'));
