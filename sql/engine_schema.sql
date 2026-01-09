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

-- Initialize default values
INSERT OR IGNORE INTO head (id, block_number, event_count) VALUES ('main', 0, 0);
INSERT OR IGNORE INTO stats (key, value) VALUES ('start_time', strftime('%s', 'now'));
