-- Torii Engine Database Schema for PostgreSQL

CREATE SCHEMA IF NOT EXISTS engine;

CREATE TABLE IF NOT EXISTS engine.head (
    id TEXT PRIMARY KEY,
    block_number BIGINT NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS engine.stats (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS engine.extractor_state (
    id BIGSERIAL PRIMARY KEY,
    extractor_type TEXT NOT NULL,
    state_key TEXT NOT NULL,
    state_value TEXT NOT NULL,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT),
    UNIQUE(extractor_type, state_key)
);

CREATE TABLE IF NOT EXISTS engine.contract_decoders (
    contract_address TEXT PRIMARY KEY,
    decoder_ids TEXT NOT NULL,
    identified_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT)
);

CREATE TABLE IF NOT EXISTS engine.block_timestamps (
    block_number BIGINT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    block_hash BYTEA
);

INSERT INTO engine.head (id, block_number, event_count)
VALUES ('main', 0, 0)
ON CONFLICT (id) DO NOTHING;

INSERT INTO engine.stats (key, value)
VALUES ('start_time', (EXTRACT(EPOCH FROM NOW())::BIGINT)::TEXT)
ON CONFLICT (key) DO NOTHING;
