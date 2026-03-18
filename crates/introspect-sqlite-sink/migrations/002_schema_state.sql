CREATE TABLE IF NOT EXISTS introspect_sink_schema_state (
    table_id TEXT PRIMARY KEY,
    table_schema_json TEXT NOT NULL,
    alive INTEGER NOT NULL DEFAULT 1,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);
