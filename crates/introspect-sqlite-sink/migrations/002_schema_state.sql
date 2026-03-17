CREATE TABLE IF NOT EXISTS torii_introspect_schema_state (
    table_id BLOB PRIMARY KEY,
    table_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);
