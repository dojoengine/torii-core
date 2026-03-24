CREATE TABLE IF NOT EXISTS introspect_sink_schema_state (
    "schema" TEXT NOT NULL,
    table_id TEXT NOT NULL,
    table_schema_json TEXT NOT NULL,
    alive INTEGER NOT NULL DEFAULT 1,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY ("schema", table_id)
);
