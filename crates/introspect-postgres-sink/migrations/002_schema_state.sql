CREATE TABLE IF NOT EXISTS introspect.introspect_sink_schema_state (
    table_id TEXT PRIMARY KEY,
    table_schema_json TEXT NOT NULL,
    alive BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
