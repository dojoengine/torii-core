CREATE TABLE IF NOT EXISTS dojo_tables (
    owner BLOB NOT NULL,
    id BLOB NOT NULL,
    name TEXT NOT NULL,
    attributes TEXT NOT NULL,
    keys_json TEXT NOT NULL,
    values_json TEXT NOT NULL,
    legacy INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    created_block INTEGER NOT NULL,
    updated_block INTEGER NOT NULL,
    created_tx BLOB NOT NULL,
    updated_tx BLOB NOT NULL,
    PRIMARY KEY (owner, id)
);

CREATE TABLE IF NOT EXISTS dojo_columns (
    owner BLOB NOT NULL,
    table_id BLOB NOT NULL,
    id BLOB NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (owner, table_id, id)
);
