CREATE SCHEMA IF NOT EXISTS introspect;
CREATE SCHEMA IF NOT EXISTS dojo;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'felt252') THEN
        CREATE DOMAIN felt252 AS bytea CHECK (octet_length(VALUE) = 32);
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE t.typname = 'attribute' AND n.nspname = 'introspect'
    ) THEN
        CREATE TYPE introspect.attribute AS (
            name TEXT,
            data bytea
        );
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS dojo.table (
    id felt252 PRIMARY KEY,
    name TEXT NOT NULL,
    attributes TEXT[] NOT NULL DEFAULT '{}',
    keys felt252[] NOT NULL,
    "values" felt252[] NOT NULL,
    legacy BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dojo.columns(
    PRIMARY KEY (table_id, column_id),
    table_id felt252 NOT NULL,
    column_id felt252 NOT NULL,
    name TEXT NOT NULL,
    attributes introspect.attribute[] NOT NULL DEFAULT '{}',
    type_def jsonb NOT NULL
);