CREATE SCHEMA IF NOT EXISTS introspect;

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

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE t.typname = 'primary_def' AND n.nspname = 'introspect'
    ) THEN
        CREATE TYPE introspect.primary_def AS (
            name TEXT,
            attributes TEXT[] NOT NULL DEFAULT '{}',
            type_def jsonb NOT NULL
        );
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS introspect.table (
    PRIMARY KEY (owner, id),
    owner felt252,
    id felt252 NOT NULL,
    name TEXT NOT NULL,
    attributes TEXT[] NOT NULL DEFAULT '{}',
    primary introspect.primary_def NOT NULL,
    columns felt252[] NOT NULL,
    alive BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS introspect.columns(
    PRIMARY KEY (owner, "table", id),
    owner felt252,
    "table" felt252 NOT NULL,
    id felt252 NOT NULL,
    name TEXT NOT NULL,
    attributes introspect.attribute[] NOT NULL DEFAULT '{}',
    type_def jsonb NOT NULL
);