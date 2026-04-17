# torii-sql

The database abstraction layer. Wraps `sqlx` to give every Torii sink a
unified API over **SQLite** and **PostgreSQL** (MySQL has hooks but is
unused), plus a set of helpers for building pools, running migrations, and
composing parametric queries across backends.

## Role in Torii

Every sink that persists state uses this crate: `torii-sql-sink`,
`torii-controllers-sink`, `torii-ecs-sink`, `introspect-sql-sink`, and the
three token sinks. `EngineDb` in the root crate does *not* use this layer
(it is pinned to raw `sqlx::Any`) — everything else does. The
`DbBackend` enum defined here is the same one `torii-runtime-common` returns
from `backend_from_url_or_path`, so a single value flows from CLI → runtime
resolver → pool → migrations → queries.

## Architecture

```text
+-------------------------------------------------------------------+
|                           torii-sql                               |
|                                                                   |
|  connection.rs                                                    |
|  +-------------------+   DbBackend::{Sqlite,Postgres}             |
|  | DbBackend         |   DbOption<T> (backend-tagged pair)        |
|  | POSTGRES_URL_...  |                                            |
|  | SQLITE_URL_...    |                                            |
|  +-------------------+                                            |
|                                                                   |
|  pool.rs                                                          |
|  +---------------------+   PoolExt / PoolConfig / DbPoolOptions   |
|  | Connection pools    |   (pool tuning helpers)                  |
|  +---------------------+                                          |
|                                                                   |
|  migrate.rs                                                       |
|  +---------------------+   SchemaMigrator (versioned, backend-    |
|  | Schema migrations   |   aware), AcquiredSchema (lock guard)    |
|  +---------------------+                                          |
|                                                                   |
|  query.rs                                                         |
|  +---------------------+   Executable trait + FlexQuery + Queries |
|  | Query composition   |   placeholder-normalisation for ?/$n     |
|  +---------------------+                                          |
|                                                                   |
|  postgres/ (feature: postgres)    sqlite/ (feature: sqlite)       |
|  +------------------+             +------------------+            |
|  | Postgres + Pg*   |             | Sqlite + Sqlite* |            |
|  +------------------+             +------------------+            |
|                                                                   |
|  runtime.rs  (feature: postgres + sqlite)                         |
|  +----------------------+                                         |
|  | DbPool / DbConnection|   runtime-picked backend                |
|  | Options              |                                         |
|  +----------------------+                                         |
+-------------------------------------------------------------------+
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `DbBackend` | `src/connection.rs` | 4 | `Postgres` / `Sqlite`; `FromStr` accepts `postgres://`, `postgresql://`, `sqlite*`, `:memory:` |
| `DbOption<T>` | `src/connection.rs` | 10 | Backend-indexed pair (stores two values, picks one at runtime) |
| `POSTGRES_URL_SCHEMES` / `SQLITE_URL_SCHEMES` | `src/connection.rs` | 15 / 16 | URL prefix constants |
| `PoolExt` / `PoolConfig` / `DbPoolOptions` | `src/pool.rs` | — | Pool tuning + trait-object friendly pool abstraction |
| `SchemaMigrator` / `AcquiredSchema` | `src/migrate.rs` | — | Versioned schema migrations with cross-process locks |
| `Executable` | `src/query.rs` | — | Execute + fetch over any backend |
| `FlexQuery` / `Queries` | `src/query.rs` | — | Normalises `?` / `$n` placeholders and bindings |
| `Postgres`, `PgPool`, `PgQuery`, `PgArguments`, `PgDbConnection` | `src/postgres/mod.rs` | — | feature `postgres` |
| `Sqlite`, `SqlitePool`, `SqliteQuery`, `SqliteArguments`, `SqliteDbConnection` | `src/sqlite/mod.rs` | — | feature `sqlite` |
| `DbPool` / `DbConnectionOptions` | `src/runtime.rs` | — | feature `postgres + sqlite`; runtime enum wrapping either side |
| `SqlxResult<T>` alias | `src/lib.rs` | 13 | `Result<T, sqlx::Error>` |

### Internal Modules

- `connection` — `DbBackend` + URL classification + `DbOption<T>` pair.
- `pool` — pool configuration helpers (size, timeouts) and the `PoolExt` blanket-impl.
- `migrate` — versioned schema migrator backed by a per-backend lock.
- `query` — `FlexQuery` and friends; bridges SQLite `?` vs. Postgres `$n` placeholder conventions.
- `postgres/` — sqlx-postgres wrappers + Postgres-specific migrate helpers + type adapters.
- `sqlite/` — sqlx-sqlite wrappers + SQLite migrate helpers.
- `runtime` — `DbPool::Postgres(pool) | DbPool::Sqlite(pool)` runtime enum (only compiled when both features are on).
- `types` — shared numeric/date conversions.

### Interactions

- **Upstream (consumers)**: `torii-runtime-common` (reads `DbBackend`), every SQL-backed sink, `introspect-sql-sink` (both PG + SQLite via its own features).
- **Downstream deps**: `sqlx` (patched fork from `bengineer42/sqlx`, see workspace `Cargo.toml` line 81), `async-trait`, `futures`.
- **Features**: `postgres`, `sqlite`, `mysql` — each pulls the corresponding sqlx driver. The `runtime` module and `DbPool` only exist when `postgres + sqlite` are both enabled.

### Extension Points

- New backend: add a feature flag + a sibling module mirroring `postgres/` and `sqlite/`, expose a `Pool`, a `Query`, and a `DbConnection`, and extend `DbBackend`.
- New cross-backend query helper: put it in `query.rs`. Keep backend-specific quirks (JSON operators, upsert syntax) behind `DbOption<&'static str>` lookups.
- New migration surface: add a `SchemaMigrator` subscriber; do not add a second migrator, the backend-aware one is authoritative.
