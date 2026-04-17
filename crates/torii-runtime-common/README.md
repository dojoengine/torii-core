# torii-runtime-common

Shared **runtime** helpers used by every Torii binary. Where
`torii-config-common` validates CLI input, this crate resolves it into
concrete database URLs, initialises sinks, and handles destructive tasks
like dropping Postgres schemas for synthetic profilers.

## Role in Torii

Each binary produces a `clap`-parsed `Config`, then calls helpers from this
crate to produce a `SingleDbSetup` / `TokenDbSetup`, wire up a throwaway
`EventBus` + `CommandBus` during `Sink::initialize`, and optionally reset
Postgres schemas between profiling runs. The resolved URLs are then fed to
`ToriiConfigBuilder::engine_database_url` and the sink-specific constructors.

## Architecture

```text
CLI parsed Config (clap)
           |
           v
+---------------------------------+
|   database.rs                   |
|   - backend_from_url_or_path    |-- DbBackend::{Sqlite,Postgres}
|   - resolve_single_db_setup     |-- SingleDbSetup
|   - resolve_token_db_setup      |-- TokenDbSetup (engine+erc20+erc721+erc1155)
|   - validate_uniform_backends   |
+---------------------------------+
           |
           v
+---------------------------------+
|   sink.rs                       |
|   - initialize_sink             |-- EventBus + SinkContext
|   - initialize_sink_with_       |-- + CommandBus with user handlers
|       command_handlers          |
|   - drop_postgres_schemas       |-- schema reset for -synth bins
+---------------------------------+
           |
           v
+---------------------------------+
|   token_support.rs              |
|   - InstalledTokenSupport       |-- which token sinks to install
|   - resolve_installed_token_... |
+---------------------------------+
           |
           v
   torii::ToriiConfigBuilder::build() → torii::run(...)
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `backend_from_url_or_path` | `database.rs` | 7 | Classify URL as `DbBackend::Postgres` or `DbBackend::Sqlite` |
| `validate_uniform_backends` | `database.rs` | 15 | Reject mixed Postgres/SQLite targets in one binary |
| `SingleDbSetup` | `database.rs` | 40 | `{ storage_url, engine_url, database_root }` |
| `resolve_single_db_setup` | `database.rs` | 46 | Derive engine path next to storage DB when not explicit |
| `TokenDbSetup` | `database.rs` | 71 | Engine + erc20 + erc721 + erc1155 URLs + each backend |
| `resolve_token_db_setup` | `database.rs` | 82 | Multi-DB resolver; rejects Postgres-engine + SQLite-storage mixes |
| `DEFAULT_SQLITE_MAX_CONNECTIONS` | `database.rs` | 5 | 500 — used by token sinks for connection pools |
| `initialize_sink` | `sink.rs` | 18 | `EventBus` + default `SinkContext`, drops its command bus immediately |
| `initialize_sink_with_command_handlers` | `sink.rs` | 24 | Keeps the `CommandBus` alive; returns `InitializedSinkRuntime` for shutdown |
| `drop_postgres_schemas` | `sink.rs` | 46 | `DROP SCHEMA ... CASCADE` helper for synthetic profilers |
| `InstalledTokenSupport` | `token_support.rs` | 1 | `{erc20, erc721, erc1155}` flags plus `any()` |
| `resolve_installed_token_support` | `token_support.rs` | 14 | Compose explicit targets with `--include-external-contracts` |

### Internal Modules

- `database` — URL classification + multi-target DB resolution with cross-backend validation.
- `sink` — builds a short-lived `EventBus`/`CommandBus` for `Sink::initialize` so binaries don’t duplicate that plumbing.
- `token_support` — little helper deciding which of the three token sinks the binary installs.

### Interactions

- **Upstream (consumers)**: `bins/torii-arcade`, `bins/torii-introspect-bin`, `bins/torii-tokens`, the three `-synth` profilers, `crates/torii-controllers-sink`, `crates/torii-ecs-sink` (all need `initialize_sink` or DB-setup helpers).
- **Downstream deps**: `torii`, `torii-sql` (for `DbBackend`), `tokio-postgres` (schema reset), `anyhow`, `tracing`.
- **Workspace deps**: `torii`, `torii-sql`.

### Extension Points

- New shared DB layout → add a `resolve_*_db_setup` returning a typed struct. Keep `validate_uniform_backends` as the one place that enforces "don’t mix backends".
- New global runtime chore (e.g., temp-dir prep, seccomp hook) belongs here, not in individual binaries.
