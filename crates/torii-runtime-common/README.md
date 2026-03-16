# torii-runtime-common

Shared runtime setup helpers for Torii binaries.

## Database helpers

- `backend_from_url_or_path(...)` to classify Postgres vs SQLite.
- `resolve_single_db_setup(db_path, database_url)` for single-storage binaries.
- `resolve_token_db_setup(db_dir, engine_database_url, storage_database_url)` for multi-storage token binaries.

## Sink helpers

- `initialize_sink(sink, database_root)`
  - Standard sink initialization with EventBus and SinkContext.
- `drop_postgres_schemas(database_url, schemas, logging_target)`
  - Shared schema reset utility used by synthetic profilers.

## Example

```rust
use torii_runtime_common::database::resolve_token_db_setup;

let db_setup = resolve_token_db_setup(
    std::path::Path::new("./torii-data"),
    config.database_url.as_deref(),
    config.storage_database_url.as_deref(),
)?;
```
