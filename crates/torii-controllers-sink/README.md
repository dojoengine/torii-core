# torii-controllers-sink

Syncs **Cartridge controller accounts** (username + address + deployment
timestamp) from the Cartridge GraphQL API into a local SQL table. Unlike
most sinks, this one is **API-driven** — it does not consume `Envelope`s;
it watches the block timestamps moving in the `ExtractionBatch` and uses
that as a tick to fetch fresh controllers from
`https://api.cartridge.gg/query`.

## Role in Torii

Binaries that serve the Cartridge ecosystem (`bins/torii-arcade`,
`bins/torii-introspect-bin`) plug this sink in alongside the token / dojo
sinks. It provides the `controllers` table other sinks JOIN against to
enrich queries with a human-readable `username` for a given on-chain
address.

## Architecture

```text
                +------------------------------+
                |     ControllersSink          |
                |                              |
                |  process(batch):             |
                |    time_window := min/max    |
                |      block.timestamp         |
                |    if window > synced_until: |
                |      fetch_controllers(GQL)  |---> https://api.cartridge.gg/query
                |      upsert batch(store)     |
                +------------------------------+
                              |
                              v
                +------------------------------+
                |      ControllersStore        |
                |  Arc<sqlx::Pool<Any>>        |
                |  backend = Sqlite | Postgres |
                |                              |
                |  controllers (id PK, addr,   |
                |      username, deployed_at,  |
                |      updated_at)             |
                |  torii_controller_sync_state |
                |      (key='synced_until',    |
                |       value=<i64 timestamp>) |
                +------------------------------+
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `ControllersSink` | `src/lib.rs` | 260 | Sink — wraps `ControllersStore`, reqwest client, GraphQL URL |
| `ControllersSink::new(db_url, max_conns, api_url)` | `src/lib.rs` | 267 | Opens the store, initialises the HTTP client |
| `CONTROLLERS_TABLE`, `CONTROLLERS_STATE_TABLE` | `src/lib.rs` | 21 / 22 | Table-name constants |
| `DEFAULT_API_QUERY_URL` | `src/lib.rs` | 20 | `https://api.cartridge.gg/query` |

Internal (not re-exported): `ControllersStore`, `StoredController`, `ControllerNode`, `CONTROLLERS_TYPE = TypeId::new("controllers.sync")`.

### Internal Modules

- `lib.rs` — the entire crate; one file. Sections: GraphQL response types, `ControllersStore` (schema + upsert + cursor), `ControllersSink` (GraphQL query + polling loop + `Sink` impl).

### Sink trait wiring

| Method | Behavior |
|---|---|
| `name` | `"controllers"` |
| `interested_types` | `[TypeId::new("controllers.sync")]` — ignored; `process` uses `batch`, not envelopes |
| `process(_envelopes, batch)` | Computes `batch_time_window`; if greater than stored `synced_until`, issues GraphQL query filtered by `createdAtGT` / `createdAtLTE`, upserts results in SQLite-sized chunks (199) or Postgres-sized chunks (10 000) |
| `topics` | `Vec::new()` — no EventBus topic |
| `build_routes` | `Router::new()` — no HTTP endpoints |
| `initialize` | Creates schema; if table is empty, performs a **full sync** from epoch to `now` to seed the data |

### Storage

- **Tables** (both created with backend-aware DDL):
  - `controllers`: `id PK` (hex address), `address UNIQUE`, `username`, `deployed_at` (RFC 3339), `updated_at` (unix seconds).
  - `torii_controller_sync_state`: key/value pair; tracks `synced_until` cursor.
- **Upsert batching**: `SQLITE_CONTROLLER_UPSERT_BATCH_SIZE = 199`, `POSTGRES_CONTROLLER_UPSERT_BATCH_SIZE = 10_000` (both avoid the 32 767-parameter SQLite limit and fit well within PG).
- **Address normalisation**: `0x` + `hex::encode(felt.to_be_bytes_vec())` so lookups match whatever shape the on-chain address landed in.

### GraphQL contract

Query shape (`src/lib.rs::ControllersSink::build_query`):

```graphql
query {
  controllers(
    where: { createdAtGT: "<lower>", createdAtLTE: "<upper>" },
    orderBy: { field: CREATED_AT, direction: ASC }
  ) {
    edges { node { address createdAt account { username } } }
  }
}
```

Retries: `MAX_RETRIES = 3`, exponential backoff starting at
`INITIAL_BACKOFF = 2s`. Partial errors from the API are logged and
dropped; total failures bubble up from `process`.

### Interactions

- **Upstream (consumers)**: `bins/torii-arcade`, `bins/torii-introspect-bin`.
- **Downstream deps**: `torii`, `torii-runtime-common`, `torii-sql`, `sqlx` (sqlite/postgres/any), `reqwest`, `chrono`, `hex`, `metrics`, `tracing`, `serde`.

### Extension Points

- Point at a different backend API → pass `api_url` override to `ControllersSink::new`.
- Add more fields → extend `ControllerNode`, `StoredController`, schema migration, and the upsert SQL in `ControllersStore::upsert_controllers_and_store_synced_until`.
- Expose the data over gRPC → build a second sink that reads the `controllers` table; keep this sink single-responsibility (sync only).
