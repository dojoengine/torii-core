# torii-sql-sink

The reference sink. Persists decoded envelopes as SQL operations into SQLite
or PostgreSQL, publishes updates on the EventBus, serves a
`torii.sinks.sql.SqlSink` gRPC service, and exposes `/sql/query` and
`/sql/events` REST endpoints. Every new sink author should read this one
first — it is the canonical implementation of the three-extension-point
pattern.

## Role in Torii

Torii's core library (`src/etl/sink/mod.rs:74`) defines the `Sink` trait but
does not persist anything. `torii-sql-sink` is the first real consumer: it
receives `Envelope`s decoded by a paired `SqlDecoder`, writes a row per
envelope, and fans the write out to three downstream channels (EventBus,
sink-specific gRPC broadcast, SQL-queryable table). It is used as the
primary demo sink in `examples/simple_sql_sink` and `examples/multi_sink_example`.

## Architecture

```text
+----------------------------------------------------------------+
|                           SqlSink                              |
|                                                                |
|  +-------------+    +---------------+    +------------------+  |
|  | SqlDecoder  |--->|  Envelope     |--->| Sink::process    |  |
|  | (sql.insert,     |  sql.insert/  |    | INSERT INTO      |  |
|  |  sql.update)     |  sql.update   |    | sql_operation    |  |
|  +-------------+    +---------------+    | (SQLite/Postgres)|  |
|                                          +------------------+  |
|                                                  |             |
|                      +---------------------------+----------+  |
|                      |                           |          |  |
|                      v                           v          v  |
|              EventBus.publish_          grpc_service       HTTP|
|              protobuf("sql", ...)       .update_tx.send()  api|
|              with matches_filters       (broadcast chan)   ↓  |
|                      |                           |     /sql/ *|
|                      v                           v             |
|               gRPC clients subscribed        SqlSinkService   |
|               to torii.Torii/Subscribe        (tonic)         |
+----------------------------------------------------------------+
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `SqlSink` | `src/lib.rs` | 44 | The `Sink` impl — holds `Arc<Pool<Any>>`, the gRPC service, and the EventBus handle |
| `SqlSink::new(database_url)` | `src/lib.rs` | 94 | Detects SQLite vs Postgres, runs the embedded schema |
| `SqlSink::get_grpc_service_impl` | `src/lib.rs` | 83 | Returns `Arc<SqlSinkService>` for registration via `with_grpc_router` |
| `SqlSink::generate_sample_events` | `src/lib.rs` | 54 | Test fixture events |
| `SqlDecoder` / `SqlInsert` / `SqlUpdate` | `src/decoder.rs` | — | Pair decoder; emits `sql.insert` / `sql.update` envelopes |
| `SqlSinkService` | `src/grpc_service.rs` | 18 | Implements `Query`, `StreamQuery`, `GetSchema`, `Subscribe` |
| `ProtoSqlOperation` / `SqlOperationUpdate` | `src/proto` (generated) | — | Wire types from `proto/sql.proto` |
| `FILE_DESCRIPTOR_SET` | `src/lib.rs` | 12 | Re-exported bytes for reflection composition |

### Internal Modules

- `lib.rs` — `Sink` impl. `process` handles both envelope types, broadcasts to EventBus + gRPC, writes the row.
- `decoder.rs` — `SqlDecoder`, `SqlInsert`, `SqlUpdate` + the `matches_filters` helper used by `EventBus::publish_protobuf`.
- `grpc_service.rs` — tonic service with a `tokio::sync::broadcast::Sender<SqlOperationUpdate>` fanning out live updates to subscribed RPC clients.
- `api.rs` — Axum handlers for `/sql/query` (arbitrary SQL) and `/sql/events` (recent rows).
- `samples.rs` — sample events for tutorials.
- `generated/` — `torii.sinks.sql.rs` + `sql_descriptor.bin` produced by `build.rs` from `proto/sql.proto`.

### Sink trait wiring

| Method | Behavior |
|---|---|
| `name` | `"sql"` |
| `interested_types` | `[TypeId::new("sql.insert"), TypeId::new("sql.update")]` |
| `process(envelopes, batch)` | For each envelope: run an `INSERT INTO {sql_operation}` via `QueryBuilder`, then encode to `ProtoSqlOperation`, publish on EventBus topic `"sql"` with filter fn `matches_filters`, and send on the gRPC broadcast channel |
| `topics` | One `TopicInfo { name: "sql", available_filters: [table, operation, value_gt, value_lt, value_gte, value_lte, value_eq] }` |
| `build_routes` | Axum router: `POST /sql/query`, `GET /sql/events` |
| `initialize(event_bus, ctx)` | Stores the `event_bus` handle so subsequent `process` calls can publish |

### Storage

Single table, same shape on both backends:

```sql
CREATE TABLE sql_operation (
    id          INTEGER PRIMARY KEY AUTOINCREMENT, -- SERIAL on Postgres
    table_name  TEXT NOT NULL,
    operation   TEXT NOT NULL,
    value       INTEGER NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Postgres uses the `sql_sink.sql_operation` schema-qualified name; SQLite uses
the bare table name. `SqlSink::table_name` (`src/lib.rs:87`) picks between
them.

### Filtering (EventBus topic `"sql"`)

| Filter key | Semantics |
|---|---|
| `table` | Exact match on `table_name` |
| `operation` | Exact match on `"insert"` / `"update"` |
| `value_gt`, `value_lt`, `value_gte`, `value_lte`, `value_eq` | Numeric comparisons on the integer `value` |

Filter logic lives in `SqlSink::matches_filters` (`src/lib.rs:~200`) and is
passed by reference to `EventBus::publish_protobuf` so the central bus can
decide per-client routing without re-decoding the protobuf for each filter
evaluation.

### Interactions

- **Upstream (consumers)**: `examples/simple_sql_sink`, `examples/multi_sink_example`.
- **Downstream deps**: `torii`, `sqlx` (sqlite/postgres/any), `tonic`, `tonic-web`, `prost`, `prost-types`, `axum`, `chrono`, `hex`, `anyhow`, `tracing`.
- **Workspace deps**: `torii` only (this sink pre-dates `torii-sql`; it uses raw `sqlx::Any`).

### gRPC registration pattern

```rust
use torii::{ToriiConfig, run};
use torii_sql_sink::{SqlDecoder, SqlSink};
use torii_sql_sink::proto::sql_sink_server::SqlSinkServer;
use tonic::transport::Server;
use std::sync::Arc;

let sink = SqlSink::new("postgres://torii:torii@localhost:5432/torii").await?;
let service = sink.get_grpc_service_impl();

let grpc_router = Server::builder()
    .accept_http1(true)
    .add_service(tonic_web::enable(SqlSinkServer::new((*service).clone())));

let config = ToriiConfig::builder()
    .add_sink_boxed(Box::new(sink))
    .add_decoder(Arc::new(SqlDecoder::new(Vec::new())))
    .with_grpc_router(grpc_router)
    .build();

run(config).await?;
```

### Extension Points

- New envelope types → widen `interested_types` + add a branch to `process` + add a new `TypeId::new("sql.*")`.
- New filter keys → add a key to `TopicInfo.available_filters` and extend `matches_filters`.
- New REST endpoints → add routes in `build_routes` (state stays in `api::SqlSinkState`).
- **Testing**: `grpcurl -plaintext localhost:8080 torii.sinks.sql.SqlSink/Query` and `curl http://localhost:8080/sql/events`.
