# torii-introspect-sql-sink

Materialises **Dojo / Introspect events** into a live SQL schema. This is
the sink that turns `IntrospectMsg::CreateTable` into a `CREATE TABLE`,
`InsertsFields` into an `INSERT`, `DropTable` into a `DROP TABLE`, and so
on. Generic over a backend (`IntrospectPgDb`, `IntrospectSqliteDb`) via
the `IntrospectProcessor` / `IntrospectQueryMaker` traits.

## Role in Torii

This is the first sink in a Dojo pipeline. It owns the SQL schema. Other
Dojo-aware sinks (`torii-ecs-sink`, `arcade-sink`) read from the tables
this sink writes. Every binary that indexes a Dojo world includes it — see
`bins/torii-introspect-bin`, `bins/torii-arcade`.

## Architecture

```text
Envelope with DOJO_TYPE_ID (from torii-dojo::DojoDecoder)
         |
         v
+-----------------------------------------------------+
|           IntrospectDb<Backend>                     |
|                                                     |
|  impl Sink:                                         |
|    process(envelopes):                              |
|      filter DojoEvent::Introspect(IntrospectMsg)    |
|      collect into Vec<IntrospectBody>               |
|      self.process_messages(...)                     |
|        (IntrospectProcessor trait)                  |
|                                                     |
|  state:                                             |
|    - tables cache (Vec<DbTable>)                    |
|    - namespace mode (NamespaceMode)                 |
|    - column/field metadata (DbColumn, DbDeadField)  |
+------+----------------------------------------------+
       |                                              ^
       |                                              |
       v                                              |
+-----------------------------+                       |
| IntrospectQueryMaker (SQL)  |                       |
|   CREATE/ALTER/DROP TABLE   |                       |
|   INSERT/DELETE rows        |                       |
|   type mapping              |                       |
+--------+--------------------+                       |
         |                                            |
         v                                            |
+-----------------------------+      +----------------+
| Postgres backend            |      | SQLite backend |
| IntrospectPgDb              |      | IntrospectSqliteDb
|   native DDL                |      |   JSON append-  |
|   per-namespace schemas     |      |   only log per  |
|   typed columns             |      |   namespace     |
+-----------------------------+      +----------------+

(feature gated: postgres, sqlite; require at least one)
runtime.rs (both features): DbPool::Postgres | DbPool::Sqlite
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `IntrospectDb<Backend>` | `src/processor.rs` | — | Generic sink; `Sink` impl lives in `sink.rs` |
| `IntrospectSqlSink` (marker trait) | `src/sink.rs` | 14 | Provides `const NAME: &'static str` returned from `Sink::name` |
| `IntrospectProcessor` | `src/backend.rs` | — | Async backend contract: `process_messages(&[&IntrospectBody])` |
| `IntrospectQueryMaker` | `src/backend.rs` | — | SQL generation contract (DDL + record writes) |
| `IntrospectInitialize` | `src/backend.rs` | — | One-shot schema-setup contract |
| `DbTable`, `DbColumn`, `DbDeadField` | `src/processor.rs` | — | In-memory schema snapshot |
| `NamespaceMode` | `src/namespace.rs` | 11 | `None` / `Single(Arc<str>)` / `Address` / `Named(map)` / `Addresses(set)` |
| `TableKey`, `NamespaceKey` | `src/namespace.rs` | 20 / 32 | Routes a table to its schema bucket |
| `Table`, `DeadField`, `DeadFieldDef` | `src/table.rs` | — | Schema evolution helpers (dropped columns, retyped fields) |
| `IntrospectPgDb` | `src/postgres/mod.rs` (feature `postgres`) | — | Postgres backend (native DDL) |
| `IntrospectSqliteDb` | `src/sqlite/mod.rs` (feature `sqlite`) | — | SQLite backend (JSON append-only log) |
| Errors | `src/error.rs` | — | `DbError`, `TableError`, `TypeError`, `UpgradeError`, etc. |

### Internal Modules

- `sink` — `impl Sink for IntrospectDb<Backend>`; the `process` branch filtering `DojoEvent::Introspect` and forwarding to `process_messages`.
- `processor` — `IntrospectDb` struct + the generic `IntrospectProcessor` impl over `Pool<DB>`. Holds the cached `DbTable` list and the `NamespaceMode`.
- `backend` — three traits (`IntrospectQueryMaker`, `IntrospectProcessor`, `IntrospectInitialize`) — the backend interface.
- `namespace` — `NamespaceMode` + `TableKey` / `NamespaceKey`; routes a Dojo table to its physical schema slot.
- `table` / `tables` — `Table`, `DeadField`, cached list of alive tables; used to resolve incremental `UpdateTable` messages against prior state.
- `error` — typed domain errors (most carry a `TableKey` or `ColumnKey` for diagnostics).
- `postgres/` — Postgres backend: DDL via native `CREATE TABLE`, `ALTER TABLE`, JSON serialisation for unknown types, typed columns where possible.
- `sqlite/` — SQLite backend: append-only JSON log per (namespace, table) with type coercion on read.
- `runtime` (feature `postgres + sqlite`) — `IntrospectDb<DbPool>` runtime enum so one binary can open either backend.

### Sink trait wiring

| Method | Behavior |
|---|---|
| `name` | `Backend::NAME` (e.g. `"introspect-pg"`, `"introspect-sqlite"`) |
| `interested_types` | `[DOJO_TYPE_ID]` |
| `process` | Extracts `IntrospectMsg` from each envelope, counts categories (create/update/inserts/deletes) for metrics, calls `self.process_messages(...)`, and logs per-message failures |
| `topics` | `Vec::new()` — no EventBus topic |
| `build_routes` | `Router::new()` — no HTTP |
| `initialize` | Calls `self.initialize_introspect_sql_sink()` — creates catalog tables, idempotent |

### Namespace modes (NamespaceMode)

- `None` — single namespace, no routing.
- `Single(Arc<str>)` — all tables in one named namespace.
- `Address` — one namespace per owner Felt address.
- `Named(HashMap<Felt, Arc<str>>)` — owner → explicit namespace map.
- `Addresses(HashSet<Felt>)` — allow-list; unknown owners are dropped.

Pick the mode when the binary constructs the backend; it determines how
tables are grouped in Postgres schemas or SQLite filenames.

### Metrics

| Metric | Labels |
|---|---|
| `torii_introspect_sink_messages_total` | `message=create_table|update_table|inserts_fields` |
| `torii_introspect_sink_records_total` | `message=inserts_fields|delete_records` |

### Interactions

- **Upstream (consumers)**: `bins/torii-introspect-bin`, `bins/torii-arcade`, `bins/torii-introspect-synth`.
- **Downstream deps**: `torii`, `torii-dojo`, `torii-introspect`, `torii-sql`, `sqlx` (feature-gated), `introspect-types`, `starknet-types-core`, `starknet-types-raw`, `primitive-types`, `xxhash-rust` (feature `postgres`), `tokio`, `async-trait`, `itertools`, `hex`, `metrics`, `tracing`.
- **Features**: `postgres`, `sqlite` (at least one required). The `runtime` module needs both.

### Extension Points

- New backend → impl `IntrospectQueryMaker` + `IntrospectProcessor` + `IntrospectInitialize` + provide `IntrospectSqlSink::NAME`. The `Sink` impl is generic; no changes there.
- New namespace strategy → add a variant to `NamespaceMode` and extend every `resolve_*` helper that pattern-matches it.
- Change the SQLite row encoding → swap the JSON writer in `sqlite/` and bump the schema version; catalog tables detect the mismatch on startup.
