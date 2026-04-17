# arcade-sink (torii-arcade-sink)

Projects **Cartridge Arcade** entities (Games, Editions, Collections,
Listings, Sales) out of Dojo/Introspect events. The sink watches
`IntrospectMsg` envelopes from the `torii-dojo` decoder for mutations of
Arcade-model tables, plans refresh/delete operations, and calls into an
`ArcadeService` backed by the introspect SQL store + the ERC721 store to
keep a live read-model that the `arcade.v1` gRPC service serves.

## Role in Torii

`bins/torii-arcade` is the only consumer. It runs a full Dojo+token
pipeline and this sink sits near the end of it: introspect-sql-sink
materialises the raw Dojo tables, arcade-sink reads from them
(`ArcadeService::refresh_*`) to build Arcade-specific projections, and
exposes them over gRPC to the Arcade UI. ERC721 data is joined in for NFT
edition metadata.

## Architecture

```text
DojoDecoder (torii-dojo)
        |
        v  DojoBody { DojoEvent::Introspect(IntrospectMsg) }
        |
+--------------------------------+
|           ArcadeSink           |
|                                |
|  build_batch_plan(envelopes):  |
|   - InsertsFields / Delete-    |
|     Records / CreateTable /    |
|     RenamePrimary → plan ops   |
|   - detects requires_full_     |
|     rebuild or reload of       |
|     tracked_tables             |
|                                |
|  process():                    |
|   if full_rebuild:             |
|     service.bootstrap_from_    |
|       source()                 |
|   for op in ops:               |
|     TrackedTable.refresh()     |
|     TrackedTable.delete()      |
|                                |
|  tracked_tables:               |
|   HashMap<table_id_hex,        |
|           TrackedTable>        |
|     Game | Edition |           |
|     Collection | Listing | Sale|
+----------+---------------------+
           |
           v
+--------------------------------+
|        ArcadeService           |
|  Arc<sqlx::Pool<Any>>          |
|  (arcade + erc721 DBs)         |
|                                |
|  refresh_game/edition/…        |
|  delete_game/edition/…         |
|  bootstrap_from_source()       |
+--------------------------------+
           |
           v
   gRPC: arcade.v1 service (proto/arcade.v1.proto)
   via tonic + descriptor set (arcade_descriptor)
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `ArcadeSink` | `src/sink.rs` | 16 | Sink — holds `Arc<ArcadeService>` + `RwLock<HashMap<table_id, TrackedTable>>` |
| `ArcadeSink::new(db_url, erc721_db_url, max_conns)` | `src/sink.rs` | 111 | Opens the arcade + erc721 pools |
| `ArcadeService` | `src/grpc_service.rs` | — | gRPC service backing the `arcade.v1` RPCs |
| `FILE_DESCRIPTOR_SET` | `src/lib.rs` | 14 | Bytes for gRPC reflection |
| `proto::arcade` | `src/lib.rs` | 4 | Generated `arcade.v1` types |

Internal: `ProjectionOp` (Refresh / Delete), `ProjectionBatchPlan`, `TrackedTable` (5 known Arcade models + their hard-coded table IDs), `from_table_name()` fallback by model name (`ARCADE-Game`, `ARCADE-Edition`, `ARCADE-Collection`, `ARCADE-Order`/`ARCADE-Listing`, `ARCADE-Sale`).

### Internal Modules

- `lib.rs` — re-exports + `tonic::include_proto!("arcade.v1")` + descriptor bytes.
- `sink.rs` — `ArcadeSink`, `ArcadeSink::build_batch_plan`, `TrackedTable` mapping + async `refresh` / `delete` dispatchers.
- `grpc_service.rs` — `ArcadeService`; SQL reader + writer; implements the `arcade.v1` service.

### Sink trait wiring

| Method | Behavior |
|---|---|
| `name` | `"arcade"` |
| `interested_types` | `[DOJO_TYPE_ID]` — consumes all Dojo envelopes and filters for `IntrospectMsg::Inserts/Delete/CreateTable/RenamePrimary` |
| `process(envelopes, _batch)` | Builds a `ProjectionBatchPlan` (maybe with `reload_tracked_tables` or `requires_full_rebuild`), then calls `refresh_*` / `delete_*` on the service per op |
| `topics` | `Vec::new()` — no EventBus topic |
| `build_routes` | `Router::new()` — no HTTP routes (gRPC only) |
| `initialize` | No-op (pools already open from `new`) |

The gRPC service is **not** registered through `Sink::build_routes` — like
every gRPC-exposing sink, the binary wires it into tonic's router via
`ToriiConfigBuilder::with_grpc_router(Server::builder().add_service(...))`
before calling `torii::run`.

### Protobuf & gRPC

- Proto: `proto/arcade.v1.proto`.
- `build.rs` invokes `tonic_build` to generate the module and the descriptor set (`arcade_descriptor`).
- The descriptor is exported so binaries can compose reflection:
  `ToriiConfigBuilder::with_custom_reflection(true)` + a user-built `tonic_reflection` registering `TORII_DESCRIPTOR_SET` + `arcade_sink::FILE_DESCRIPTOR_SET` + any token-sink descriptors.

### Storage

Two SQL pools:
- **Arcade DB**: Arcade-specific tables populated by `bootstrap_from_source` + `refresh_*`.
- **ERC721 DB**: reads NFT ownership/metadata written by `torii-erc721`.

Both use sqlx's `Any` backend, so the binary can pick SQLite or PostgreSQL
via `torii-runtime-common::resolve_*_db_setup`.

### Interactions

- **Upstream (consumers)**: `bins/torii-arcade` only.
- **Downstream deps**: `torii`, `torii-common`, `torii-dojo`, `torii-introspect`, `torii-erc721`, `torii-sql`, `tonic`, `prost`, `sqlx`, `starknet`, `async-trait`, `tokio`, `tracing`.

### Extension Points

- New tracked table → add an `ARCADE-Foo` table ID to `TrackedTable::known_table_ids` + a `TrackedTable::Foo` variant + `refresh_foo` / `delete_foo` on `ArcadeService`.
- New projection operation → extend `ProjectionOp` and add a branch in `build_batch_plan_from_tables` + `process`.
- New RPCs → add to `proto/arcade.v1.proto` and `ArcadeService`. The descriptor set regenerates automatically on build.
