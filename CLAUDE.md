# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Torii is a modular blockchain indexer and stream processor for Starknet. It's a pluggable ETL (Extract-Transform-Load) engine that processes blockchain events through custom sinks, providing real-time gRPC streaming and HTTP REST APIs.

**Key Architecture**: Library-only Rust crate with pluggable sink architecture. Not a binary — users build applications that use Torii as a library.

## Common Commands

### Rust (Core Engine)

```bash
cargo build                              # Build all workspace crates
cargo test                               # Run all tests
cargo test -p torii                      # Test core crate
cargo test -p torii-sql-sink             # Test specific crate
cargo check --workspace                  # Check all crates
cargo run --example multi_sink_example   # Run multi-sink demo
cargo run --example simple_sql_sink      # Run SQL sink demo
cargo run --example eventbus_only_sink   # Run EventBus-only demo
cargo run --example http_only_sink       # Run HTTP-only demo
```

### TypeScript SDK (torii.js)

```bash
cd torii.js
bun run build          # Build CLI + client library
bun run typecheck      # Type checking
bun run dev            # Run CLI in dev mode
```

### Client App (SvelteKit)

```bash
cd client
pnpm install
pnpm run dev           # Start dev server
pnpm run build         # Production build
pnpm run check         # Type checking
pnpm run generate      # Generate TS code from running server via gRPC reflection
```

### Testing with grpcurl

```bash
grpcurl -plaintext localhost:8080 list
grpcurl -plaintext localhost:8080 torii.Torii/ListTopics
grpcurl -plaintext -d '{"client_id":"test","topics":[{"topic":"sql"}]}' \
  localhost:8080 torii.Torii/SubscribeToTopicsStream
```

## Monorepo Structure

```
/                          Core torii library crate (Rust)
├── src/                   Core ETL engine source
│   ├── lib.rs             ToriiConfig builder + run() orchestration (687 lines)
│   ├── grpc.rs            SubscriptionManager + ToriiService (312 lines)
│   ├── http.rs            Axum HTTP router + /health endpoint (100 lines)
│   └── etl/
│       ├── envelope.rs    Envelope, TypeId, TypedBody (92 lines)
│       ├── engine_db.rs   SQLite state tracking, cursors (422 lines)
│       ├── decoder/
│       │   ├── mod.rs     Decoder trait, DecoderId, ContractFilter (277 lines)
│       │   └── context.rs DecoderContext routing logic (222 lines)
│       ├── extractor/
│       │   ├── mod.rs     Extractor trait, ExtractionBatch (252 lines)
│       │   ├── block_range.rs  BlockRangeExtractor (358 lines)
│       │   ├── event.rs   EventExtractor - starknet_getEvents (838 lines)
│       │   ├── composite.rs CompositeExtractor - multi-source (199 lines)
│       │   ├── sample.rs  SampleExtractor - demo/testing (197 lines)
│       │   └── retry.rs   RetryPolicy - exponential backoff (236 lines)
│       └── sink/
│           ├── mod.rs     Sink trait, EventBus, TopicInfo (270 lines)
│           └── multi.rs   MultiSink - sequential processing (196 lines)
├── crates/
│   ├── torii-sql-sink/    SQL sink (SQLite, gRPC SqlSink service, HTTP API)
│   ├── torii-log-sink/    Log sink (VecDeque, gRPC LogSink service, HTTP API)
│   ├── torii-erc20/       ERC20 decoder + sink (multi-format Transfer/Approval)
│   ├── torii-erc721/      ERC721 decoder + sink
│   ├── torii-erc1155/     ERC1155 decoder + sink
│   └── torii-common/      Shared Felt/U256 conversions (95 lines)
├── bins/
│   ├── torii-erc20/       ERC20 indexer binary
│   └── torii-tokens/      Multi-token indexer binary
├── torii.js/              TypeScript SDK (@toriijs/sdk)
│   ├── src/client/        GrpcTransport, protobuf codec, ToriiClient
│   └── src/cli/           Code generator (reflection + proto parsing)
├── client/                SvelteKit 5 dashboard app
├── examples/              6 example applications
├── proto/                 Core protobuf definitions (torii.proto)
└── sql/                   SQL schema files for EngineDb
```

## Rust ETL Engine

### Config & Startup Flow

**`src/lib.rs`** — ToriiConfig fields:
- `port`, `host` — Network bind (default 8080, "0.0.0.0")
- `sinks` — `Vec<Box<dyn Sink>>` (uninitialized)
- `decoders` — `Vec<Arc<dyn Decoder>>`
- `extractor` — `Option<Box<dyn Extractor>>` (default SampleExtractor)
- `partial_grpc_router` — Pre-built gRPC router with sink services
- `custom_reflection` — User-provided reflection flag
- `contract_filter` — ContractFilter (whitelist/blacklist)
- `cycle_interval`, `events_per_cycle` — ETL tuning
- `database_root` — EngineDb location
- `shutdown_timeout` — Graceful shutdown seconds (default 30)

**`run()` function (`src/lib.rs:370`)** — Orchestration steps:
1. Initialize SubscriptionManager and EventBus
2. Initialize sinks with event_bus and SinkContext
3. Create MultiSink from initialized sinks
4. Create EngineDb for state persistence
5. Create/select Extractor
6. Create DecoderContext with contract filtering
7. Build gRPC router (user-provided or fresh) + reflection
8. Build HTTP router merging sink routes
9. **ETL Pipeline Loop**: Extract → Decode → Process → Commit cursor
10. Start HTTP/gRPC server with graceful shutdown (SIGINT/SIGTERM)

**Critical**: Cursor is committed only AFTER sink processing succeeds (`src/lib.rs:587-594`), preventing data loss on crash.

### Extractor System

**Extractor trait (`src/etl/extractor/mod.rs:173`):**
```rust
async fn extract(&mut self, cursor: Option<&str>, engine_db: &EngineDb) -> Result<ExtractionBatch>;
fn is_finished(&self) -> bool;
async fn commit_cursor(&self, cursor: &str, engine_db: &EngineDb) -> Result<()>;
```

**ExtractionBatch** — events (Vec), blocks (HashMap), transactions (HashMap), declared_classes, deployed_contracts, cursor, chain_head.

Four implementations:
- **BlockRangeExtractor** (`block_range.rs`) — Fetches blocks via JSON-RPC batch requests. Cursor format: `"block:N"`. Supports chain head following when `to_block` is None.
- **EventExtractor** (`event.rs`) — Uses `starknet_getEvents` for contract-specific filtering. Each contract tracked separately. Supports multi-contract batch requests.
- **CompositeExtractor** (`composite.rs`) — Wraps multiple extractors with round-robin scheduling. Each child maintains own cursor.
- **SampleExtractor** (`sample.rs`) — Cycles through predefined events for testing.

**RetryPolicy** (`retry.rs`) — Configurable exponential backoff. Default: 5 retries, 1s initial, 60s max, 2.0x multiplier. Presets: `no_retry()`, `aggressive()`.

### Decoder System

**Decoder trait (`src/etl/decoder/mod.rs:94`):**
```rust
fn decoder_name(&self) -> &str;              // Hashed to DecoderId
async fn decode_event(&self, event: &EmittedEvent) -> Result<Vec<Envelope>>;
```

**DecoderId** (`mod.rs:156`) — Hash-based from `decoder_name()`. Deterministic across restarts. Sortable for deterministic ordering.

**ContractFilter** (`mod.rs:214`) — Three-level contract routing:
1. **Blacklist** (HashSet) — Skip entirely (O(1) check)
2. **Mapping** (HashMap<Felt, Vec<DecoderId>>) — Use only specified decoders (O(k))
3. **Unmapped** — Try all decoders (auto-discovery)

**DecoderContext** (`context.rs:26`) — Orchestrates multiple decoders. Checks blacklist first, then mapping, then falls back to all decoders. Decoders return empty Vec if uninterested.

### Envelope System

**`src/etl/envelope.rs`:**
```rust
pub struct Envelope {
    pub id: String,
    pub type_id: TypeId,                    // Hash-based from type name string
    pub body: Box<dyn TypedBody>,           // Downcasted by sinks
    pub metadata: HashMap<String, String>,
    pub timestamp: i64,
}
```

`TypedBody` trait requires `envelope_type_id()` and `as_any()`/`as_any_mut()` for downcasting.

### Sink System

**Sink trait (`src/etl/sink/mod.rs:68`):**
```rust
fn name(&self) -> &str;
fn interested_types(&self) -> Vec<TypeId>;       // Optional type filtering
async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()>;
fn topics(&self) -> Vec<TopicInfo>;              // EventBus topics
fn build_routes(&self) -> Router;                // HTTP endpoints
async fn initialize(&mut self, event_bus: Arc<EventBus>, context: &SinkContext) -> Result<()>;
```

**MultiSink** (`multi.rs`) — Runs multiple sinks sequentially. Aggregates topics and merges HTTP routes.

**EventBus** (`mod.rs:182`) — Publishes to SubscriptionManager with topic-based filtering. Method: `publish_protobuf(topic, type_id, data, decoded, update_type, filter_fn)`.

### EngineDb

**`src/etl/engine_db.rs`** — SQLite with WAL mode.
- `get_head()` / `update_head()` — Track block_number and event_count
- `get_extractor_state()` / `set_extractor_state()` — Cursor persistence per extractor type
- `get_block_timestamps()` / `insert_block_timestamps()` — Block timestamp caching

### gRPC Service

**`src/grpc.rs`** — SubscriptionManager + ToriiService.
- `SubscriptionManager` (`line 37`) — Manages client subscriptions with topic → filters mapping via `RwLock<HashMap<String, ClientSubscription>>`.
- **RPCs**: `GetVersion`, `ListTopics`, `SubscribeToTopicsStream` (server streaming), `SubscribeToTopics` (bidirectional streaming with dynamic subscription updates).
- Multiplexing: Core `torii.Torii` service + sink-specific services + gRPC reflection on single port.

### HTTP Service

**`src/http.rs`** — Axum router. Built-in `GET /health` endpoint returning `{status, version, uptime_seconds}`. Merges routes from all sinks.

## Sink Implementations

### SqlSink (`crates/torii-sql-sink/`)

- **lib.rs** (359 lines) — SQLite pool, EventBus, gRPC service. Interested types: `sql.insert`, `sql.update`. Topics: "sql" with filters (table, operation, value comparisons).
- **decoder.rs** (168 lines) — `SqlInsert`/`SqlUpdate` structs. Matches `selector!("insert")` and `selector!("update")`.
- **grpc_service.rs** (325 lines) — `query()`, `stream_query()`, `get_schema()`, `subscribe()` with broadcast channel (capacity 1000).
- **api.rs** (133 lines) — `POST /sql/query`, `GET /sql/events`.

### LogSink (`crates/torii-log-sink/`)

- **lib.rs** (201 lines) — In-memory VecDeque with max_logs capacity. Topic: "logs" (no filters).
- **decoder.rs** (123 lines) — `LogEntry` struct. Optional key_filter matching.
- **grpc_service.rs** (132 lines) — `query_logs()`, `subscribe_logs()` (sends recent first, then streams new).
- **api.rs** (84 lines) — `GET /logs?limit=5`, `GET /logs/count`.

### ERC20/721/1155 (`crates/torii-erc20/`, `torii-erc721/`, `torii-erc1155/`)

- **Decoder** handles multiple event encoding formats (8 distinct Transfer/Approval patterns for ERC20)
- **Sink** batch-inserts to SQLite via Erc20Storage
- "Live" detection: only publishes to EventBus when within 100 blocks of chain head
- Topics: "erc20.transfer" (filters: token, from, to, wallet), "erc20.approval" (filters: token, owner, spender, account)

### torii-common (`crates/torii-common/`)

Shared Felt/U256 conversion utilities:
- `felt_to_blob()` / `blob_to_felt()` — 32-byte big-endian BLOB
- `u256_to_blob()` / `blob_to_u256()` — Compact variable-length encoding (1-32 bytes)

## TypeScript SDK (torii.js)

**Package**: `@toriijs/sdk` — Bun-first with Node fallback. Single dep: `@bufbuild/protobuf` v2.

### GrpcTransport (`src/client/GrpcTransport.ts`, 147 lines)

gRPC-Web over HTTP `fetch()`:
- **Unary**: POST with `Content-Type: application/grpc-web+proto`, frames request with 5-byte header, reads full response.
- **Streaming**: Uses `response.body.getReader()` for chunked reading. Frame format: `[0x00, len32_be, ...message]` for data, `[0x80, ...]` for trailers.

### Protobuf Codec (`src/client/protobuf.ts`, 424 lines)

Custom wire format codec:
- Wire types: VARINT(0), FIXED64(1), LENGTH_DELIMITED(2), FIXED32(5)
- `encodeProtobufObject()` / `decodeProtobufObject()` — Position-based (field numbers as f1, f2, ...)
- `encodeWithSchema()` / `decodeWithSchema()` — Schema-aware with field name ↔ number mapping
- Handles: varint encoding, zigzag for signed ints, BigInt, nested messages, `google.protobuf.Any`

### Schema Registry

- `setSchemaRegistry()` / `getSchemaRegistry()` — Global registry populated per-client on construction
- `MessageSchema` — name, fullName, fields (number, type, repeated, optional, messageType/enumType)
- Dual key registration: both short name and fully qualified name for flexible lookup

### BaseSinkClient (`src/client/BaseSinkClient.ts`, 81 lines)

Abstract base for generated clients:
- `unaryCall<T>(path, request, options)` — Single request/response
- `streamCall<T>(path, request, options)` — AsyncGenerator for server streaming
- `_subscribeWithCallbacks<T>(...)` — Wraps async generator in callback-style API with AbortController

### ToriiClient (`src/client/ToriiClient.ts`, 221 lines)

Plugin architecture with JavaScript Proxy pattern:
- Core methods: `getVersion()`, `listTopics()`, `subscribeTopics()`, `disconnect()`
- Plugins: `createToriiClient(baseUrl, { sql: SqlSinkClient, log: LogSinkClient })`
- Proxy intercepts property access: `client.sql.query()`, `client.listTopics()` — both work transparently

## CLI Code Generator

**Binary**: `torii.js` (via `torii.js/bin/torii.js`)

```bash
torii.js --url http://localhost:8080 --output ./generated    # Reflection mode
torii.js ./protos --output ./src/generated                    # Proto files mode
```

### Reflection Mode (`src/cli/reflection.ts`, 335 lines)

- Queries `ServerReflectionInfo` RPC via gRPC-Web
- Lists services, filters out `grpc.reflection.*` and `grpc.health.*`
- Fetches `FileDescriptorProto` per service
- Extracts types using `@bufbuild/protobuf` for parsing

### Proto File Mode (`src/cli/protos.ts`, 174 lines)

- Discovers `.proto` files via Bun Glob
- Regex-based parsing of package, service, rpc, message definitions
- Extracts field types, repeated/optional flags, map support

### Generator Output (`src/cli/generator.ts` + `types-generator.ts`)

Generates 4 files per run:
1. **`types.ts`** — TypeScript interfaces and enums for all protobuf messages
2. **`schema.ts`** — Runtime `MessageSchema` objects with field metadata for encoding/decoding
3. **`{ServiceName}Client.ts`** — Service client class extending `BaseSinkClient`. Unary methods return `Promise<T>`, streaming methods return `AsyncGenerator<T>` + callback variant `on{Method}()`
4. **`index.ts`** — Re-exports all clients and types

## Client App

SvelteKit 5 dashboard (`client/`):
- Uses `@toriijs/sdk` with generated clients
- Multi-topic subscription management with filters
- SQL query execution (HTTP + gRPC)
- Log sink subscription with real-time updates
- Connection health monitoring

## Examples

- **`multi_sink_example/`** — SqlSink + LogSink on single port with full gRPC router + reflection
- **`simple_sql_sink/`** — Single SqlSink with EventBus + gRPC + HTTP
- **`eventbus_only_sink/`** — No storage, no HTTP, no gRPC service — EventBus publishing only
- **`http_only_sink/`** — In-memory storage + REST routes, no streaming
- **`multi_sink_client_example/`** — Client-side multi-sink usage
- **`test_block_extractor/`** — BlockRangeExtractor integration test

## Key File Index

### Core Engine
- `src/lib.rs:370` — `run()` function (main orchestration)
- `src/lib.rs:67` — `ToriiConfig` struct
- `src/etl/sink/mod.rs:68` — `Sink` trait
- `src/etl/decoder/mod.rs:94` — `Decoder` trait
- `src/etl/extractor/mod.rs:173` — `Extractor` trait
- `src/etl/envelope.rs:40` — `Envelope` struct
- `src/etl/engine_db.rs:24` — `EngineDb` struct
- `src/grpc.rs:37` — `SubscriptionManager`
- `src/grpc.rs:154` — `ToriiService` gRPC impl

### Extractors
- `src/etl/extractor/block_range.rs:53` — `BlockRangeExtractor`
- `src/etl/extractor/event.rs:63` — `EventExtractor`
- `src/etl/extractor/composite.rs:49` — `CompositeExtractor`
- `src/etl/extractor/sample.rs:15` — `SampleExtractor`
- `src/etl/extractor/retry.rs:12` — `RetryPolicy`

### Sink Crates
- `crates/torii-sql-sink/src/lib.rs:41` — `SqlSink` struct
- `crates/torii-sql-sink/src/grpc_service.rs:16` — `SqlSinkService`
- `crates/torii-log-sink/src/lib.rs:36` — `LogSink` struct
- `crates/torii-log-sink/src/grpc_service.rs:51` — `LogSinkService`
- `crates/torii-erc20/src/decoder.rs:78` — `Erc20Decoder`
- `crates/torii-erc20/src/sink.rs:41` — `Erc20Sink`

### TypeScript SDK
- `torii.js/src/client/ToriiClient.ts:53` — `ToriiClientImpl` class
- `torii.js/src/client/ToriiClient.ts:186` — `createToriiClient()` (Proxy factory)
- `torii.js/src/client/GrpcTransport.ts:23` — `GrpcTransport` class
- `torii.js/src/client/protobuf.ts:278` — `encodeWithSchema()`
- `torii.js/src/client/protobuf.ts:332` — `decodeWithSchema()`
- `torii.js/src/client/BaseSinkClient.ts:11` — `BaseSinkClient` abstract class
- `torii.js/src/cli/reflection.ts:12` — `generateFromReflection()`
- `torii.js/src/cli/generator.ts:30` — `generateClientCode()`

## Critical Design Decisions

1. **Cursor commit ordering** — Cursor persisted only after sink processing succeeds (`src/lib.rs:587-594`). Prevents data loss: if sink fails, events will be re-extracted on restart.

2. **Contract filtering** — Three-level strategy (blacklist → explicit mapping → auto-discovery). Blacklist is O(1) HashSet check. Explicit mapping allows precise decoder routing. Auto-discovery sends events to all decoders as fallback.

3. **EventBus filter optimization** — `publish_protobuf()` accepts a `filter_fn` closure that checks subscription filters before encoding. Avoids serializing data for non-matching subscribers.

4. **DecoderId determinism** — Hash of `decoder_name()` string. Sorted via BTreeSet for deterministic ordering across restarts. No global registry needed.

5. **ERC20 live detection** — Sinks only publish to EventBus when within 100 blocks of chain head. During backfill, avoids flooding subscribers with historical data.

6. **gRPC multiplexing** — User builds partial router with sink services + reflection, passes to Torii which adds core service. Required because Rust's type system doesn't support dynamic service registration.

7. **TypeScript protobuf codec** — Custom implementation instead of generated code. Schema-aware encoding/decoding with runtime field name ↔ number mapping. Enables code generation from gRPC reflection without proto files.

## Important Patterns

### Building Multi-Sink Applications

```rust
// 1. Create sinks
let sql_sink = SqlSink::new(&db_path).await?;
let log_sink = LogSink::new(1000);

// 2. Get gRPC service implementations
let sql_grpc = sql_sink.get_grpc_service_impl();
let log_grpc = log_sink.get_grpc_service_impl();

// 3. Build gRPC router with ALL services + reflection
let reflection = tonic_reflection::server::Builder::configure()
    .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET)
    .register_encoded_file_descriptor_set(torii_sql_sink::FILE_DESCRIPTOR_SET)
    .register_encoded_file_descriptor_set(torii_log_sink::FILE_DESCRIPTOR_SET)
    .build_v1()?;

let grpc_router = tonic::transport::Server::builder()
    .add_service(SqlSinkServer::new(sql_grpc))
    .add_service(LogSinkServer::new(log_grpc))
    .add_service(reflection);

// 4. Configure and run
let config = ToriiConfig::builder()
    .add_sink_boxed(Box::new(sql_sink))
    .add_sink_boxed(Box::new(log_sink))
    .add_decoder(Arc::new(SqlDecoder::new()))
    .add_decoder(Arc::new(LogDecoder::new()))
    .with_grpc_router(grpc_router)
    .with_custom_reflection(true)
    .build();

torii::run(config).await?;
```

### Downcast Pattern in Sinks

```rust
async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
    for envelope in envelopes {
        if let Some(data) = envelope.downcast_ref::<SqlInsert>() {
            // Process typed data, access batch.blocks/transactions for context
        }
    }
    Ok(())
}
```

### EventBus Publishing

```rust
if let Some(event_bus) = &self.event_bus {
    event_bus.publish_protobuf("topic", type_id, &data, &decoded, update_type, |filters| {
        // Return true if this subscriber's filters match
        filters.get("table").map_or(true, |t| t == &table_name)
    }).await?;
}
```

## Notes

- Library crate: users import and configure it, not a standalone binary
- Sinks initialized with `initialize(&mut self, event_bus)` before processing
- `Arc` used for sharing sinks and decoders across async tasks
- EventBus uses Tokio broadcast channels
- EngineDb uses SQLite WAL mode for concurrent read/write performance
- Protobuf workflow: `.proto` in `proto/` (core) or `crates/*/proto/` (sinks), compiled via `tonic-build` in `build.rs`
