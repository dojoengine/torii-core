---
name: torii-maintainer
description: "Use when working on Torii (Starknet indexer): adding or debugging a sink / decoder / extractor / identification rule / command handler, tracing the ETL loop, cursor commit, EventBus / CommandBus / SubscriptionManager flow, gRPC / HTTP surface, contract identification, or introspect / dojo / token pipelines. Keywords: torii, ETL, sink, decoder, extractor, envelope, event bus, command bus, engine db, contract identifier, identification rule, introspect, dojo, erc20, erc721, erc1155, arcade, starknet indexer, cursor commit, prefetch queue."
globs: ["**/Cargo.toml", "src/etl/**", "crates/**"]
user-invocable: true
---

# torii-maintainer

Deep architectural knowledge for the Torii Starknet indexer. Every
per-crate README exists to fill in the details; this skill is the map.

## 1. When to use this skill

Trigger on any of:

- "writing a new sink / decoder / extractor / identification rule / command handler"
- "events lost after restart" / "cursor seems wrong" / "duplicate processing"
- "sink never receives envelope" / "decoder returns nothing" / "decoder fallback is slow"
- "auto-identification cache miss" / "`torii_registry_identify_jobs_total{status='dropped'}` is increasing"
- "gRPC 404 after adding a service" / "reflection does not see my service"
- "adding TLS / a topic / a REST route / a Prometheus metric"
- anything touching `src/lib.rs::run`, `src/etl/...`, `crates/torii-*-sink/`, `crates/dojo/`, `crates/introspect*/`, `crates/torii-erc{20,721,1155}/`.

Do **not** use for: generic Rust questions (prefer `m01-ownership`, `m06-error-handling`, `m07-concurrency`, `m11-ecosystem`), CLI UX (`domain-cli`), or web design (`domain-web`).

## 2. System map

```text
                                 ┌─────────────────────┐
                                 │  bins/*/main.rs     │
                                 │  (build ToriiConfig │
                                 │   via builder)      │
                                 └──────────┬──────────┘
                                            │
                                            v
   ┌────────────────────────────────────────────────────────────────────┐
   │                       torii::run(config)   (src/lib.rs:613)        │
   │                                                                    │
   │ Task 1 — producer           Task 2 — identifier   Task 3 — consumer│
   │   extractor.extract()          identify_contracts    prefetch_rx   │
   │   → mpsc<PrefetchedBatch>      (optional)            → decoder     │
   │     capacity =                 on contract_addresses   context     │
   │     max_prefetch_batches                             .decode_events│
   │                                                      → multi_sink  │
   │                                                        .process    │
   │                                                      → commit_cursor
   │                                                                    │
   │   side channels:                                                   │
   │     EventBus ─ SubscriptionManager ─ gRPC clients                  │
   │     CommandBus ─ CommandHandlers (async, fire-and-forget)          │
   │                                                                    │
   │   persistence:                                                     │
   │     EngineDb (head, cursor, contract→decoder cache)                │
   │                                                                    │
   │   surface:                                                         │
   │     axum HTTP /health /metrics + sink-merged routes                │
   │     tonic gRPC torii.Torii + sink services + reflection            │
   │     optional TLS listener                                          │
   └────────────────────────────────────────────────────────────────────┘
```

**Cursor-commit invariant** (`src/lib.rs:1113-1127`): the extractor's
cursor is committed **only after** `multi_sink.process(...)` returns
`Ok`. A crash mid-batch replays the last window; it never loses it.

## 3. Trait contracts

All four traits live in the root `torii` crate's `src/etl/`. Each sink or
decoder you ship impls one or more of these.

### `Sink` — `src/etl/sink/mod.rs:74`

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    fn name(&self) -> &str;
    fn interested_types(&self) -> Vec<TypeId>;
    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> anyhow::Result<()>;
    fn topics(&self) -> Vec<TopicInfo>;
    fn build_routes(&self) -> Router;
    async fn initialize(&mut self, event_bus: Arc<EventBus>, context: &SinkContext) -> anyhow::Result<()>;
}
```

Invariants:

- `interested_types` is **advisory** today — the `Sink::process` call receives *every* envelope, not just matching ones. Filter in `process` by `envelope.type_id`.
- `topics` is consumed at startup by `GrpcState` (`src/grpc.rs:134`) and published via `ListTopics`.
- `build_routes` output is `.merge`d into the main HTTP router by `MultiSink::build_routes` (`src/etl/sink/multi.rs:87`).
- `initialize` sees the `EventBus` + `SinkContext`. **This is your only chance to capture them by `Arc` for later use** — after `MultiSink` wraps the sink in `Arc<dyn Sink>` you lose mutable access. See `src/lib.rs:645` for the call site.
- `SinkContext { database_root, command_bus }` (`src/etl/sink/mod.rs:23`) — use `context.command_bus` to dispatch work to `CommandHandler`s.

### `Decoder` — `src/etl/decoder/mod.rs:96`

```rust
#[async_trait]
pub trait Decoder: Send + Sync {
    fn decoder_name(&self) -> &str;
    async fn decode(&self, keys: &[Felt], data: &[Felt], context: EventContext) -> anyhow::Result<Vec<Envelope>>;
    async fn decode_event(&self, event: &StarknetEvent) -> anyhow::Result<Vec<Envelope>> { /* default */ }
    async fn decode_events(&self, events: &[StarknetEvent]) -> anyhow::Result<Vec<Envelope>> { /* default */ }
}
```

Invariants:

- `decoder_name` produces a stable `DecoderId` via `xxh3_64`. **Never** change the name of a decoder in production — the registry cache is keyed on it and changing it leaves stale mappings referencing nothing (the routing logic in `DecoderContext::decode` evicts them, but you pay a full re-identification round per contract).
- Each decoder should return *only* envelopes it wants to produce. Returning an empty `Vec` is the "not interested" signal.
- `EventContext` = `{ from_address, block_number, transaction_hash }`; add more via `envelope.metadata` if a sink needs it.

### `Extractor` — `src/etl/extractor/mod.rs:299`

```rust
#[async_trait]
pub trait Extractor: Send + Sync {
    fn set_start_block(&mut self, start_block: u64);
    async fn extract(&mut self, cursor: Option<String>, engine_db: &EngineDb) -> Result<ExtractionBatch>;
    fn is_finished(&self) -> bool;
    async fn commit_cursor(&mut self, _cursor: &str, _engine_db: &EngineDb) -> Result<()> { Ok(()) }
    fn as_any(&self) -> &dyn std::any::Any;
}
```

Return-value semantics:

- Non-empty batch: process and call again.
- Empty batch + `is_finished() = false`: at the head; producer sleeps `cycle_interval` (see `src/lib.rs:976`) before retrying.
- Empty batch + `is_finished() = true`: shutdown ETL loop (`src/lib.rs:1035`).

### `ContractIdentifier` — `src/etl/identification/registry.rs:32`

```rust
#[async_trait]
pub trait ContractIdentifier: Send + Sync {
    async fn identify_contracts(&self, contract_addresses: &[Felt]) -> Result<HashMap<Felt, Vec<DecoderId>>>;
    fn shared_cache(&self) -> Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>;
}
```

The only production impl is `ContractRegistry` (`src/etl/identification/registry.rs:64`), which:
- Batches RPC calls (`MAX_BATCH_SIZE = 500`) via `JsonRpcClient::batch_requests`.
- Keeps two caches: positive (`Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>`) and bounded negative (`NegativeCache`, capacity `100_000`).
- Persists positives to `EngineDb` via `set_contract_decoders_batch`; loads them on startup via `load_from_db`.

### `IdentificationRule` — `src/etl/identification/rule.rs:52`

```rust
pub trait IdentificationRule: Send + Sync {
    fn name(&self) -> &str;
    fn decoder_ids(&self) -> Vec<DecoderId>;
    fn identify_by_abi(&self, contract_address: Felt, class_hash: Felt, abi: &ContractAbi) -> Result<Vec<DecoderId>>;
}
```

One rule per contract shape. See `crates/torii-erc20/src/identification.rs` for the reference impl.

### `CommandHandler` — `src/command.rs:35`

```rust
#[async_trait]
pub trait CommandHandler: Send + Sync {
    fn supports(&self, command: &dyn Command) -> bool;
    fn attach_event_bus(&self, _event_bus: Arc<EventBus>) {}
    async fn handle_command(&self, command: Box<dyn Command>) -> Result<()>;
}
```

The `CommandBus` (`src/command.rs:148`) is a bounded mpsc that dispatches `Box<dyn Command>` to the **single** matching handler. Ambiguous matches are rejected (`CommandDispatchError::Ambiguous`).

## 4. ETL loop anatomy

`src/lib.rs::run` (line 613 → 1293) spawns three tokio tasks:

1. **Producer** (`src/lib.rs:856-980`) — loops `extractor.extract(cursor, engine_db)`, pushes `PrefetchedBatch { batch, cursor, extractor_finished }` into an `mpsc` of capacity `etl_concurrency.max_prefetch_batches`. Also enqueues unique `contract_addresses` onto the identifier's channel (non-blocking `try_send`; full queue → `torii_registry_identify_jobs_total{status=dropped}`).
2. **Identifier** (`src/lib.rs:824-848`) — optional; present only if `ToriiConfigBuilder::with_contract_identifier` was called. Consumes addresses and calls `ContractIdentifier::identify_contracts`. Populates the registry cache; `DecoderContext::decode` reads through it on every event.
3. **Consumer** (`src/lib.rs:985-1141`) — awaits a `PrefetchedBatch`, decodes via `DecoderContext.decode_events`, processes via `MultiSink.process`, then **only on success** calls `extractor.commit_cursor`.

**Graceful shutdown** (`src/lib.rs:1156-1269`):
- `CancellationToken` flipped by SIGINT/SIGTERM.
- Server gets 15 s to drain (`SERVER_SHUTDOWN_TIMEOUT_SECS = 15`).
- ETL loop gets `config.shutdown_timeout` s (default 30) to finish the in-flight batch.

## 5. EventBus, CommandBus, SubscriptionManager

| Component | File | Purpose |
|---|---|---|
| `SubscriptionManager` | `src/grpc.rs:42` | `Arc<RwLock<HashMap<String, ClientSubscription>>>` — one entry per connected gRPC client |
| `ClientSubscription` | `src/grpc.rs:31` | `{ topics: HashMap<topic, filter_map>, tx: mpsc::Sender<TopicUpdate> }` |
| `EventBus` | `src/etl/sink/mod.rs:188` | Sink-facing façade over `SubscriptionManager`; exposes `publish_protobuf(topic, type_id, data, decoded, update_type, filter_fn)` |
| `GrpcState` | `src/grpc.rs:134` | Carries `subscription_manager` + `topics: Vec<TopicInfo>` |
| `ToriiService` | `src/grpc.rs:156` | `torii.Torii` service (`GetVersion`, `ListTopics`, `SubscribeToTopicsStream`, `SubscribeToTopics`) |
| `CommandBus` / `CommandBusSender` | `src/command.rs:148 / :71` | Bounded mpsc, routes `Box<dyn Command>` to matching `CommandHandler` |

Key routing rules:

- `EventBus::publish_protobuf` takes both `&Any` and `&T decoded`; the `filter_fn: Fn(&T, &filters) -> bool` runs *once per connected client* — keep it cheap. This avoids re-decoding the payload per client.
- A topic is visible to `ListTopics` only if a registered sink includes it in `topics()`.
- `SubscriptionManager::update_subscriptions` is additive for new topics and honours `unsubscribe_topics` for removal.
- `CommandBus::dispatch` fails if zero or >1 handlers match (`CommandDispatchError::Unsupported | Ambiguous`).

## 6. Storage stack

Three layers; don't confuse them:

- **`EngineDb`** (`src/etl/engine_db.rs:33`) — cursor, head, extractor state, contract→decoder cache. One per binary. `sqlx::Pool<Any>` (not `torii-sql`). Embedded schema under `sql/engine_schema*.sql`.
- **`torii-sql`** (`crates/sql/`) — shared abstraction for every *sink's* DB. `DbBackend::{Postgres, Sqlite}`, `PoolExt`, `SchemaMigrator`. Token sinks + `introspect-sql-sink` + `torii-controllers-sink` + `torii-ecs-sink` all route through it.
- **Sink-private storage** — `Erc20Storage`, `Erc721Storage`, `Erc1155Storage`, `ControllersStore`, `IntrospectDb<Backend>`. Each owns its own schema and connection pool.

Binary plumbing to resolve URLs: `torii-runtime-common::database::{resolve_single_db_setup, resolve_token_db_setup}` and `backend_from_url_or_path`. **Never mix** a Postgres engine with SQLite storage — `resolve_token_db_setup` rejects that combination.

## 7. Sink authoring recipe

1. Define a body type implementing `TypedBody` (or use the `typed_body_impl!` macro from `src/etl/envelope.rs`).
2. Decide the `TypeId` — call `TypeId::new("your_sink")`; make it a `const` at the top of the file.
3. Implement `Decoder` if your sink needs one (see `crates/torii-erc20/src/decoder.rs`).
4. Implement `Sink`:
   - `name()` returns a short stable name.
   - `interested_types()` lists your `TypeId`(s).
   - `process(envelopes, batch)` — downcast matching envelopes, persist, optionally `EventBus::publish_protobuf` and/or `grpc_service.broadcast`. Use `batch.is_live(threshold)` to skip broadcasting during historical backfill.
   - `topics()` returns the topic descriptors (`TopicInfo::new`). Keep filter keys stable.
   - `build_routes()` returns an `axum::Router`.
   - `initialize()` captures `event_bus` + `context.command_bus` into `self`.
5. If you have a gRPC service, expose it via `get_grpc_service_impl(&self) -> Arc<Service>` so binaries can mount it via `ToriiConfigBuilder::with_grpc_router(...)` before calling `torii::run`. Reference: `crates/torii-sql-sink/README.md`.
6. If you have custom reflection, set `with_custom_reflection(true)` and register both `TORII_DESCRIPTOR_SET` and your `FILE_DESCRIPTOR_SET` bytes in a `tonic_reflection::server::Builder`.

## 8. Decoder authoring recipe

1. Pick a stable `decoder_name` (e.g. `"erc20"`); the `DecoderId` is `xxh3_64(name)` — keep the name **forever**.
2. In `decode(keys, data, ctx)`:
   - If the event's first key isn't one you care about, `return Ok(Vec::new())`.
   - Deserialise; return one or more `Envelope`s via `EventMsg::to_envelope(ctx)` (see `src/etl/envelope.rs:118`).
3. Register with `ToriiConfigBuilder::add_decoder(Arc::new(MyDecoder::new()))`.
4. **For auto-discovery**: implement `IdentificationRule` returning the same `DecoderId::new("erc20")` and register the rule on a `ContractRegistry`.
5. **For performance**: encourage binaries to populate `ContractFilter::map_contract(addr, vec![decoder_id])` for known contracts. Routing priority in `DecoderContext::decode` (`src/etl/decoder/context.rs:313`): blacklist → explicit mapping → registry cache → fallback (try all decoders).

## 9. Binary authoring recipe

Every binary follows this skeleton — see `bins/torii-erc20/src/main.rs` for the minimal form:

1. `clap::Parser` config → validate via `torii-config-common`.
2. Resolve DB setup via `torii-runtime-common::database::resolve_{single,token}_db_setup`.
3. Construct provider (`JsonRpcClient<HttpTransport>`) and extractor.
4. Create storage + sink + gRPC service; capture `Arc<Service>` for router assembly.
5. Create decoder(s) + `IdentificationRule`(s); build a `ContractRegistry` and call `load_from_db().await`.
6. Build `tonic::transport::server::Router` with all sink gRPC services + optional reflection.
7. Build `ToriiConfig`:
   - `.add_sink_boxed(Box::new(sink))`
   - `.add_decoder(Arc::new(decoder))`
   - `.with_grpc_router(grpc_router)` + `.with_custom_reflection(true)` if you composed reflection yourself
   - `.with_contract_identifier(registry)` for auto-discovery
   - `.with_command_handler(Box::new(handler))` for async work
   - `.with_tls(ToriiTlsConfig::new(cert, key))` if needed
   - `.etl_concurrency(EtlConcurrencyConfig { max_prefetch_batches })`
8. `torii::run(config).await`.

**Synthetic pair**: for every binary that backs a production workload there is usually a `-synth` sibling (`bins/torii-erc20-synth`, `bins/torii-tokens-synth`, `bins/torii-introspect-synth`) that swaps the RPC extractor for a deterministic `SyntheticExtractor` and writes a `RunReport` JSON. Use those for perf regression testing; do **not** add new knobs to the production binary for profiling purposes.

## 10. Bug classes & runbooks

### 10.1 "Events missing after restart / duplicate processing"
- Check `torii_cursor_commit_failures_total`. If non-zero, look at the `extractor.commit_cursor` error in logs.
- Re-read the invariant: cursor is committed *after* `multi_sink.process` succeeds. If a sink panics, the cursor is *not* advanced → batch replays.
- If a sink has side-effects (e.g. sending to an external API) and you require exactly-once, add idempotency on the sink side — the pipeline gives at-least-once for side-effects between extract and cursor-commit.

### 10.2 "Sink never receives an envelope"
- Confirm the decoder's `decode()` actually returns an envelope for the event. Start at `src/etl/decoder/context.rs:313` and trace which branch is taken (blacklist / mapping / registry / fallback).
- Match `TypeId` — the `envelope.type_id` must match one of `sink.interested_types()`. Mismatched `TypeId::new("…")` strings are the most common cause.
- If the contract isn't in an explicit mapping and no registry is configured, only the **fallback all-decoders** path runs. See `has_registry` in `DecoderContext` (`context.rs:49`).

### 10.3 "Decoder fallback is slow"
- With many decoders, fallback tries all of them serially per event. Fix by registering a `ContractRegistry` + `IdentificationRule` to cache the mapping after first contact, or by adding an explicit `ContractFilter::map_contract`.
- `torii_registry_identify_duration_seconds` histogram shows how long identification itself takes; `torii_rpc_chunk_duration_seconds{extractor="registry"}` shows the RPC portion.

### 10.4 "Identification queue full / dropped"
- `torii_registry_identify_jobs_total{status="dropped"}` increasing means the producer's `identify_tx.try_send` is failing on a full channel.
- Increase `etl_concurrency.max_prefetch_batches` (the identify queue is sized at `prefetch_capacity.saturating_mul(2).max(8)`) or reduce batch size.

### 10.5 "gRPC 404 after adding a service"
- You must pass the router to Torii via `.with_grpc_router(router)`; Torii will mount the core service into it. If you also composed reflection yourself, set `.with_custom_reflection(true)` or you'll get duplicate-service errors.
- Confirm the service descriptor set is fed to `tonic_reflection` — otherwise `grpcurl list` won't show it.

### 10.6 "Sink panics abort the pipeline"
- They don't. `MultiSink::process` (`src/etl/sink/multi.rs:51-66`) logs the error, increments `torii_sink_failures_total{sink="<name>"}`, but continues. The cursor is **not** advanced because the multi-sink returns its own error to the ETL loop if *any* sink failed.
- If you need all-or-nothing semantics, wrap your sink to convert transient errors into retries, or sequence multiple sinks behind an ordered façade (see `bins/torii-arcade` / `bins/torii-introspect-bin`).

### 10.7 "EngineDb locked / PG pool exhausted"
- Engine DB is a single pool with `max_connections = 5` for real SQLite files and `1` for `:memory:` (`src/etl/engine_db.rs:80-88`). Do not hold an `EngineDb` transaction across `extract` / `decode` / `process`.
- Postgres sinks pool separately; check `max_db_connections` in binary configs.

### 10.8 "TLS listener won't start"
- Both `--tls-cert` and `--tls-key` must be present (enforced in `torii-introspect-bin/src/config.rs` and `torii-arcade/src/config.rs`). The PEM files are re-loaded at startup only; hot reload is not supported.

### 10.9 "Can't change decoder name without breaking clients"
- The `DecoderId` hash is persisted in the engine DB. If you change `decoder_name`, on next startup `DecoderContext::decode` will see stale cache entries pointing at an unknown `DecoderId`, evict them (`context.rs:341-360`), and fall back to all-decoders for the affected contracts until re-identified. This is safe but slow — renaming a decoder costs a full re-identification pass.

## 11. Observability reference

Emitted from `src/lib.rs::run` unless otherwise noted. All use the `metrics` facade backed by `metrics-exporter-prometheus`.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `torii_etl_cycle_total` | counter | `status={ok,empty,decode_error,sink_error,extract_error}` | One increment per ETL cycle |
| `torii_etl_cycle_duration_seconds` | histogram | — | End-to-end cycle latency |
| `torii_etl_cycle_gap_blocks` | gauge | — | `chain_head - last_block` |
| `torii_etl_last_success_timestamp_seconds` | gauge | — | Last successful batch wall time |
| `torii_etl_inflight_cycles` | gauge | — | 0 or 1 |
| `torii_etl_prefetch_queue_depth` | gauge | — | Prefetched batches waiting |
| `torii_etl_prefetch_stall_seconds` | histogram | — | Producer vs consumer pressure |
| `torii_events_extracted_total` | counter | — | Raw events out of the extractor |
| `torii_events_decoded_total` | counter | — | Events that reached decode |
| `torii_decode_envelopes_total` | counter | — | Envelopes produced |
| `torii_events_processed_total` | counter | — | Events after sink processing |
| `torii_transactions_processed_total` | counter | — | Transactions after sink processing |
| `torii_extract_batch_size_total` | counter | `unit={events,blocks,transactions}` | Batch fan-out |
| `torii_decode_failures_total` | counter | `stage` | Decode errors |
| `torii_cursor_commit_failures_total` | counter | — | Commit failures (data-safety canary) |
| `torii_sink_process_duration_seconds` | histogram | `sink` | Per-sink latency |
| `torii_sink_failures_total` | counter | `sink` | Per-sink failure count |
| `torii_registry_identify_duration_seconds` | histogram | — | Identification worker cycle |
| `torii_registry_identify_jobs_total` | counter | `status={enqueued,dropped,closed}` | Identify-queue health |
| `torii_rpc_chunk_duration_seconds` | histogram | `extractor`, `method` | Chunked RPC latency |
| `torii_rpc_parallelism` | gauge | — | Configured concurrency |
| `torii_command_dispatch_total` | counter | `command`, `status={enqueued,unsupported,ambiguous,dropped_full,closed}` | CommandBus dispatch outcomes |
| `torii_command_handle_total` | counter | `command`, `status={ok,error}` | Handler outcomes |
| `torii_command_handle_duration_seconds` | histogram | `command` | Handler latency |
| `torii_observability_enabled` | gauge | — | Seeded to 1 when metrics are on |
| `torii_uptime_seconds` | gauge | — | Set from `/health` |
| `torii_build_info` | gauge | `version` | Build info beacon |

Enable / disable via `TORII_METRICS_ENABLED` (`src/metrics.rs:20`).

## 12. Crate map

Root library:

- [`src/README.md`](../../../src/README.md) — `torii` (root library). Owns `ToriiConfig`, `run()`, `EngineDb`, `DecoderContext`, `MultiSink`, the three tokio tasks, CommandBus, SubscriptionManager.

Shared / core:

- [`crates/torii-common/README.md`](../../../crates/torii-common/README.md) — U256/Felt blob codecs, metadata + token-URI helpers.
- [`crates/types/README.md`](../../../crates/types/README.md) — `StarknetEvent`, `EventContext`, `BlockContext`.
- [`crates/torii-config-common/README.md`](../../../crates/torii-config-common/README.md) — CLI validation helpers.
- [`crates/torii-runtime-common/README.md`](../../../crates/torii-runtime-common/README.md) — DB setup + sink init helpers.
- [`crates/sql/README.md`](../../../crates/sql/README.md) — `torii-sql` (SQLite + Postgres abstraction).
- [`crates/testing/README.md`](../../../crates/testing/README.md) — `torii-test-utils` (event fixtures + FakeProvider).
- [`crates/pathfinder/README.md`](../../../crates/pathfinder/README.md) — Pathfinder SQLite reader + optional `etl` extractor.

Sinks:

- [`crates/torii-sql-sink/README.md`](../../../crates/torii-sql-sink/README.md) — **reference sink**; SQL operations + gRPC + HTTP + EventBus.
- [`crates/torii-log-sink/README.md`](../../../crates/torii-log-sink/README.md) — in-memory log ring, the minimum viable sink.
- [`crates/torii-controllers-sink/README.md`](../../../crates/torii-controllers-sink/README.md) — GraphQL-driven Cartridge controller sync.
- [`crates/arcade-sink/README.md`](../../../crates/arcade-sink/README.md) — Arcade projections from Dojo introspect events.
- [`crates/torii-ecs-sink/README.md`](../../../crates/torii-ecs-sink/README.md) — legacy `world.World` gRPC facade.

Tokens:

- [`crates/torii-erc20/README.md`](../../../crates/torii-erc20/README.md) — full ERC20 stack (decoder + rule + sink + service).
- [`crates/torii-erc721/README.md`](../../../crates/torii-erc721/README.md) — full ERC721 stack; owner-of tracking + EIP-4906.
- [`crates/torii-erc1155/README.md`](../../../crates/torii-erc1155/README.md) — full ERC1155 stack; transfer-history-first.

Domain / introspect:

- [`crates/dojo/README.md`](../../../crates/dojo/README.md) — `DojoDecoder`, `DojoEvent`, external-contract registration.
- [`crates/introspect/README.md`](../../../crates/introspect/README.md) — `IntrospectMsg` (13 schema-mutation variants).
- [`crates/introspect-sql-sink/README.md`](../../../crates/introspect-sql-sink/README.md) — Postgres / SQLite materializer for introspect events.

Binaries:

- [`bins/torii-erc20/README.md`](../../../bins/torii-erc20/README.md) — canonical minimal binary.
- [`bins/torii-erc20-synth/README.md`](../../../bins/torii-erc20-synth/README.md) — ERC20 perf harness (Postgres only).
- [`bins/torii-tokens/README.md`](../../../bins/torii-tokens/README.md) — unified ERC20 + ERC721 + ERC1155.
- [`bins/torii-tokens-synth/README.md`](../../../bins/torii-tokens-synth/README.md) — multi-standard perf harness with subcommands.
- [`bins/torii-introspect-bin/README.md`](../../../bins/torii-introspect-bin/README.md) — full-stack Dojo + tokens (`torii-server`).
- [`bins/torii-introspect-synth/README.md`](../../../bins/torii-introspect-synth/README.md) — Dojo introspect perf harness.
- [`bins/torii-arcade/README.md`](../../../bins/torii-arcade/README.md) — Cartridge Arcade backend.

## Verification cues before acting on memory

Before recommending a file:line referenced above, verify it still exists:

```bash
rg -n "^pub trait Sink\\b"            src/etl/sink/mod.rs
rg -n "^pub trait Decoder\\b"         src/etl/decoder/mod.rs
rg -n "^pub trait Extractor\\b"       src/etl/extractor/mod.rs
rg -n "^pub trait ContractIdentifier" src/etl/identification/registry.rs
rg -n "^pub trait IdentificationRule" src/etl/identification/rule.rs
rg -n "pub async fn run"              src/lib.rs
```

If any of these no longer match, the skill is drifting — open the
per-crate README to re-anchor before editing.
