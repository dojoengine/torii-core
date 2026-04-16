# torii (root library)

The `torii` library is the heart of the indexer. It wires a three-stage ETL
pipeline (Extract → Decode → Sink) to an EventBus, a CommandBus, a gRPC
service, and an HTTP server, and exposes a single `run(ToriiConfig)`
orchestrator that binaries call.

## Role in Torii

Every binary in `bins/` builds a `ToriiConfig`, registers its own sinks,
decoders, and extractor, and hands the config to `torii::run`. This crate
owns the event loop, the cursor-commit contract, contract auto-identification,
graceful shutdown, and the gRPC/HTTP surface. Sinks and decoders plug in via
traits defined under `src/etl/`.

## Architecture

```text
                    +----------------------+
                    |  ToriiConfigBuilder  |  src/lib.rs (builder)
                    +-----------+----------+
                                |
                                v
  +---------------- torii::run(ToriiConfig) ----------------+  src/lib.rs:613
  |                                                          |
  |  tokio task #1 (producer)                                |
  |    Extractor.extract(cursor, engine_db)  ----> mpsc ---  |
  |                                                  |       |
  |  tokio task #2 (identifier, optional)            |       |
  |    ContractIdentifier.identify_contracts  <------+       |
  |                                                          |
  |  tokio task #3 (consumer loop)                           |
  |    DecoderContext.decode_events  --> Vec<Envelope>       |
  |    MultiSink.process(envelopes, batch)                   |
  |      |           |           |                            |
  |      v           v           v                            |
  |    EventBus   gRPC svc    HTTP routes    (per sink)       |
  |      |           |           |                            |
  |      +---> SubscriptionManager -> connected clients       |
  |                                                          |
  |  CommandBus: async fire-and-forget side channel          |
  |                                                          |
  |  EngineDb: cursor + stats + contract→decoder cache       |
  +----------------------------------------------------------+
                                |
                  axum + tonic on a single TCP port (+ optional TLS)
```

## Deep Dive

### Public API

| Type / Trait | File | Line | Purpose |
|---|---|---|---|
| `ToriiConfig` / `ToriiConfigBuilder` | `src/lib.rs` | 96 / 233 | All server config: sinks, decoders, extractor, filters, TLS, concurrency |
| `ToriiTlsConfig` | `src/lib.rs` | 202 | Optional TLS acceptor config with ALPN |
| `EtlConcurrencyConfig` | `src/lib.rs` | 78 | Prefetch batch cap (`max_prefetch_batches`) |
| `run(config)` | `src/lib.rs` | 613 | Single entry point for every binary |
| `Sink` (trait) | `src/etl/sink/mod.rs` | 74 | `name` / `interested_types` / `process` / `topics` / `build_routes` / `initialize` |
| `Decoder` (trait) | `src/etl/decoder/mod.rs` | 96 | `decoder_name` + `decode(keys, data, context)` |
| `Extractor` (trait) | `src/etl/extractor/mod.rs` | 299 | `extract(cursor, engine_db)` + `commit_cursor` |
| `ContractIdentifier` (trait) | `src/etl/identification/registry.rs` | 32 | Batch ABI lookup + shared cache |
| `IdentificationRule` (trait) | `src/etl/identification/rule.rs` | 52 | ABI pattern → `Vec<DecoderId>` |
| `EventBus` | `src/etl/sink/mod.rs` | 188 | Topic pub/sub via `SubscriptionManager` |
| `CommandBus` / `CommandHandler` | `src/command.rs` | 148 / 35 | Sinks dispatch fire-and-forget commands |
| `SubscriptionManager` | `src/grpc.rs` | 42 | Client registration + filter routing |
| `GrpcState` / `ToriiService` | `src/grpc.rs` | 134 / 156 | Core `torii.Torii` service |
| `EngineDb` / `EngineStats` | `src/etl/engine_db.rs` | 33 / (below) | Cursor + head + contract-decoder persistence |
| `Envelope` / `TypedBody` / `EventMsg` | `src/etl/envelope.rs` | 58 / 30 / 118 | Envelope model flowing through the pipeline |
| `DecoderContext` | `src/etl/decoder/context.rs` | 32 | Router from event → decoders (mapping > registry > all) |
| `MultiSink` | `src/etl/sink/multi.rs` | 16 | Runs sinks concurrently via `join_all`, aggregates topics & routes |
| `ContractFilter` / `DecoderId` | `src/etl/decoder/mod.rs` | 236 / 179 | Whitelist + blacklist + deterministic decoder IDs |

### Internal Modules

- `command.rs` — `Command`, `CommandHandler`, `CommandBus`, `CommandBusSender`; a bounded-queue MPSC that routes typed commands to matching handlers.
- `grpc.rs` — core `torii.Torii` service (`GetVersion`, `ListTopics`, `SubscribeToTopics[Stream]`) + `SubscriptionManager` + `ClientSubscription`.
- `http.rs` — minimal Axum router with `/health` and `/metrics`. Sinks merge their routes onto this via `MultiSink::build_routes`.
- `metrics.rs` — Prometheus recorder bootstrapped from `TORII_METRICS_ENABLED`; rendered at `/metrics`.
- `etl/mod.rs` — public re-exports (`Decoder`, `Extractor`, `Sink`, `MultiSink`, `EngineDb`, `Envelope`, `StarknetEvent`, `EventContext`, …).
- `etl/envelope.rs` — pipeline data unit: `Envelope { id, type_id, body: Box<dyn TypedBody>, metadata, timestamp }`. `EventMsg` is the sugar trait decoders most often implement via `to_envelope(ctx)`.
- `etl/engine_db.rs` — `sqlx::Pool<Any>` wrapper with embedded SQLite + PostgreSQL schema (see `sql/engine_schema*.sql`). Persists head, cursor, stats, and contract→decoder mappings.
- `etl/decoder/` — `Decoder` trait, `DecoderId`, `ContractFilter`, `DecoderContext` (routing logic).
- `etl/extractor/` — `Extractor` trait + several implementations: `SampleExtractor`, `BlockRangeExtractor`, `EventExtractor`, `GlobalEventExtractor`, `CompositeExtractor`, `SyntheticExtractorAdapter`, `SyntheticErc20Extractor`, plus `RetryPolicy` and starknet RPC helpers.
- `etl/identification/` — `IdentificationRule` trait, `ContractRegistry` (`ContractIdentifier` impl) with bounded positive/negative caches, batched JSON-RPC fetches (`MAX_BATCH_SIZE = 500`).
- `etl/sink/` — `Sink` trait, `EventBus`, `SinkContext`, `TopicInfo`; `multi.rs` drives per-sink concurrency with `futures::future::join_all`.

### Interactions

- **Upstream** (binaries): every `bins/*` calls `torii::run` with its own `ToriiConfigBuilder` assembly.
- **Downstream**: every `torii-*-sink`, `torii-dojo`, `torii-introspect`, `torii-erc20/721/1155` depends on this crate for the `Sink`, `Decoder`, `Extractor`, `Envelope`, `EventBus`, and `ContractIdentifier` types.
- **Workspace deps**: `torii-common` (indirect), `torii-types` (re-exports `StarknetEvent` / `EventContext` / `BlockContext`), `torii-sql` (not a direct dep — `engine_db` uses raw `sqlx::Any`).

### Key Invariants

- **Cursor safety**: the cursor is committed (`extractor.commit_cursor`) *only after* `multi_sink.process` returns `Ok` — see `src/lib.rs:1113-1127`. A crash between extract and sink processing replays the last batch; it never loses it.
- **Decoder determinism**: `DecoderContext::decoder_ids` sorts `DecoderId` values so the decode order is reproducible across restarts (`src/etl/decoder/context.rs:197`).
- **Decoder priority**: blacklist → explicit mapping → registry cache → fallback to all decoders. Stale cache entries pointing at unknown `DecoderId`s are evicted automatically (`src/etl/decoder/context.rs:341-360`).
- **Multi-sink isolation**: one sink failing logs & increments `torii_sink_failures_total` but does *not* abort the batch. Other sinks still run (`src/etl/sink/multi.rs:51-66`).
- **Shutdown**: SIGINT/SIGTERM cancels a `CancellationToken`. The HTTP/gRPC server has 15 s to drain; the ETL loop gets `config.shutdown_timeout` seconds (default 30 s) to finish its current batch.

### Extension Points

- Add a sink → implement `Sink` (see `crates/torii-sql-sink/src/lib.rs` as the reference).
- Add a decoder → implement `Decoder` (see `crates/torii-erc20/src/decoder.rs`).
- Add a custom extractor → implement `Extractor` (RPC-backed: `etl/extractor/event.rs`; deterministic: `SyntheticExtractor` under `etl/extractor/synthetic.rs`).
- Add auto-identification → implement `IdentificationRule` and register it on a `ContractRegistry`, then plug the registry in via `ToriiConfigBuilder::with_contract_identifier`.
- Add a background worker → implement `CommandHandler` and register via `ToriiConfigBuilder::with_command_handler`.
