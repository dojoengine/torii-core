# torii-log-sink

A minimal, **in-memory** sink that turns decoded events into log entries,
publishes them on the EventBus, streams them over gRPC, and serves recent
entries over REST. Intended as a learning reference and a smoke-test sink
in the `multi_sink_example`. Data lives in a bounded `VecDeque` — old
entries fall off the back.

## Role in Torii

`LogSink` is the smallest possible real sink. It does not touch a database,
so binaries can drop it into a pipeline with zero infrastructure and still
observe the gRPC + EventBus + HTTP extension points. Use it alongside any
sink (`torii-sql-sink`, `torii-erc20`, etc.) to get a human-readable audit
log of events without provisioning a database.

## Architecture

```text
+---------------------------------------------------------+
|                        LogSink                          |
|                                                         |
|  +-------------+   +----------------+   +------------+  |
|  | LogDecoder  |-->|  Envelope      |-->| process()  |  |
|  | log.entry   |   |  LogEntry      |   +-----+------+  |
|  +-------------+   +----------------+         |         |
|                                               v         |
|                             +-----------------+--------+|
|                             |                          ||
|                             v                          v|
|                   grpc_service.log_store()        EventBus
|                     .add_log()                    publish_protobuf
|                   (VecDeque<ProtoLogEntry>          "logs"
|                    in Arc<RwLock<..>>)                  |
|                             |                          v|
|                             v                    gRPC topic
|                     LogSinkService                "logs"
|                     (QueryLogs,                        |
|                      SubscribeLogs)                    |
|                             |                          |
|                             v                          |
|                     HTTP: /logs, /logs/count           |
+---------------------------------------------------------+
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `LogSink` | `src/lib.rs` | 36 | Zero-config in-memory sink |
| `LogSink::new(max_logs)` | `src/lib.rs` | 49 | Bounded ring buffer sized at construction |
| `LogSink::get_grpc_service_impl` | `src/lib.rs` | 90 | Returns `Arc<LogSinkService>` for `with_grpc_router` |
| `LogDecoder` | `src/decoder.rs` | — | Turns arbitrary events into `LogEntry` envelopes; optional filter |
| `LogEntry` | `src/decoder.rs` | — | In-memory body: `message`, `block_number`, `event_key` |
| `LogSinkService` | `src/grpc_service.rs` | — | Implements `QueryLogs`, `SubscribeLogs`; holds the `VecDeque` |
| `ProtoLogEntry`, `LogUpdate` | `src/proto` (generated) | — | Wire types from `proto/log.proto` |
| `FILE_DESCRIPTOR_SET` | `src/lib.rs` | 11 | Re-exported bytes for reflection composition |

### Internal Modules

- `lib.rs` — `Sink` impl; holds the counter and the gRPC service.
- `decoder.rs` — `LogDecoder` + `LogEntry` body; supports optional event-key filter.
- `grpc_service.rs` — `LogSinkService` with `VecDeque` storage and a `tokio::sync::broadcast` channel; server impl for `QueryLogs` / `SubscribeLogs`.
- `api.rs` — Axum handlers: `GET /logs?limit=N`, `GET /logs/count`.
- `generated/` — `torii.sinks.log.rs` + `log_descriptor.bin` from `proto/log.proto`.

### Sink trait wiring

| Method | Behavior |
|---|---|
| `name` | `"log"` |
| `interested_types` | `[TypeId::new("log.entry")]` |
| `process(envelopes, _batch)` | Stores via `log_store().add_log()` (evicts oldest when full), publishes on EventBus topic `"logs"`, broadcasts via gRPC channel |
| `topics` | One `TopicInfo { name: "logs", available_filters: [] }` |
| `build_routes` | Axum router: `GET /logs`, `GET /logs/count` |
| `initialize` | Stores the `event_bus` handle |

### Storage

- In-memory `VecDeque<ProtoLogEntry>` wrapped in `Arc<RwLock<..>>`.
- Configured size at construction (`LogSink::new(max_logs)`); overflow evicts oldest.
- No persistence — restart loses all state.

### Interactions

- **Upstream (consumers)**: `examples/multi_sink_example`; any tutorial that wants visible side-effects without a DB.
- **Downstream deps**: `torii`, `tonic`, `prost`, `prost-types`, `tokio-stream`, `chrono`, `anyhow`, `tracing`.
- **Workspace deps**: `torii` only.

### gRPC registration pattern

```rust
use torii::{ToriiConfig, run};
use torii_log_sink::{LogDecoder, LogSink};
use torii_log_sink::proto::log_sink_server::LogSinkServer;
use tonic::transport::Server;
use std::sync::Arc;

let sink = LogSink::new(500);
let service = sink.get_grpc_service_impl();

let grpc_router = Server::builder()
    .accept_http1(true)
    .add_service(tonic_web::enable(LogSinkServer::new((*service).clone())));

let config = ToriiConfig::builder()
    .add_sink_boxed(Box::new(sink))
    .add_decoder(Arc::new(LogDecoder::new(None)))
    .with_grpc_router(grpc_router)
    .build();
run(config).await?;
```

### Extension Points

- Persist logs → replace `VecDeque` with a DB-backed store in `grpc_service.rs`; keep the `add_log` API shape so `Sink::process` stays untouched.
- Structured filters → widen `TopicInfo.available_filters` and add a filter fn to `EventBus::publish_protobuf` (pattern: see `torii-sql-sink::matches_filters`).
- New REST endpoints → add routes in `build_routes` (re-use the existing `LogSinkService` state).
