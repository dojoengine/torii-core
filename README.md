# Torii

A high-performance, modular blockchain indexer and stream processor for Starknet.

## 🎯 Overview

Torii is a **pluggable ETL (Extract-Transform-Load) engine** that processes blockchain events through custom sinks. It provides real-time gRPC streaming, HTTP REST APIs, and a flexible architecture for building custom indexers.

This work is still in progress, refer to the modules documentation for more details.

### Core Concepts

- **ETL Pipeline**: Extract events → Decode to typed envelopes → Process in sinks.
- **Pluggable Sinks**: Custom data processors with their own HTTP routes, gRPC services, and business logic.
- **EventBus**: Central hub for topic-based event streaming over gRPC.
- **Modular Architecture**: Library-only crate with separate sink packages.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────┐
│              TORII CORE ENGINE                  │
├─────────────────────────────────────────────────┤
│                                                 │
│  Extractor → Decoder → Sink → EventBus         │
│     ↓          ↓         ↓        ↓            │
│  Events    Envelopes  Storage  gRPC/HTTP       │
│                                                 │
└─────────────────────────────────────────────────┘
```

### ETL Flow

1. **Extract** - Fetch events from blockchain (Archive or Starknet RPC)
2. **Transform** - Decode events into typed envelopes via custom decoders
3. **Load** - Process envelopes in sinks, store data, publish to EventBus

## 🚀 Quick Start

### Start PostgreSQL (Docker Compose)

```bash
docker compose up -d postgres
```

Default connection string:

```bash
export DATABASE_URL=postgres://torii:torii@localhost:5432/torii
```

### Observability Stack (Prometheus + Grafana)

```bash
# Start metrics stack + postgres exporter
docker compose up -d postgres postgres-exporter prometheus grafana
```

Torii exposes Prometheus metrics on `GET /metrics` (enabled by default).

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3002` (default `admin/admin`)
- Postgres exporter metrics: `http://localhost:9187/metrics`

If Torii runs outside Compose (default), Prometheus scrapes:

- `host.containers.internal:3000/metrics` (Podman-compatible)
- `host.docker.internal:3000/metrics` (Docker Desktop-compatible)

If Torii uses a custom `--port`, update `observability/prometheus/prometheus.yml` target port accordingly.

Disable metrics export in Torii with:

```bash
export TORII_METRICS_ENABLED=false
```

### Running Examples

```bash
# Multi-sink example with SQL + Log sinks (can be used with the client as demo).
cargo run --example multi_sink_example

# EventBus-only (no storage).
cargo run --example eventbus_only_sink

# HTTP-only (no gRPC streaming).
cargo run --example http_only_sink
```

### Testing with gRPC

```bash
# List all services
grpcurl -plaintext localhost:8080 list

# List available topics
grpcurl -plaintext localhost:8080 torii.Torii/ListTopics

# Subscribe to updates
grpcurl -plaintext -d '{"client_id":"test","topics":[{"topic":"sql"}]}' \
  localhost:8080 torii.Torii/SubscribeToTopicsStream
```

## 📚 Examples

### EventBus-Only Sink

See `examples/eventbus_only_sink/` - Minimal sink that only publishes to EventBus (no storage, no HTTP).

**Use case**: Real-time event broadcasting without persistence.

### HTTP-Only Sink

See `examples/http_only_sink/` - Sink with REST endpoints but no gRPC streaming.

**Use case**: Query API without real-time subscriptions.

### Full-Featured Sinks

See `crates/torii-sql-sink/` and `crates/torii-log-sink/` for complete implementations with:
- Custom protobuf definitions
- gRPC services
- HTTP REST endpoints
- EventBus integration
- Full reflection support

## 🧪 Client Demo

A TypeScript/Svelte client with gRPC-web support is available in `client/`:

```bash
# First start the example multi sink:
cargo run --example multi_sink_example

# Then start the client:
cd client
pnpm install
pnpm run dev
```

Features:
- Real-time gRPC-web streaming
- Multi-topic subscriptions with filters
- Protobuf decoding
- SQL and Log sink integration

## 🔧 Advanced Topics

### Adding Custom gRPC Services

To expose a custom gRPC service for your sink:

1. Define protobuf schema in your sink's `proto/` directory
2. Generate descriptor sets in `build.rs`
3. Build reflection with both core and sink descriptor sets
4. Use `.with_grpc_router()` and `.with_custom_reflection(true)`

See `crates/torii-sql-sink/` for a complete example.

## 📝 License

MIT
