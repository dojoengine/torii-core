# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Torii is a high-performance, modular blockchain indexer and stream processor for Starknet. It's a pluggable ETL (Extract-Transform-Load) engine that processes blockchain events through custom sinks, providing real-time gRPC streaming and HTTP REST APIs.

**Key Architecture**: Library-only Rust crate with pluggable sink architecture. Not a binary - users build applications that use Torii as a library.

## Common Commands

### Rust (Core Engine)

```bash
# Build the library and all workspace crates
cargo build

# Run tests
cargo test

# Run a specific example
cargo run --example multi_sink_example
cargo run --example eventbus_only_sink
cargo run --example http_only_sink
cargo run --example simple_sql_sink

# Build a specific sink crate
cargo build -p torii-sql-sink
cargo build -p torii-log-sink

# Run tests for a specific crate
cargo test -p torii
cargo test -p torii-sql-sink

# Check all crates
cargo check --workspace
```

### TypeScript/Svelte Client

```bash
cd client

# Install dependencies
pnpm install

# Generate TypeScript code from protobuf definitions
pnpm run proto:generate
# Or directly:
./generate-proto.sh

# Start development server
pnpm run dev

# Build for production
pnpm run build

# Type checking
pnpm run check

# Formatting
pnpm run format
pnpm run lint
```

## High-Level Architecture

### ETL Pipeline Flow

The core architecture follows an Extract-Transform-Load pattern:

```
Extractor → Decoder → Sink → EventBus
   ↓          ↓        ↓        ↓
Events   Envelopes Storage  gRPC/HTTP
```

1. **Extractor**: Fetches raw events from blockchain (Archive or Starknet RPC)
2. **Decoder**: Transforms raw events into typed `Envelope` objects
3. **Sink**: Processes envelopes, stores data, publishes to EventBus
4. **EventBus**: Broadcasts updates to subscribed clients via gRPC

### Core Components

**`src/etl/`** - ETL pipeline components:
- `extractor/`: Blockchain event extraction
  - `BlockRangeExtractor`: Fetches blocks from Starknet full nodes using batch JSON-RPC requests
  - `SampleExtractor`: Demo extractor for testing with predefined events
  - `starknet_helpers.rs`: Converts Starknet RPC types to internal `BlockData` structure
  - Extracts events, transactions, declared classes, and deployed contracts
- `decoder/`: Event decoding to typed envelopes. Each decoder produces `Envelope` objects with typed bodies
- `sink/`: Pluggable data processors. The `Sink` trait is the main extension point
- `envelope.rs`: Core data structure (`Envelope`) that flows through the pipeline
- `engine_db.rs`: Tracks ETL state, cursor persistence, and statistics (SQLite-based)

**`src/grpc.rs`** - Central gRPC service implementation:
- `SubscriptionManager`: Manages client subscriptions to topics
- Core `Torii` gRPC service: `ListTopics`, `SubscribeToTopicsStream`, `SubscribeToTopics`
- Handles topic-based multiplexing across multiple sinks

**`src/http.rs`** - HTTP router that merges routes from all sinks

**`src/lib.rs`** - Main library entry point:
- `ToriiConfig` and builder pattern for configuration
- `run()` function that orchestrates the entire system

### Pluggable Sink Architecture

Sinks implement the `Sink` trait and can expose functionality through three channels:

1. **EventBus** (topic-based subscriptions): Publish updates via `event_bus.publish()`
2. **HTTP REST** (custom endpoints): Return `Router` from `build_routes()`
3. **gRPC services** (custom services): User adds sink services to router before passing to Torii

**Key Design**: Due to Rust's type system, gRPC services must be manually added by the user when building their application. See `examples/multi_sink_example/main.rs` for the pattern.

#### Sink Implementation Pattern

See `crates/torii-sql-sink/` and `crates/torii-log-sink/` for reference implementations.

Each sink crate typically contains:
- `proto/`: Protobuf definitions for sink-specific messages
- `build.rs`: Compiles protobufs and generates descriptor sets for reflection
- `decoder.rs`: Implements `Decoder` trait to produce sink-specific envelopes
- `grpc_service.rs`: Sink-specific gRPC service (if needed)
- `api.rs`: HTTP REST endpoints (if needed)
- `lib.rs`: Main `Sink` implementation

### Envelope System

`Envelope` is the core data structure:
```rust
pub struct Envelope {
    pub id: String,
    pub type_id: TypeId,  // Hash-based type identifier
    pub body: Box<dyn TypedBody>,  // Downcasted by sinks
    pub metadata: HashMap<String, String>,
    pub timestamp: i64,
}
```

- Decoders create envelopes with typed bodies
- Sinks filter by `type_id` and downcast bodies to concrete types
- Multiple decoders can produce multiple envelopes from one raw event

### gRPC Multiplexing

The system supports multiple gRPC services on a single port:

1. Core `torii.Torii` service (automatically added)
2. Sink-specific services (user adds via `with_grpc_router()`)
3. gRPC reflection (user builds with all descriptor sets)

**Pattern**: User builds partial router → adds sink services + reflection → passes to `ToriiConfig.with_grpc_router()` → Torii adds core service.

### Context Enrichment

The `process()` method receives both envelopes and the original `ExtractionBatch`:
- `batch.events`: Original raw events (Vec)
- `batch.blocks`: Deduplicated block context (HashMap for O(1) lookup)
- `batch.transactions`: Deduplicated transaction context (HashMap for O(1) lookup)
- `batch.declared_classes`: Declared classes from Declare transactions (Vec)
- `batch.deployed_contracts`: Deployed contracts from DeployAccount transactions (Vec)

This allows sinks to access block timestamps, transaction calldata, sender addresses, declared classes, deployed contracts, etc., without iterating through events.

### Contract Registry & Identification

The `ContractRegistry` routes events to appropriate decoders based on contract identification:

**Architecture**:
- Single source of truth for contract → decoder mappings
- Lazy identification: Contracts are identified on first event
- Pluggable identification rules (sink developers can add custom rules)
- Global policy: SRC-5 strict mode vs ABI heuristics

**Identification Flow**:
1. Event arrives from unknown contract
2. Registry fetches class hash and ABI
3. Runs all registered `IdentificationRule` implementations:
   - SRC-5 `supports_interface()` checks (if enabled)
   - ABI heuristics (function/event name matching)
4. Caches result: contract → [decoder_ids]
5. Routes subsequent events directly to cached decoders

**Identification Rule API**:
```rust
impl IdentificationRule for Erc20Rule {
    fn name(&self) -> &str { "ERC20" }

    // SRC-5: Returns (interface_id, decoder_ids)
    fn src5_interface(&self) -> Option<(Felt, Vec<DecoderId>)> {
        Some((ERC20_INTERFACE_ID, vec![DecoderId::ERC20]))
    }

    // ABI Heuristics: Pattern match on functions/events
    fn identify_by_abi(&self, contract_address: Felt, class_hash: Felt, abi: &ContractAbi) -> Result<Vec<DecoderId>> {
        if abi.has_function("transfer") && abi.has_function("balance_of") {
            Ok(vec![DecoderId::ERC20])
        } else {
            Ok(vec![])
        }
    }
}
```

**Configuration** (using `bitflags` for clean API):
```rust
use bitflags::bitflags;

// Combine multiple identification modes
let mode = ContractIdentificationMode::SRC5 |
           ContractIdentificationMode::ABI_HEURISTICS;

ToriiConfig::new()
    .identification_mode(mode)
    .map_contract(addr, vec![DecoderId::ERC20]) // Explicit override
    .with_identification_rule(Erc20Rule::new())

// Or strict SRC-5 only mode
ToriiConfig::new()
    .identification_mode(ContractIdentificationMode::SRC5)
```

**Design Benefits**:
- SRC-5: Rule provides interface ID + decoders, registry just checks
- ABI: Rule inspects ABI, returns decoders
- BTreeSet ensures decoders are deduplicated and deterministically ordered
- No duplicate logic between rule and registry
- Clean bitflags API for mode configuration

**Key Files**:
- `src/etl/extractor/contract_registry.rs`: Core registry implementation
- `ContractIdentificationMode`: Bitflags for SRC5/ABI_HEURISTICS
- `IdentificationRule` trait: Pluggable contract type detection

### Extractor System

Extractors fetch events from various sources:

**`SampleExtractor`** (demo/testing):
- Cycles through predefined events
- Used in examples for testing without network access

**`BlockRangeExtractor`** (production):
- Fetches blocks from Starknet full nodes via JSON-RPC
- Supports batch fetching for efficiency
- Automatic cursor persistence in `EngineDb`
- Retry logic for network failures
- Polls for new blocks when `to_block` is `None`
- Skips pre-confirmed blocks (only processes mined blocks)

**Extractor Configuration**:
```rust
use torii::etl::extractor::{BlockRangeExtractor, BlockRangeConfig, RetryPolicy};
use starknet::providers::jsonrpc::{JsonRpcClient, HttpTransport};
use starknet::providers::Url;

let config = BlockRangeConfig {
    rpc_url: "https://starknet-mainnet.public.blastapi.io".to_string(),
    from_block: 100_000,
    to_block: Some(200_000),  // Or None to follow chain head
    batch_size: 100,
    retry_policy: RetryPolicy::default(),
};

// Create provider
let provider = JsonRpcClient::new(HttpTransport::new(
    Url::parse(&config.rpc_url)?
));

// Create extractor (generic over any Provider)
let extractor = BlockRangeExtractor::new(Arc::new(provider), config);
```

**Cursor Management**:
- Cursors are opaque strings (e.g., `"block:123456"`)
- Automatically saved to `EngineDb.extractor_state` table
- Survives restarts - extraction resumes from last processed block
- Multiple extractors can coexist with different state keys

**Retry Policy**:
- Configurable max retries, backoff, and multiplier
- Reusable across all extractors
- Prevents transient network failures from stopping ingestion

## Key Files

- `src/lib.rs:262` - `run()` function (main orchestration)
- `src/etl/sink/mod.rs:48` - `Sink` trait definition (core extension point)
- `src/etl/decoder/mod.rs` - `Decoder` trait for event transformation
- `src/etl/extractor/mod.rs:112` - `Extractor` trait for fetching events
- `src/etl/extractor/block_range.rs` - Production block range extractor
- `src/etl/extractor/retry.rs` - Reusable retry policy for network failures
- `src/etl/engine_db.rs:205` - Extractor state persistence methods
- `src/grpc.rs:38` - `SubscriptionManager` for client subscriptions
- `proto/torii.proto` - Core gRPC service definitions
- `examples/multi_sink_example/main.rs` - Full multi-sink setup pattern

## Protobuf Workflow

### Rust Side

1. Define `.proto` files in `proto/` (core) or `crates/*/proto/` (sinks)
2. Compile in `build.rs` using `tonic-build`
3. Generate descriptor sets for gRPC reflection
4. Include generated code: `tonic::include_proto!("package.name")`

### Client Side

1. Proto files are copied from Rust crates: `cp ../proto/*.proto ./proto/`
2. Run `./generate-proto.sh` to generate TypeScript code
3. Generated files appear in `client/src/generated/`
4. Use `@protobuf-ts` for runtime serialization

## Development Workflow

### Adding a New Sink

1. Create new crate in `crates/my-sink/`
2. Define proto in `crates/my-sink/proto/my-sink.proto`
3. Implement `Decoder` to produce typed envelopes
4. Implement `Sink` trait (name, interested_types, process, initialize, etc.)
5. Optional: Implement gRPC service and HTTP routes
6. In your application: Add sink services to router, build reflection with all descriptors, pass to Torii

### Testing with Client

1. Start example: `cargo run --example multi_sink_example`
2. In another terminal: `cd client && pnpm install && pnpm run dev`
3. Open browser to test real-time subscriptions

### Testing with grpcurl

```bash
# List all services
grpcurl -plaintext localhost:8080 list

# List topics
grpcurl -plaintext localhost:8080 torii.Torii/ListTopics

# Subscribe to topics (server-side streaming)
grpcurl -plaintext -d '{"client_id":"test","topics":[{"topic":"sql"}]}' \
  localhost:8080 torii.Torii/SubscribeToTopicsStream

# Subscribe with filters
grpcurl -plaintext -d '{"client_id":"test","topics":[{"topic":"sql","filters":{"table":"user"}}]}' \
  localhost:8080 torii.Torii/SubscribeToTopicsStream
```

## Important Patterns

### Building Multi-Sink Applications

Always use this pattern (see `examples/multi_sink_example/main.rs`):

1. Create sinks
2. Build gRPC router with sink services AND reflection (all descriptor sets)
3. Create decoders
4. Use `ToriiConfig::builder()` to configure
5. Call `torii::run(config).await`

### gRPC Reflection

Must include ALL descriptor sets for complete reflection:
```rust
let reflection = tonic_reflection::server::Builder::configure()
    .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET)
    .register_encoded_file_descriptor_set(torii_sql_sink::FILE_DESCRIPTOR_SET)
    .register_encoded_file_descriptor_set(torii_log_sink::FILE_DESCRIPTOR_SET)
    .build_v1()?;
```

Then use `.with_custom_reflection(true)` to prevent Torii from adding its own reflection.

### EventBus Publishing

From a sink's `process()` method:
```rust
if let Some(event_bus) = &self.event_bus {
    event_bus.publish("topic_name", update_type, type_id, data).await?;
}
```

### Downcast Pattern

In sink's `process()`:
```rust
for envelope in envelopes {
    if let Some(data) = envelope.downcast_ref::<MyType>() {
        // Process typed data
    }
}
```

## Workspace Structure

- `/` - Core `torii` library crate
- `crates/torii-sql-sink/` - SQL sink implementation (SQLite)
- `crates/torii-log-sink/` - Log sink implementation (in-memory)
- `examples/` - Example applications showing different patterns
- `client/` - TypeScript/Svelte gRPC-web demo client
- `proto/` - Core protobuf definitions
- `sql/` - SQL schema files for engine database

## Notes

- This is a library crate, not a binary. Users import and configure it.
- Sinks are initialized with `initialize(&mut self, event_bus)` before processing.
- The system uses `Arc` for sharing sinks and decoders across async tasks.
- EventBus uses Tokio channels for broadcasting to subscribed clients.
- EngineDb tracks ETL state in SQLite with WAL mode for performance.
