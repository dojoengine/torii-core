# Torii-Core Project Handoff

## 1. What Is This Project?

**Torii-Core** is a modular **Starknet blockchain indexer** written in Rust. It extracts on-chain events from Cairo/Starknet smart contracts, decodes them from Cairo's binary serialization format, maps them onto dynamically-generated relational schemas (tables, columns, types), and sinks the processed data into PostgreSQL (or other backends). The system is designed around an ETL (Extract → Decode → Sink) pipeline with pluggable components at every stage.

### Two-Workspace Layout

The project spans **two Git repositories** on the developer's machine:

| Repo | Path | Role |
|------|------|------|
| **torii-core** | `/home/ben/cgg/torii-core` | Main indexer — ETL pipeline, Dojo decoder, sinks, binaries |
| **introspect** | `/home/ben/cgg/introspect` | Shared types crate — Cairo type system, serialization/deserialization, byte sources |

Key types from `introspect` that `torii-core` depends on:
- `CairoSeFrom<'a, 'de, T, D, C>` — Zero-copy Cairo→target-format serializer
- `CairoSerialization`, `CairoDeserialization<D>`, `CairoDeserializer` — Serialization traits
- `IntoByteSource`, `ByteSource`, `DerefBytesSource` — Byte stream abstraction
- `TypeDef`, `ColumnDef`, `PrimaryDef`, `TableSchema`, `Attribute` — Schema types
- `Felt` (re-exported from `starknet-types-core`) — 252-bit field element

---

## 2. Workspace Structure (torii-core)

```
Cargo.toml                   # Workspace root — 15 crate members
src/                         # Core library: ETL pipeline
  lib.rs                     # Re-exports grpc, http, etl
  grpc.rs                    # gRPC server
  http.rs                    # Axum HTTP server
  etl/
    mod.rs                   # Re-exports all ETL types
    envelope.rs              # Envelope, EventBody<T>, TypeId
    event.rs                 # Event primitives
    engine_db.rs             # Engine state persistence
    decoder/mod.rs           # Decoder trait, DecoderId, ContractFilter
    extractor/mod.rs         # Extractor trait, ExtractionBatch, contexts
    sink/mod.rs              # Sink trait, MultiSink, EventBus
    identification/          # Contract identification rules

crates/
  introspect/                # Introspect message types + schema logic
    src/events.rs            # IntrospectMsg enum (14 variants), event structs
    src/tables.rs            # RecordSchema, RecordFrame, CairoFrame, SerializeEntries
    src/manager.rs           # Table, Manager<Store>, StoreTrait
    src/sink.rs              # Introspect sink implementation
    src/types.rs             # Introspect-specific type helpers

  dojo/                      # Dojo World integration
    src/decoder.rs           # DojoDecoder, DojoEventProcessor trait
    src/table.rs             # DojoTable — Cairo→bytes field transcoding
    src/manager.rs           # DojoTableManager, DojoTableStore, JsonStore
    src/error.rs             # DojoToriiError enum

  introspect-postgres-sink/  # PostgreSQL sink
    src/processor.rs         # PostgresSchema, PostgresSimpleDb
    src/table.rs             # PgTable — DDL generation, schema lookups
    src/sql.rs               # SQL generation utilities
    src/json.rs              # JSON serialization for PG
    src/types.rs             # PgTableStructure, PostgresType, PgStructDef

  introspect-json-sink/      # JSON file sink
  introspect-sqlite-sink/    # SQLite sink (WIP)
  torii-common/              # Shared utilities
  torii-erc20/               # ERC-20 specific crate
  torii-erc721/              # ERC-721 specific crate
  torii-erc1155/             # ERC-1155 specific crate
  torii-log-sink/            # Log/observability sink
  torii-sql-sink/            # Generic SQL sink

bins/
  torii-erc20/               # ERC-20 indexer binary
  torii-tokens/              # Multi-token indexer binary
```

---

## 3. ETL Pipeline Architecture

```
┌─────────────┐     ┌──────────┐     ┌─────────────┐     ┌──────────┐
│  Extractor   │────▶│  Decoder  │────▶│  Envelope    │────▶│   Sink   │
│ (blockchain) │     │ (events)  │     │ (typed msg)  │     │ (output) │
└─────────────┘     └──────────┘     └─────────────┘     └──────────┘
```

### 3.1 Extractor (`src/etl/extractor/mod.rs`)

Pulls raw blockchain data from Starknet RPC nodes. Key types:

- **`ExtractionBatch`** — A batch of events with deduplication. Blocks/transactions stored in `HashMap` by hash, events reference them by hash to avoid data duplication.
- **`BlockContext`** — Block metadata (number, hash, timestamp)
- **`TransactionContext`** — Transaction details
- **`EventContext`** — Individual event with keys + data + parent refs
- **`Extractor` trait** — `extract()`, `is_finished()`, `commit_cursor()` methods
- Multiple extractor types: `BlockRangeExtractor`, `EventExtractor`, `SyntheticExtractor`, `SyntheticErc20Extractor`

### 3.2 Decoder (`src/etl/decoder/mod.rs`)

Converts raw events into typed `Envelope` messages. Key concepts:

- **`Decoder` trait** — Central trait. `decode(&self, ctx, event) → Vec<Envelope>`. One event can produce multiple envelopes if multiple decoders match.
- **`DecoderId`** — Deterministic decoder identification: `namespace:name:version` hashed to SHA-256
- **`ContractFilter`** — Controls which contracts/events a decoder handles:
  - `explicit_mappings`: Map of `contract_address → Set<event_selectors>` for targeted decoding
  - `blacklist`: Set of contract addresses to skip entirely
- **Multi-decoder pattern** — A single event can be processed by multiple decoders (e.g., ERC-20 + Dojo), producing separate typed envelopes for different sinks

### 3.3 Envelope (`src/etl/envelope.rs`)

Type-safe message wrapper. Key types:

- **`Envelope`** — Contains `TypeId`, `dyn TypedBody`, `DecoderId`, event metadata
- **`TypedBody` trait** — `Any + Send + Sync` with type erasure, allows `downcast_ref::<T>()`
- **`EventBody<T>`** — Generic over message type `T`, includes context fields
- **`TypeId`** — String identifier used for routing envelopes to sinks
- **`EventMsg` trait** — Messages implement this to provide `event_id()` and `envelope_type_id()`

### 3.4 Sink (`src/etl/sink/mod.rs`)

Consumes typed envelopes and writes to output. Key types:

- **`Sink` trait** — `process(envelopes)`, `interested_types() → &[TypeId]`, `build_routes()`
- **`MultiSink`** — Dispatches envelopes to registered sinks by `TypeId`
- **Three sink output modes**: EventBus (gRPC streaming), HTTP (Axum routes), gRPC (tonic services)
- **`EventBus`** — Pub/sub for gRPC streaming; sinks publish, subscribers consume topics

---

## 4. Key Subsystems In Detail

### 4.1 Introspect Events (`crates/introspect/src/events.rs`)

The `IntrospectMsg` enum is the core message type. It has **14 variants** for all schema/data operations:

```
IntrospectMsg
├── None
├── CreateTable(CreateTable)      — New table with complete schema
├── UpdateTable(UpdateTable)      — Full schema update
├── RenameTable(RenameTable)      — Rename
├── RenamePrimary(RenamePrimary)  — Rename primary key
├── RetypePrimary(RetypePrimary)  — Change primary key type
├── RenameColumns(RenameColumns)  — Rename columns
├── RetypeColumns(RetypeColumns)  — Change column types
├── AddColumns(AddColumns)        — Add new columns
├── DropTable(DropTable)          — Remove table
├── DropColumns(DropColumns)      — Remove columns
├── InsertsFields(InsertsFields)  — Data insertion (the hot path)
├── DeleteRecords(DeleteRecords)  — Row deletion
└── DeletesFields(DeletesFields)  — Column value deletion
```

**`InsertsFields`** is the most performance-critical variant:
```rust
pub struct InsertsFields {
    pub table: Felt,           // Target table ID
    pub columns: Vec<Felt>,    // Column selectors
    pub records: Vec<Record>,  // Batch of records
}

pub struct Record {
    pub id: [u8; 32],          // Primary key value (BE bytes)
    pub values: Vec<u8>,       // Cairo-encoded binary column values
}
```

The `EnumFrom` derive macro (from `introspect-rust-macros`) auto-generates `From<Variant> for IntrospectMsg` for each variant. This was previously done with a custom `enum_from_variants!` declarative macro, but has since been replaced with the derive macro.

### 4.2 Schema & Serialization (`crates/introspect/src/tables.rs`)

This file contains the core serialization pipeline for converting Cairo binary records to JSON:

#### `RecordSchema<'a>`
Holds references to a primary key definition and an ordered list of column definitions. Created by table types (e.g., `PgTable::get_schema()`) when processing records.

```rust
pub struct RecordSchema<'a> {
    primary: &'a PrimaryDef,
    columns: Vec<&'a ColumnDef>,
}
```

#### `RecordFrame<'a, C, M>`
The serializable unit — bundles a single record's data with its schema, a Cairo serialization strategy, and optional metadata:

```rust
pub struct RecordFrame<'a, C: CairoSerialization, M: SerializeEntries> {
    primary: &'a PrimaryDef,
    columns: &'a [&'a ColumnDef],
    id: &'a [u8; 32],         // Primary key bytes
    values: &'a [u8],         // Record value bytes
    cairo_se: &'a C,          // Serialization strategy
    metadata: &'a M,          // Extra entries injected into output
}
```

When `RecordFrame` is serialized (via `serde::Serialize`), it:
1. Creates a serde map
2. Deserializes the primary key bytes using `CairoSeFrom` and emits as a key-value entry
3. Deserializes each column's bytes using `CairoSeFrom` and emits entries
4. Delegates to `metadata.serialize_entries()` for any extra fields

This produces a flat JSON object per record.

#### `SerializeEntries` trait
Allows injecting extra key-value entries into a serde map during serialization:

```rust
pub trait SerializeEntries {
    fn entry_count(&self) -> usize;
    fn serialize_entries<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error>;
}
```

Implemented for:
- `()` — no-op (zero extra entries)
- `CairoSeFrom<..>` — deserializes columns and emits as map entries
- `RecordFrame<..>` — emits primary + columns + metadata entries

#### `AsEntryPair` trait
Extracts a key-value pair from a type, used by `CairoSeFrom`'s `SerializeEntries` impl to iterate schema items:

```rust
pub trait AsEntryPair {
    type Key;   // e.g., String (column name)
    type Value;  // e.g., TypeDef (column type)
    fn to_entry_pair(&self) -> (&Self::Key, &Self::Value);
}
```

### 4.3 CairoSeFrom — The Zero-Copy Serializer (`introspect` repo)

Located at `/home/ben/cgg/introspect/crates/types/src/serialize.rs`.

```rust
pub struct CairoSeFrom<'a, 'de, T, D: CairoDeserializer, C: CairoSerialization> {
    schema: &'a T,                    // Schema to drive serialization
    de: &'de RefCell<&'de mut D>,     // Cairo byte deserializer (mutable, shared)
    cairo_se: &'a C,                  // Serialization strategy
}
```

**How it works**: When serde asks this struct to `Serialize`, it consumes bytes from the deserializer `D` according to schema `T`, and serializes them via strategy `C`. This means **deserialization and serialization happen simultaneously** — no intermediate representation.

**Type parameters**:
- `'a` — Lifetime of schema and cairo_se borrows
- `'de` — Lifetime of the `RefCell` and its mutable deserializer
- `T` — Schema type (drives which Cairo types to decode)
- `D` — Byte source / deserializer (e.g., `DerefBytesSource<&[u8]>`)
- `C` — Serialization strategy (e.g., "serialize Felt as hex string")

**Why two lifetimes**: `RefCell<&'de mut D>` is **invariant** over `'de`. With a single lifetime `'a`, the compiler couldn't shorten the `RefCell` borrow's lifetime independently from the schema borrow. Splitting to `'a, 'de` allows functions to create local `RefCell`s with `for<'de>` higher-ranked trait bounds.

### 4.4 Byte Source System (`introspect` repo)

Located at `/home/ben/cgg/introspect/crates/types/src/bytes.rs`.

```rust
trait ByteSource {
    fn next(&mut self) -> Option<u8>;
    fn nexts<const N: usize>(&mut self) -> DecodeResult<[u8; N]>;
    fn position(&self) -> usize;
}

trait IntoByteSource {
    type Source: ByteSource;
    fn into_source(self) -> Self::Source;
}
```

`IntoByteSource` is implemented for `&[u8]`, `Vec<u8>`, `Arc<[u8]>`, and `&mut S: ByteSource`. The primary implementation is `DerefBytesSource<B>` which wraps any `Deref<Target=[u8]>`.

### 4.5 Dojo Decoder (`crates/dojo/src/decoder.rs`)

Translates Dojo World contract events into `IntrospectMsg` variants:

```rust
pub struct DojoDecoder<M, F> {
    manager: M,     // DojoTableManager (holds table registry)
    fetcher: F,     // Schema fetcher (RPC calls)
    primary_field: String,
}
```

**Event mapping**:
| Dojo Event | → IntrospectMsg |
|------------|-----------------|
| `ModelRegistered` | `CreateTable` |
| `ModelWithSchemaRegistered` | `CreateTable` |
| `EventRegistered` | `CreateTable` |
| `ModelUpgraded` / `EventUpgraded` | `UpdateTable` |
| `StoreSetRecord` | `InsertsFields` |
| `StoreUpdateRecord` | `InsertsFields` |
| `StoreUpdateMember` | `InsertsFields` |
| `EventEmitted` | `InsertsFields` |
| `StoreDelRecord` | `DeleteRecords` |

**`DojoEventProcessor<M, F>` trait**: Each Dojo event type implements this trait with a `process` method. The decoder dispatches on the event's selector (first key) to the appropriate processor.

### 4.6 Dojo Table (`crates/dojo/src/table.rs`)

```rust
pub struct DojoTable {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnDef>,
    pub key_fields: Vec<Felt>,     // Fields with "key" attribute
    pub value_fields: Vec<Felt>,   // Non-key fields
    pub legacy: bool,
}
```

**Key methods**:
- `parse_keys(keys: Vec<Felt>) → Vec<u8>` — Transcodes Cairo key felts into binary
- `parse_values(values: Vec<Felt>) → (Vec<Felt>, Vec<u8>)` — Returns column selectors + binary values
- `parse_record(keys, values) → (Vec<Felt>, Vec<u8>)` — Combined key+value transcoding
- `parse_field(selector, data) → Vec<u8>` — Single field transcoding

Uses a **`Transcode` trait** to convert from Cairo `Vec<Felt>` to `Vec<u8>` binary representation.

### 4.7 Table Manager (`crates/dojo/src/manager.rs` + `crates/introspect/src/manager.rs`)

Thread-safe table registry using `RwLock`:

```rust
pub struct DojoManagerInner<Store> {
    pub tables: HashMap<Felt, RwLock<DojoTable>>,
    pub store: Store,
}

pub struct DojoTableStore<Store>(RwLock<DojoManagerInner<Store>>);
```

**`StoreTrait`** abstracts persistence:
```rust
pub trait StoreTrait {
    type Table;
    fn dump(&self, table_id: Felt, data: &Self::Table) -> Result<()>;
    fn load(&self, table_id: Felt) -> Result<Self::Table>;
    fn load_all(&self) -> Result<Vec<(Felt, Self::Table)>>;
}
```

**`JsonStore`** is the default — stores table schemas as `{felt_id}.json` files on disk.

### 4.8 PostgreSQL Sink (`crates/introspect-postgres-sink/`)

#### `PgTable` (`table.rs`)
Manages a single table's PostgreSQL representation:

```rust
pub struct PgTable {
    pub name: String,
    pub postgres: PgTableStructure,  // DDL types, struct defs, enum defs
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnDef>,
    pub order: Vec<Felt>,
    pub alive: bool,
}
```

- `PgTable::new()` generates DDL via `PgTableStructure::new()`, appending CREATE TABLE + type statements to a `&mut Vec<String>`
- `PgTable::get_schema()` creates a `RecordSchema` for record serialization
- Design decision: Queries are collected as `Vec<String>` rather than executed immediately — improves testability and separation of concerns

#### `PostgresSchema` (`processor.rs`)
Manages the full set of tables for a namespace:

```rust
pub struct PostgresSchema {
    namespace: Option<String>,
    tables: HashMap<Felt, PgTable>,
}
```

**Insert strategy** (the hot path):
```sql
INSERT INTO table_name
SELECT * FROM jsonb_populate_recordset(NULL::table_name, $1::jsonb)
ON CONFLICT (primary_key) DO UPDATE SET
  col1 = COALESCE(EXCLUDED."col1", "table_name"."col1"),
  col2 = COALESCE(EXCLUDED."col2", "table_name"."col2"),
  ...
```

- `jsonb_populate_recordset` enables batch inserts from a JSON array
- `COALESCE(EXCLUDED."col", "table"."col")` preserves existing values during partial updates (when a record has only some columns populated)

---

## 5. Issues Resolved During Development

### 5.1 RecordSchema Lifetime Bug

**Problem**: `get_record_schema()` created a local `Arc<Table>`, borrowed it into `RecordSchema<'a>`, and returned the `RecordSchema`. The borrow outlived the local `Arc`, causing a dangling reference.

**Fix**: Changed `RecordSchema` to own the `Arc<Table>` and validated column IDs upfront, with accessor methods instead of direct field access.

**Current state**: `RecordSchema` was later simplified back to borrowing (`&'a PrimaryDef`, `Vec<&'a ColumnDef>`) since the calling code now ensures the table outlives the schema via `PgTable::get_schema(&self, ...)`.

### 5.2 Duplicate Async/Sync Decoder Traits

**Problem**: Two nearly-identical traits existed: `DojoEventAsyncDecoder` (async) and `DojoEventDecoder` (sync). This caused code duplication.

**Fix**: Merged into a single `DojoEventProcessor` async trait. Sync implementations simply don't `.await`. The call site (`decode_event`) is already async, so this was straightforward.

### 5.3 Broken Orphan `to_introspect_msg` Functions

**Problem**: Free functions like `fn to_introspect_msg(self) -> IntrospectMsg` existed outside any `impl` block, trying to use `self` without a receiver.

**Fix**: Replaced with auto-generated `From<Variant> for IntrospectMsg` impls. Originally used a custom `enum_from_variants!` declarative macro, now uses `#[derive(EnumFrom)]` from the `introspect-rust-macros` crate. Decoder code uses `IntrospectMsg::from(variant)` instead.

### 5.4 CairoSeFrom Invariance Trap (RefCell Lifetime)

**Problem**: `CairoSeFrom<'a, T, D, C>` had a single lifetime `'a`. When `RefCell<&'a mut D>` was used, `RefCell` is **invariant** over its type parameter. This meant `'a` couldn't be shortened by the compiler, preventing local `RefCell` creation (local variables can't satisfy `'a` from an outer scope).

**Root cause**: With one lifetime, schema borrows (`'a`) and the `RefCell` borrow (`'a`) were tied together. The `RefCell`'s invariance "infected" the entire struct, preventing normal lifetime shortening.

**Fix**: Split into two lifetimes:
```rust
// Before:
pub struct CairoSeFrom<'a, T, D, C> {
    schema: &'a T,
    de: &'a RefCell<&'a mut D>,  // Invariant over 'a!
    cairo_se: &'a C,
}

// After:
pub struct CairoSeFrom<'a, 'de, T, D, C> {
    schema: &'a T,
    de: &'de RefCell<&'de mut D>,  // Invariant over 'de, independent of 'a
    cairo_se: &'a C,
}
```

Functions that create local `RefCell`s use `for<'de>` higher-ranked trait bounds to work with any `'de`.

### 5.5 CairoFrame Local RefCell Issue

**Problem**: A `CairoFrame` struct stored `data: B` (raw bytes). Its `SerializeEntries` impl created a local `RefCell::new(&mut source)` inside `serialize_entries()`. The local couldn't satisfy the lifetime needed by `CairoSeFrom`.

**Fix**: Changed `CairoFrame` to store `RefCell<D>` directly, so `serialize_entries()` just borrows from the struct field (whose lifetime is `'a`) rather than creating a local.

**Note**: `CairoFrame` may have been refactored further or removed — the current `tables.rs` uses `RecordFrame` instead, which creates local `RefCell`s in `parse_primary_as_entry` and `parse_record_entries` but these work because they produce values consumed within the same function scope.

### 5.6 Runtime Deserialization Bug (Partially Resolved)

**Problem**: `JsonError("unexpected end of input")` when processing a `StoreSetRecord` event for a table named `blob_arena-Attack` with complex nested types (arrays of structs containing enums).

**Symptoms**: The Cairo binary data ran out of bytes before deserialization completed, suggesting a mismatch between the schema's expected layout and the actual binary encoding.

**Possible causes**:
1. Incorrect byte count in the binary `Record.values` field
2. Array type consuming too many or too few bytes during deserialization
3. Enum variant deserialization reading wrong number of bytes for a variant
4. Schema definition not matching the actual contract's layout

**Test data location**: `/home/ben/tc-tests/manager/0x7e21e142953aef1863c9e0b51413ed1e2de11b6b45238d13567868cbdd81850.json` (the table's schema JSON)

**Status**: ⚠️ Partially diagnosed, not fully resolved.

---

## 6. Important Design Patterns

### 6.1 Simultaneous Deserialize-Serialize (CairoSeFrom)
Instead of: `Cairo bytes → intermediate Rust types → serde JSON`, the system does:
`Cairo bytes → [CairoSeFrom drives both sides] → JSON directly`.

`CairoSeFrom` implements both `serde::Serialize` and `CairoDeserialization<D>`. When serde asks it to serialize, it reads from the byte source and produces serde output in one pass. No intermediate representation exists.

### 6.2 Schema-Driven Type Dispatch
The `TypeDef` enum (from the introspect crate) drives how bytes are consumed and serialized. `CairoSeFrom`'s `Serialize` impl pattern-matches on the schema type to decide how many bytes to read and what format to emit.

### 6.3 Query Collection Pattern (PostgreSQL)
DDL/DML statements are collected into `Vec<String>` rather than executed immediately. This allows:
- Testing SQL generation without a database connection
- Batching multiple statements in a single transaction
- Separating schema generation from execution

### 6.4 Envelope Type Erasure + Downcast
Envelopes use `dyn TypedBody` (which is `Any + Send + Sync`) so the ETL pipeline can transport any message type. Sinks use `envelope.downcast_ref::<IntrospectMsg>()` to recover the concrete type.

### 6.5 Multi-Decoder Pattern
One blockchain event can be processed by multiple decoders. For example, a token transfer event might be handled by both an ERC-20 decoder and a Dojo world decoder, producing separate typed envelopes for different sinks.

### 6.6 ExtractionBatch Deduplication
Blocks and transactions are stored in `HashMap`s by hash. Events reference them by hash rather than embedding copies. This eliminates duplicated context data when many events share the same block/transaction.

---

## 7. Key Type Relationships

```
IntoByteSource → ByteSource → CairoDeserializer
                                      ↓
                              CairoDeserialization<D>
                                      ↓
                    CairoSeFrom<'a, 'de, T, D, C>
                    ├── impl Serialize (drives output)
                    ├── impl CairoDeserialization<D> (consumes bytes)
                    └── impl SerializeEntries (emits map entries)
                                      ↓
                              RecordFrame<'a, C, M>
                              ├── impl Serialize
                              ├── impl SerializeEntries
                              └── uses CairoSeFrom internally
                                      ↓
                              RecordSchema<'a>
                              ├── to_frame() → RecordFrame
                              └── parse_records_with_metadata()
```

---

## 8. Data Flow Example (StoreSetRecord)

1. **Extractor** pulls Starknet block, finds a `StoreSetRecord` event
2. **Decoder** (`DojoDecoder`) matches the event selector
3. `StoreSetRecord` processor:
   - Extracts `entity_id`, `keys`, `values` from event data
   - Uses `DojoTable::parse_record(keys, values)` to transcode Cairo felts → binary bytes
   - Produces `InsertsFields { table: felt_id, columns: [selector1, selector2, ...], records: [Record { id, values }] }`
   - Wraps in `IntrospectMsg::from(inserts_fields)` → `EventBody<IntrospectMsg>` → `Envelope`
4. **Sink** (`PostgresSink`) receives the envelope:
   - Downcasts to `IntrospectMsg::InsertsFields`
   - Looks up `PgTable` by table felt ID
   - Calls `PgTable::get_schema(column_ids)` → `RecordSchema`
   - Creates `RecordFrame` with schema + record + metadata + cairo serializer
   - Serializes `RecordFrame` to JSON (simultaneously deserializing Cairo bytes)
   - Generates upsert SQL with `jsonb_populate_recordset`
   - Executes against PostgreSQL

---

## 9. Build & Run

```bash
# Build everything
cargo build --workspace

# Run the ERC-20 indexer binary
cargo run --bin torii-erc20

# Run the multi-token indexer
cargo run --bin torii-tokens

# Run tests
./scripts/test.sh

# Run lints
./scripts/lint.sh

# Run examples
cargo run --example simple_sql_sink
cargo run --example eventbus_only_sink
```

The workspace uses `resolver = "2"` and has extensive Clippy lint configuration in the root `Cargo.toml`.

---

## 10. Open Issues & Future Work

1. **Runtime deserialization bug** with complex nested types (arrays of structs with enums) — see §5.6
2. **SQLite sink** (`crates/introspect-sqlite-sink/`) appears to be work-in-progress
3. **Introspect manager module** is commented out in `crates/introspect/src/lib.rs`: `// pub mod manager;`
4. **Metadata injection** via `SerializeEntries` on `RecordFrame` is implemented but the infrastructure to populate metadata from upstream (decoder → envelope → sink) may need further work
5. **The introspect repo** (`/home/ben/cgg/introspect`) needs to be available at the expected path for workspace builds to succeed

---

## 11. Important Files Quick Reference

| File | What to look at |
|------|----------------|
| `src/etl/decoder/mod.rs` | Decoder trait, multi-decoder arch, ContractFilter |
| `src/etl/envelope.rs` | TypedBody, Envelope, TypeId routing |
| `src/etl/extractor/mod.rs` | ExtractionBatch deduplication |
| `src/etl/sink/mod.rs` | Sink trait, EventBus, three output modes |
| `crates/introspect/src/events.rs` | IntrospectMsg enum, Record struct |
| `crates/introspect/src/tables.rs` | RecordSchema, RecordFrame, SerializeEntries |
| `crates/dojo/src/decoder.rs` | DojoDecoder, event→IntrospectMsg mapping |
| `crates/dojo/src/table.rs` | DojoTable, Cairo→binary transcoding |
| `crates/introspect-postgres-sink/src/processor.rs` | PostgresSchema, upsert logic |
| `crates/introspect-postgres-sink/src/table.rs` | PgTable DDL generation |
| `/home/ben/cgg/introspect/crates/types/src/serialize.rs` | CairoSeFrom (critical) |
| `/home/ben/cgg/introspect/crates/types/src/bytes.rs` | ByteSource, IntoByteSource |
