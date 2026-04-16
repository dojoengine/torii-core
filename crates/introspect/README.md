# torii-introspect

The **schema event vocabulary** shared between `torii-dojo` and every
introspect-aware sink. No decoding, no persistence — just the data types
that describe a table being created, renamed, retyped, repopulated, or
deleted at runtime.

## Role in Torii

`DojoDecoder` (`crates/dojo/src/decoder.rs`) takes Cairo-level Dojo events
and translates them into this crate's `IntrospectMsg` enum. The resulting
envelopes are then consumed by `introspect-sql-sink` (materialises the
tables to SQL), `torii-ecs-sink` (classifies as entity vs event-message),
and `arcade-sink` (projects into Arcade models). Because everybody agrees
on this vocabulary, the same event stream fans out to all three without
sharing any code.

## Architecture

```text
Cairo events from the Dojo world contract
         |
         v
  torii-dojo::DojoDecoder
         |
         v  IntrospectMsg (13 variants, see below)
         |
         +---> introspect-sql-sink  (DDL + records)
         +---> torii-ecs-sink       (entity/event-message classification)
         +---> arcade-sink          (Arcade projections)

+---------------------------------------------------------+
|                    IntrospectMsg enum                   |
|                                                         |
|  Schema lifecycle     Column lifecycle   Record lifecyc.|
|  - CreateTable        - AddColumns       - InsertsFields|
|  - UpdateTable        - RenameColumns    - DeleteRecords|
|  - RenameTable        - RetypeColumns    - DeletesFields|
|  - RenamePrimary      - DropColumns                    |
|  - RetypePrimary      - DropTable                      |
+---------------------------------------------------------+
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `IntrospectMsg` (13 variants) | `src/events.rs` | 16 | The union type every Dojo-aware sink pattern-matches |
| `IntrospectBody = EventBody<IntrospectMsg>` | `src/events.rs` | 32 | Convenience alias for the envelope body |
| `EventId` (trait) | `src/events.rs` | 34 | Every variant has a stable `event_id()` for dedup |
| `CreateTable` | `src/events.rs` | 69 | `id`, `name`, `attributes`, `primary`, `columns`, `append_only` |
| `UpdateTable` | `src/events.rs` | 79 | Same as `CreateTable` minus `append_only` |
| `RenameTable`, `RenamePrimary`, `RetypePrimary`, `RenameColumns`, `RetypeColumns`, `AddColumns`, `DropColumns`, `DropTable` | `src/events.rs` | 88–128 | Schema mutations |
| `InsertsFields`, `Record` | `src/events.rs` | 144 / 138 | Bulk insert of rows (row id + raw serialized values) |
| `DeleteRecords`, `DeletesFields` | `src/events.rs` | 151 / 157 | Row and field deletions |
| `ColumnKey` | `src/schema.rs` | — | Newtype wrapping column identity |
| `TableSchema` (`From<CreateTable>`, `From<UpdateTable>`) | `src/schema.rs` | — | Non-event-shaped view of a table (id + name + attrs + primary + columns) used internally |

### Internal Modules

- `events` — `IntrospectMsg` + 13 variant structs + their `EventId` impls + `EventMsg` impl (`TypeId::new("introspect")`).
- `schema` — `TableSchema` + `ColumnKey` newtype + conversions to/from `CreateTable`/`UpdateTable`.
- `tables` — multi-table cache helpers (used by the SQL sink).
- `types` — `TypeLibrary` trait for cairo type reference expansion.
- `store` — trait stub (no persistence lives here).
- `manager` — reserved for a future schema manager facade; currently inert.
- `postgres/` — PostgreSQL-specific type mappers; consumed by `introspect-sql-sink` when the `postgres` feature is on.

### Interactions

- **Upstream (consumers)**: `torii-dojo` (emits `IntrospectMsg` inside `DojoEvent::Introspect`), `introspect-sql-sink` (materialises), `torii-ecs-sink` (classifies), `arcade-sink` (projects).
- **Downstream deps**: `torii`, `torii-common`, `torii-sql`, `introspect-events`, `introspect-rust-macros`, `introspect-types`, `starknet-types-raw`, `starknet-types-core`, `serde`, `sqlx` (only when feature `postgres` is on).
- **Features**: `postgres` — pulls in `sqlx` and compiles the `postgres/` mappers.

### Extension Points

- New schema event → add a variant to `IntrospectMsg` + `EventId` impl + bump every matching sink. Because `IntrospectMsg` is an exhaustive enum, the compiler enforces the fan-out.
- New column attribute → extend `introspect-types::Attribute` (external crate); no change needed here.
- The type itself is ABI-stable on the wire — changing variant names or orderings breaks every sink.
