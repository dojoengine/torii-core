# torii-types

The lowest-level shared-types crate. Holds the three primitives that every
decoder, extractor, and sink agrees on: `StarknetEvent`, `EventContext`, and
`BlockContext`. Nothing here is async; nothing here does I/O.

## Role in Torii

`StarknetEvent` is the atomic unit produced by every `Extractor` and consumed
by every `Decoder`. `EventContext` is the identity of an event
(`from_address` + `block_number` + `tx_hash`) and is what decoders receive in
`Decoder::decode(keys, data, context)`. `BlockContext` is the dedup-key
value in `ExtractionBatch.blocks`. Because every other Torii crate depends
on the root `torii` crate, this crate is implicitly on every compile path —
keeping it tiny and dependency-free is the point.

## Architecture

```text
+-----------------------------------------------------------+
|                         torii-types                        |
|                                                            |
|  event.rs                    block.rs                      |
|  +-------------------+       +-----------------------+     |
|  | StarknetEvent     |       | BlockContext          |     |
|  |   from_address    |       |   number              |     |
|  |   keys            |       |   hash                |     |
|  |   data            |       |   parent_hash         |     |
|  |   block_number    |       |   timestamp           |     |
|  |   transaction_hash|       +-----------------------+     |
|  +-------------------+                                     |
|       |                                                    |
|       v                                                    |
|  EventContext  <-- .context()                              |
|    from_address | block_number | transaction_hash          |
|                                                            |
|  (optional) feature "field": TryFrom<starknet::EmittedEvent|
+-----------------------------------------------------------+
          ^                 ^                 ^
          |                 |                 |
   Extractor impls    Decoder trait     ExtractionBatch
   (rpc, synthetic)   (DecoderContext)  (src/etl/extractor)
```

## Deep Dive

### Public API

| Type | File | Line | Purpose |
|---|---|---|---|
| `StarknetEvent` | `src/event.rs` | 4 | `from_address` + `keys` + `data` + `block_number` + `transaction_hash` |
| `StarknetEvent::context` | `src/event.rs` | 75 | Project event identity into an `EventContext` |
| `StarknetEvent::filter_pending` | `src/event.rs` | 83 | Collect items implementing `TryInto<StarknetEvent>`, dropping failures (typically events without a block number) |
| `EventContext` | `src/event.rs` | 13 | Copy-hashable identity used as a key in decoder code |
| `MissingBlockNumber` | `src/event.rs` | 19 | Error returned when converting from `starknet::core::types::EmittedEvent` without a confirmed block |
| `BlockContext` | `src/block.rs` | 5 | Dedupe-key for `ExtractionBatch.blocks` |

### Internal Modules

- `event` — `StarknetEvent`, `EventContext`, `MissingBlockNumber`, optional `feature = "field"` conversion from `starknet::core::types::EmittedEvent`.
- `block` — one struct.

### Interactions

- **Upstream (consumers)**: the root `torii` crate re-exports both types from `src/etl/mod.rs:18` and `src/etl/extractor/mod.rs:33`. All sinks/decoders use them through that re-export.
- **Downstream deps**: `starknet-types-raw`; optional `starknet` (feature `field`) for the `EmittedEvent` conversion.
- **Feature flags**: `field` (opt-in) — adds the `TryFrom<starknet::core::types::EmittedEvent>` impl. Enabled by the root crate via `torii-types = { workspace = true, features = ["field"] }`.

### Extension Points

- Anything cross-cutting that isn’t a `StarknetEvent` or a `BlockContext` does **not** belong here — keep it pure. Transaction / class / deployment contexts live in `src/etl/extractor/mod.rs` with the rest of the batch types.
