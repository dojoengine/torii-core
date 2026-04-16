# torii-pathfinder

Reads Starknet events directly from a **Pathfinder SQLite database**. Used
when indexing from an archival Pathfinder node is faster than calling
`starknet_getEvents` over RPC — typically for backfills. Under the `etl`
feature it also exposes an `Extractor` implementation so Torii can plug it
into the normal ETL loop.

## Role in Torii

Ordinary binaries read from an RPC endpoint via `BlockRangeExtractor` or
`EventExtractor` (`src/etl/extractor/`). For bulk historical ingests, where
going through RPC is prohibitively slow, this crate lets a binary point at
a local Pathfinder SQLite DB and stream events out directly — decoding the
binary event-column format Pathfinder uses (zstd-compressed bincode over
felt arrays).

## Architecture

```text
+------------------------------------------------------+
|                    torii-pathfinder                  |
|                                                      |
|  utils.rs          sqlite.rs                         |
|  +----------+      +-------------------------+       |
|  | connect()|----->| SqliteExt trait         |       |
|  | (rusqlite)      | - get_block_context_rows|       |
|  +----------+      | - get_block_events_rows |       |
|                    | - get_block_tx_hash_rows|       |
|                    +-------------------------+       |
|                               |                      |
|                               v                      |
|  decoding.rs                                         |
|  +----------------+  zstd-decompress +               |
|  | BlockEvents    |  bincode-decode  +               |
|  | BlockTxHashes  |  felt-array parsing              |
|  +----------------+                                  |
|                               |                      |
|                               v                      |
|  fetcher.rs                                          |
|  +---------------------------------+                 |
|  | EventFetcher (trait)            |                 |
|  |   get_events(from, to)          |                 |
|  |   get_events_with_context(...)  |                 |
|  | impl EventFetcher for Connection|                 |
|  +---------------------------------+                 |
|                               |                      |
|                               v  (feature = "etl")   |
|  extractor.rs                                        |
|  +---------------------------------+                 |
|  | PathfinderExtractor             |                 |
|  | PathfinderCombinedExtractor<T>  |--- impl Extractor
|  +---------------------------------+                 |
+------------------------------------------------------+
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `connect(path)` | `src/utils.rs` | — | Open a `rusqlite::Connection` with the read-only pragmas Pathfinder expects |
| `EventFetcher` trait | `src/fetcher.rs` | 19 | `get_events(from, to)` / `get_events_with_context(from, to)` — blanket impl for `rusqlite::Connection` |
| `BlockEvents` / `BlockTxHashes` | `src/decoding.rs` | — | Zstd-bincode decoders for Pathfinder's blob columns |
| `BlockContextRow` | `src/sqlite.rs` | — | Row shape for `block_headers` |
| `SqliteExt` trait | `src/sqlite.rs` | — | Raw SQL readers used by `EventFetcher` |
| `PathfinderExtractor` | `src/extractor.rs` (feature `etl`) | 8 | Stateful block-range extractor keeping a `Mutex<Connection>` |
| `PathfinderCombinedExtractor<T>` | `src/extractor.rs` (feature `etl`) | 17 | Switches from the Pathfinder DB to a live-head extractor `T` once caught up |
| `PFError` / `PFResult<T>` | `src/error.rs` | — | Domain error covering IO, SQLite, bincode, zstd, and invariant failures |

### Internal Modules

- `decoding` — zstd + bincode felt-array parsing.
- `error` — `PFError` (with typed constructors) and `PFResult<T>`.
- `fetcher` — `EventFetcher` trait and its blanket impl over `rusqlite::Connection`.
- `sqlite` — raw SQL extensions and row structs.
- `utils` — `connect` (read-only open).
- `extractor` (feature `etl`) — `PathfinderExtractor` + combined variant that swaps to a live extractor once the range is exhausted.

### Interactions

- **Upstream (consumers)**: `examples/pathfinder/main.rs` demonstrates usage; binaries that want Pathfinder backfill opt in by enabling the `etl` feature.
- **Downstream deps**: `rusqlite` (bundled), `zstd`, `bincode`, `serde`, `starknet-types-raw`, `torii-types`. With `etl`: `torii`, `async-trait`, `anyhow`.
- **Features**: `etl` — adds the `Extractor` implementation and the `torii` dep. Binaries that use only `EventFetcher` do not need it.

### Extension Points

- New snapshot source: add a module beside `sqlite` (e.g. `postgres.rs`) and implement `EventFetcher` for its connection. `PathfinderExtractor` becomes reusable because it consumes `EventFetcher` traits, not concrete types.
- New blob format: add a module under `decoding` and widen `BlockEvents::try_from(row)` to dispatch by column version.
