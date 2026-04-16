# torii-test-utils

Test-only helpers for working with captured Starknet event fixtures and
Dojo schemas. Kept out of `torii-common` because it pulls in `resolve-path`,
`alphanumeric-sort`, and `dojo-introspect` — weight no production binary
should carry.

## Role in Torii

Unit and integration tests across the workspace replay on-chain events from
JSON fixtures instead of hitting a real RPC. `EventIterator` turns a
directory of `events_*.json` batch files into an `Iterator<Item =
EmittedEvent>`; `MultiContractEventIterator` round-robins between several
such directories to mimic multi-contract workloads. `FakeProvider` gives
`torii-dojo` tests a `DojoSchemaFetcher` that reads schemas from disk instead
of performing a `starknet_getClass` RPC call.

## Architecture

```text
+---------------------------------------------------------+
|                    torii-test-utils                     |
|                                                         |
|  event_reader.rs                                        |
|  +---------------------+   Event (serde-compatible JSON)|
|  | EventBatch          |---+                            |
|  | EventIterator       |   |                            |
|  | MultiContractEvent- |   v                            |
|  |   Iterator          |  starknet_types_raw::          |
|  +---------------------+    EmittedEvent                |
|                                                         |
|  dojo.rs                                                |
|  +---------------------+   reads {contract:#066x}.json  |
|  | FakeProvider        |---> impls DojoSchemaFetcher    |
|  +---------------------+                                |
|                                                         |
|  utils.rs                                               |
|  +---------------------+                                |
|  | read_json_file      |   `~/` expansion via resolve_  |
|  | resolve_path_like   |   path                         |
|  +---------------------+                                |
+---------------------------------------------------------+
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `EventIterator` | `src/event_reader.rs` | 39 | Streams `EmittedEvent`s from a directory of batch JSON files (sorted alphanumerically) |
| `MultiContractEventIterator` | `src/event_reader.rs` | 48 | Round-robin merge of several `EventIterator`s |
| `EventBatch` / `Event` | `src/event_reader.rs` | 9 / 32 | Serde-compatible wire shapes used by fixture files |
| `FakeProvider` | `src/dojo.rs` | 11 | `DojoSchemaFetcher` impl that loads `{contract:#066x}.json` from disk |
| `read_json_file<T: DeserializeOwned>(path)` | `src/utils.rs` | — | Typed JSON loader used by both modules |
| `resolve_path_like` | `src/utils.rs` | — | Expands `~`-relative paths via `resolve-path` |

### Internal Modules

- `event_reader` — two iterators and their batch JSON schemas.
- `dojo` — a single `DojoSchemaFetcher` impl for test-time introspection.
- `utils` — JSON loader + `~/` path expander (the module is `pub` but the functions are re-exported at the crate root).

### Interactions

- **Upstream (consumers)**: tests and benches across the workspace; the root crate lists this in its `[dev-dependencies]` (`Cargo.toml:244`).
- **Downstream deps**: `dojo-introspect`, `introspect-types`, `starknet-types-core`, `starknet-types-raw`, `serde`, `serde_json`, `resolve-path`, `alphanumeric-sort`, `async-trait`.
- **Workspace deps**: none at runtime — it is only pulled in as a dev-dep.

### Extension Points

- New fixture shape: add a module, re-export from `lib.rs`, keep fixtures beside their tests rather than under this crate.
- Do **not** put production code here — anything in this crate's deps (e.g. `resolve-path`) will leak into binaries if they accidentally take a runtime dep.
