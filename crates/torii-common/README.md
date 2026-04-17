# torii-common

Shared low-level utilities used by every Torii sink and decoder: compact U256
and Felt BLOB encodings, JSON helpers, token metadata fetching, and a
pluggable token-URI resolver. This crate has no knowledge of the ETL loop —
it is a pure toolbox.

## Role in Torii

`torii-common` is consumed everywhere amounts, addresses, or token metadata
need to move between the chain and the database. The token sinks
(`torii-erc20`, `torii-erc721`, `torii-erc1155`) store amounts and token IDs
as variable-length BLOBs via `U256Blob`, and fetch name/symbol/decimals from
on-chain views or off-chain metadata URIs via `MetadataFetcher` +
`TokenUriService`.

## Architecture

```text
+-----------------------------------------------------------+
|                       torii-common                        |
|                                                           |
|   src/lib.rs         src/metadata.rs      src/token_uri.rs|
|   +-----------+      +----------------+   +--------------+|
|   | U256Blob  |      | MetadataFetcher|   |TokenUriService|
|   | FeltBlob  |      | TokenMetadata  |   |TokenStandard  |
|   +-----------+      +----------------+   |TokenUriRequest|
|        ^                     ^            |TokenUriStore  |
|        |                     |            +--------------+|
|        |                     |                    ^       |
|        |                     |                    |       |
|   Sinks / Decoders <---------+--------------------+       |
|   (torii-erc20, torii-erc721, torii-erc1155)              |
+-----------------------------------------------------------+
```

## Deep Dive

### Public API

| Type / Fn | File | Line | Purpose |
|---|---|---|---|
| `U256Blob` | `src/lib.rs` | 22 | Compact big-endian BLOB codec for U256 (leading zeros stripped, zero → `[0]`) |
| `FeltBlob` | `src/lib.rs` | 103 | BLOB codec for `RawFelt` / `CoreFelt` |
| `u256_to_blob` / `blob_to_u256` | `src/lib.rs` | 72 / 80 | Generic helpers over `U256Blob` |
| `felt_to_blob` / `blob_to_felt` | `src/lib.rs` | 134 / 148 | Generic helpers over `FeltBlob` |
| `MetadataFetcher` | `src/metadata.rs` | — | Async HTTP + on-chain metadata enrichment |
| `TokenMetadata` | `src/metadata.rs` | — | Name / symbol / decimals / URI |
| `TokenUriService` | `src/token_uri.rs` | — | Resolves on-chain `token_uri` / `uri` to structured `TokenUriResult`s |
| `TokenStandard` | `src/token_uri.rs` | — | ERC20 / ERC721 / ERC1155 tag |
| `process_token_uri_request` | `src/token_uri.rs` | — | One-shot resolver used by metadata command handlers |

### Internal Modules

- `json` — small serde helpers (hex parsing, optional fields).
- `metadata` — async fetcher, retry policy, error types, unit-tested around HTTP mocking.
- `token_uri` — token-standard-aware URI resolution (`ipfs://`, `data:` URIs, on-chain strings).
- `utils` — miscellaneous helpers (hex printing, string normalization).

### Interactions

- **Upstream (consumers)**: `torii-erc20`, `torii-erc721`, `torii-erc1155`, `torii-dojo`, `arcade-sink`, `introspect-sql-sink`. The root crate re-exports `StarknetEvent` from `torii-types`, not from here.
- **Downstream deps**: `reqwest` (HTTP), `serde` / `serde_json`, `primitive-types`, `starknet` / `starknet-types-raw`, `tokio`.
- **Workspace deps**: none (pure library).

### Extension Points

- Adding a new token standard: extend `TokenStandard` in `src/token_uri.rs` and the corresponding branches of `process_token_uri_request`.
- New wire encoding: impl `U256Blob` or `FeltBlob` for a custom numeric type; the existing helpers become generic over it automatically.
- New metadata source: wrap `MetadataFetcher` or register a new `CommandHandler` in the binary (see `torii-erc20::FetchErc20MetadataCommand`).
