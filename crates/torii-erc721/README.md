# torii-erc721

End-to-end **ERC721 NFT indexer**. Decodes Transfer / Approval /
ApprovalForAll / MetadataUpdate / BatchMetadataUpdate (EIP-4906) events,
tracks per-token ownership in a dedicated ownership table, maintains
token-URI metadata out of band, and exposes a gRPC service for queries and
live updates.

## Role in Torii

Mirror of `torii-erc20` but for non-fungible tokens. The big shape
differences: balance tracking is replaced with an explicit `ownership`
row per `(token, token_id)`, metadata has a richer lifecycle because
individual tokens can change URI, and the `Erc721Rule` uses the presence
of `owner_of` (not `balance_of`) to distinguish ERC721 from ERC20.

## Architecture

```text
StarknetEvent (ERC721 selector)
        |
        v
+--------------------------------+
|         Erc721Decoder          |   identification::Erc721Rule
|  Transfer | Approval |         |   (ABI needs owner_of + Transfer evt)
|  ApprovalForAll |              |
|  MetadataUpdate |              |
|  BatchMetadataUpdate           |
|  → Envelope(ERC721_TYPE)       |
+---------------+----------------+
                |
                v
+--------------------------------+
|           Erc721Sink           |
|                                |
|  process():                    |
|    - store transfer / approval |
|    - upsert ownership          |
|    - MetadataUpdate → enqueue  |
|      Erc721TokenUriCommand     |
|      via command bus           |
|    - if batch.is_live(100):    |
|       EventBus publish         |
|       gRPC broadcast           |
+---------------+----------------+
                |
                v
+--------------------------------+
|        Erc721Storage           |
|  erc721_transfer (cursor)      |
|  erc721_ownership              |
|  erc721_approval               |
|  erc721_operator_approval      |
|  erc721_metadata               |
|  erc721_token_uri              |
+---------------+----------------+
                |
                v
+--------------------------------+
|         Erc721Service          |
|  gRPC: torii.sinks.erc721      |
+--------------------------------+
                ^
                |
+--------------------------------+
|  Erc721MetadataCommandHandler  |  <- CommandBus
|  Erc721TokenUriCommandHandler  |     (name/symbol,
|  (uses torii-common::          |      token URIs,
|   MetadataFetcher +            |      per-token meta)
|   TokenUriService)             |
+--------------------------------+
```

## Deep Dive

### Public API

| Item | File | Purpose |
|---|---|---|
| `Erc721Decoder`, `Erc721Msg`, `Erc721Body`, `ERC721_TYPE` | `src/decoder.rs` | Decoder + event enum + wrapper body |
| `NftTransfer`, `NftApproval`, `OperatorApproval` | `src/decoder.rs` | Transfer + per-token Approval + ApprovalForAll |
| `MetadataUpdate`, `BatchMetadataUpdate` | `src/decoder.rs` | EIP-4906 metadata invalidation events |
| `Erc721Rule` | `src/identification.rs` | ABI-based identification (requires `owner_of`) |
| `Erc721Sink` | `src/sink.rs` | `Sink` impl with `with_grpc_service`, `with_token_uri_pipeline`, etc. |
| `Erc721Storage`, `NftOwnershipData`, `NftTransferData`, `TransferCursor` | `src/storage.rs` | Persistence + ownership |
| `Erc721Service` | `src/grpc_service.rs` | tonic service |
| `Erc721MetadataCommandHandler`, `Erc721TokenUriCommandHandler` | `src/handlers.rs` | `CommandHandler` implementations |
| `SyntheticErc721Config`, `SyntheticErc721Extractor` | `src/synthetic.rs` | Deterministic generator |
| `FILE_DESCRIPTOR_SET` | `src/lib.rs:53` | Descriptor bytes |

### Internal Modules

- `decoder` — matches Transfer / Approval / ApprovalForAll / MetadataUpdate / BatchMetadataUpdate. Token IDs are `U256`, stored via `torii-common::U256Blob`.
- `identification` — `Erc721Rule`: requires `owner_of` + `balance_of` + `Transfer` event. Keeps ERC721 and ERC20 distinct under auto-identification (ERC20 has `balance_of` but no `owner_of`).
- `sink` — writes the row, updates ownership, dispatches metadata + token-URI commands, broadcasts when live.
- `storage` — `erc721_transfer`, `erc721_ownership`, `erc721_approval`, `erc721_operator_approval`, `erc721_metadata`, `erc721_token_uri`.
- `grpc_service` — paginated queries + broadcast channel.
- `handlers` — two `CommandHandler`s: contract-level metadata + per-token URI.
- `synthetic` — `SyntheticErc721Extractor` implementing `SyntheticExtractor`.
- `conversions` — proto ↔ storage conversions.

### Sink trait wiring

| Method | Behavior |
|---|---|
| `name` | `"erc721"` |
| `interested_types` | `[ERC721_TYPE]` (one `TypeId::new("erc721")`) |
| `process` | Persist transfer + upsert ownership + optional approval row; dispatch metadata/token-URI commands; broadcast live updates if `batch.is_live(100)` |
| `topics` | 2 topics: `erc721.transfer` (filters: `token`, `from`, `to`, `wallet`) and `erc721.metadata` (filter: `token`) |
| `build_routes` | `Router::new()` — gRPC only |
| `initialize` | Captures `event_bus` + `ctx.command_bus` |

### Storage (key tables)

- `erc721_transfer (token, token_id, from, to, block_number, tx_hash, ..)` — event history
- `erc721_ownership (token, token_id, owner)` — one row per NFT; upserted on every Transfer
- `erc721_approval (token, token_id, owner, approved)` — per-token approvals (overwritten)
- `erc721_operator_approval (token, owner, operator, approved)` — ApprovalForAll bitmap
- `erc721_metadata (token, name, symbol, …)` — collection-level metadata
- `erc721_token_uri (token, token_id, uri, fetched_json)` — per-token URI + cached off-chain JSON (populated asynchronously via command handler)

All `Felt`/`U256` fields are BLOB encoded via `torii-common::{FeltBlob,U256Blob}`.

### Interactions

- **Upstream (consumers)**: `bins/torii-tokens`, `bins/torii-introspect-bin`, `bins/torii-arcade` (via `ArcadeService`), `bins/torii-tokens-synth`.
- **Downstream deps**: `torii`, `torii-common`, `torii-types`, `sqlx`, `tonic`, `prost`, `starknet`, `starknet-types-raw`, `primitive-types`, `tokio`, `async-trait`, `chrono`, `hex`, `metrics`, `tracing`.

### Extension Points

- Additional per-token metadata → add columns to `erc721_token_uri` + proto + `Erc721TokenUriCommandHandler`.
- New event (e.g. Pausable, EnumerableExt) → extend `Erc721Msg` + decoder branch + sink branch.
- Ownership history → add an `erc721_ownership_history` table; the sink already has the transfer insert site to hook into.
