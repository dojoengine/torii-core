# torii-erc1155

End-to-end **ERC1155 semi-fungible token indexer**. Decodes TransferSingle,
TransferBatch, ApprovalForAll, and URI events; stores transfer history,
operator approvals, and URI updates; serves a gRPC query/subscribe API.
**Balance tracking is intentionally not authoritative** — the sink writes
balance deltas but clients should query the chain for authoritative state
(see note below).

## Role in Torii

Same shape as `torii-erc20` / `torii-erc721` but for ERC1155. The
`Erc1155Rule` auto-identifier keys off `safeTransferFrom` + `balanceOf` +
`TransferSingle`. The sink emits three EventBus topics: `erc1155.transfer`,
`erc1155.metadata`, `erc1155.uri`.

## Architecture

```text
StarknetEvent (ERC1155 selector)
         |
         v
+-------------------------------+
|        Erc1155Decoder         |   identification::Erc1155Rule
|  TransferSingle | Batch |     |   (safeTransferFrom + balanceOf +
|  ApprovalForAll | URI         |    TransferSingle)
|  → Envelope(ERC1155_TYPE_ID)  |
+---------------+---------------+
                |
                v
+-------------------------------+
|         Erc1155Sink           |
|                               |
|  process():                   |
|    - persist transfers        |
|      (batch expanded to N rows|
|       one per (id,amount))    |
|    - update operator approval |
|    - URI update → enqueue     |
|      Erc1155TokenUriCommand   |
|    - balance delta tracked    |
|      in erc1155_balance       |
|      (reconciled on negative  |
|       via Erc1155BalanceFetcher
|       when enabled)           |
|    - broadcast only when      |
|      batch.is_live(100)       |
+---------------+---------------+
                |
                v
+-------------------------------+
|        Erc1155Storage         |
|  erc1155_transfer (cursor)    |
|  erc1155_operator_approval    |
|  erc1155_uri                  |
|  erc1155_balance              |
|  erc1155_balance_adjustment   |
|  erc1155_metadata             |
|  erc1155_token_uri            |
+---------------+---------------+
                |
                v
+-------------------------------+
|        Erc1155Service         |
|  gRPC: torii.sinks.erc1155    |
+-------------------------------+
```

## Deep Dive

### Public API

| Item | File | Purpose |
|---|---|---|
| `Erc1155Decoder`, `Erc1155Message`, `Erc1155Body`, `ERC1155_TYPE_ID` | `src/decoder.rs` | Decoder + event enum + wrapper body |
| `TransferData`, `OperatorApproval`, `UriUpdate` | `src/decoder.rs` | Typed event payloads |
| `Erc1155Rule` | `src/identification.rs` | ABI-based identification |
| `Erc1155Sink` | `src/sink.rs` | `Sink` impl |
| `Erc1155Storage`, `TokenTransferData`, `TokenUriData`, `Erc1155BalanceData`, `Erc1155BalanceAdjustment`, `TransferCursor` | `src/storage.rs` | Persistence layer |
| `Erc1155Service` | `src/grpc_service.rs` | gRPC service |
| `Erc1155BalanceFetcher`, `Erc1155BalanceFetchRequest` | `src/balance_fetcher.rs` | On-chain balance reconciliation |
| `Erc1155MetadataCommandHandler`, `Erc1155TokenUriCommandHandler` | `src/handlers.rs` | Async command handlers |
| `SyntheticErc1155Config`, `SyntheticErc1155Extractor` | `src/synthetic.rs` | Deterministic generator |
| `FILE_DESCRIPTOR_SET` | `src/lib.rs:55` | Descriptor bytes |

### Internal Modules

- `decoder` — matches ERC1155 selectors, **expands `TransferBatch` into N transfers** (one per `(id, amount)` pair) so downstream code treats single and batch identically.
- `identification` — requires `safeTransferFrom`, `balanceOf`, `TransferSingle`.
- `sink` — processes envelopes; dispatches metadata + token-URI commands.
- `storage` — transfer history + balance + URI tables.
- `balance_fetcher` — optional reconciliation path using `JsonRpcClient`.
- `handlers` — two `CommandHandler`s parallel to ERC721.
- `grpc_service` — pagination + broadcast channel.
- `synthetic` — `SyntheticErc1155Extractor`.

### Sink trait wiring

| Method | Behavior |
|---|---|
| `name` | `"erc1155"` |
| `interested_types` | `[ERC1155_TYPE_ID]` |
| `process` | Persists expanded transfers, updates operator-approval bitmap, stores URI updates, updates balance delta, broadcasts when `batch.is_live(100)` |
| `topics` | 3 topics: `erc1155.transfer` (filters: `token`, `from`, `to`, `wallet`), `erc1155.metadata` (filter: `token`), `erc1155.uri` (filters: `token`, `token_id`) |
| `build_routes` | `Router::new()` — gRPC only |
| `initialize` | Captures `event_bus` + `ctx.command_bus` |

### Storage note

ERC1155 maintains `erc1155_balance (token, account, token_id, amount)` and
an `erc1155_balance_adjustment` audit table, but the lib docs (`src/lib.rs:3`) state:

> Like ERC20, we only track transfer history - NOT balances. Clients should
> query the chain for actual balances to avoid inaccuracies from genesis
> allocations.

Treat the balance tables as a **best-effort cache** that the optional
`Erc1155BalanceFetcher` reconciles on divergence; authoritative balance
queries belong on-chain.

### Interactions

- **Upstream (consumers)**: `bins/torii-tokens`, `bins/torii-tokens-synth`, `bins/torii-introspect-bin`, `bins/torii-arcade`.
- **Downstream deps**: `torii`, `torii-common`, `torii-types`, `sqlx`, `tonic`, `prost`, `starknet`, `starknet-types-raw`, `primitive-types`, `tokio`, `async-trait`, `chrono`, `hex`, `metrics`, `tracing`.

### Extension Points

- TransferBatch-level aggregation → add a `TokenBatchTransferData` alongside expanded rows.
- Additional filters → extend the `TopicInfo.available_filters` lists and the matching `matches_*_filters` helper in `sink.rs`.
- Flip balance tracking from best-effort to authoritative → wire a continuous reconciler through `Erc1155BalanceFetcher` + CommandBus; keep the note in `lib.rs` in sync.
