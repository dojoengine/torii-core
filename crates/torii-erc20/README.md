# torii-erc20

End-to-end **ERC20 token indexer**. Ships a `Decoder` for Transfer / Approval
events, an `IdentificationRule` for ABI-based auto-discovery, a `Sink` that
persists the history + maintains balances with on-chain reconciliation,
and a gRPC service that answers queries and pushes live updates. A paired
synthetic extractor makes reproducible profiling workloads possible
without a live RPC endpoint.

## Role in Torii

Slotted into any binary as (a) an auto-identification rule registered with
the `ContractRegistry` so Torii learns which contracts are ERC20s, (b) the
`Erc20Decoder` that turns Transfer/Approval selectors into typed envelopes,
and (c) the `Erc20Sink` that writes the rows, keeps balances sane, and
serves the `torii.sinks.erc20.Erc20` gRPC service. Wires cleanly into
`torii::run` via `ToriiConfigBuilder::with_grpc_router` +
`with_contract_identifier`.

## Architecture

```text
StarknetEvent (Transfer/Approval selector)
        |
        v
+------------------------------+
|        Erc20Decoder          |   identification::Erc20Rule
|  Transfer|Approval → Envelope|   (ABI-based auto-discovery)
|  ERC20_TYPE_ID               |
+--------------+---------------+
               |
               v
+------------------------------+
|          Erc20Sink           |
|                              |
|  process():                  |
|    - store transfer/approval |
|    - compute balance delta   |
|    - detect negative         |
|      balance → enqueue       |
|      BalanceFetcher          |
|    - if batch.is_live(100):  |
|        EventBus publish      |
|        grpc broadcast        |
|    - dispatch                |
|      FetchErc20MetadataCmd   |
|      for unseen tokens       |
|  AtomicU64 counters          |
+--------------+---------------+
               |
               v
+------------------------------+
|         Erc20Storage         |
|                              |
|  erc20_transfer  (cursor PK) |
|  erc20_approval  (cursor PK) |
|  erc20_balance   (token, acc,|
|                   amount U256)|
|  erc20_balance_adjustment    |
|  erc20_metadata              |
|                              |
|  BLOB encoded via torii-     |
|  common::U256Blob / FeltBlob |
+--------------+---------------+
               |
               v
+------------------------------+
|         Erc20Service         |
|  gRPC: torii.sinks.erc20     |
|  cursor-paginated queries    |
|  broadcast subscriptions     |
+------------------------------+
               ^
               |
+------------------------------+
|   Erc20MetadataCommandHandler| <-- CommandBus
|   (fetches name/symbol/      |     FetchErc20MetadataCommand
|    decimals via JsonRpcClient|
|    + torii-common::Metadata- |
|    Fetcher)                  |
+------------------------------+

(optional) synthetic::SyntheticErc20Extractor feeds deterministic events
for benchmarking — impl SyntheticExtractor, fed via
ToriiConfigBuilder::with_synthetic_extractor
```

## Deep Dive

### Public API

| Item | File | Purpose |
|---|---|---|
| `Erc20Decoder`, `Transfer`, `Approval`, `Erc20Event`, `ERC20_TYPE_ID` | `src/decoder.rs` | Decoder impl + event variants + the `TypeId` sinks subscribe to |
| `Erc20Rule` | `src/identification.rs` | `IdentificationRule` — ABI must expose `transfer`, `balance_of`, and a `Transfer` event |
| `Erc20Sink` | `src/sink.rs` | `Sink` impl with fluent setters: `with_grpc_service`, `with_balance_tracking(provider)`, `with_metadata_pipeline(...)` |
| `Erc20Storage`, `TransferData`, `ApprovalData`, `BalanceData`, `BalanceAdjustment`, cursors | `src/storage.rs` | Persistence layer |
| `Erc20Service` | `src/grpc_service.rs` | gRPC queries + subscriptions |
| `BalanceFetcher`, `BalanceFetchRequest` | `src/balance_fetcher.rs` | Queued on-chain balance lookups (via JsonRpcClient) |
| `Erc20MetadataCommandHandler`, `FetchErc20MetadataCommand` | `src/handlers.rs` | `CommandHandler` that fetches name/symbol/decimals out of band |
| `SyntheticErc20Config`, `SyntheticErc20Extractor` | `src/synthetic.rs` | Deterministic generator for profiling |
| `FILE_DESCRIPTOR_SET` | `src/lib.rs:53` | Descriptor bytes for reflection |

### Internal Modules

- `decoder` — matches the Transfer/Approval selectors, parses the felt payload, attaches block+tx metadata to the envelope.
- `identification` — `Erc20Rule`: returns `[DecoderId::new("erc20")]` when the ABI has the three required items.
- `sink` — the core `Sink` impl (see "Sink trait wiring" below).
- `storage` — sqlx-based CRUD over `erc20_transfer` / `erc20_approval` / `erc20_balance` / `erc20_balance_adjustment` / `erc20_metadata`.
- `balance_fetcher` — fetches on-chain `balance_of` results to reconcile negative-balance anomalies (genesis, airdrop).
- `handlers` — async metadata pipeline; the sink dispatches `FetchErc20MetadataCommand`s that the handler processes off the hot path.
- `grpc_service` — tonic service; cursor-paginated queries + `tokio::sync::broadcast` live updates.
- `synthetic` — `SyntheticErc20Extractor` implementing `SyntheticExtractor`.

### Sink trait wiring

| Method | Behavior |
|---|---|
| `name` | `"erc20"` |
| `interested_types` | `[ERC20_TYPE_ID]` |
| `process(envelopes, batch)` | Persists rows; updates the `AtomicU64` counters; updates balance; on negative balance queues a `BalanceFetcher` request. **Broadcasts to EventBus + gRPC only when `batch.is_live(LIVE_THRESHOLD_BLOCKS=100)`** — historical indexing stays silent to avoid flooding clients. Dispatches a metadata command once per new token. |
| `topics` | 3 topics: `erc20.transfer` (filters: `token`, `from`, `to`, `wallet` — OR semantics for `wallet`), `erc20.approval` (filters: `token`, `owner`, `spender`, `account`), `erc20.metadata` (filter: `token`) |
| `build_routes` | `Router::new()` — gRPC only |
| `initialize` | Captures `event_bus` and `ctx.command_bus` |

### Storage

- Amounts stored via `torii_common::u256_to_bytes` → **variable-length big-endian BLOB** (zero is `[0]`, no leading-zero padding).
- Addresses via `FeltBlob`.
- Cursors: `(block_number, event_idx)` monotonic pagination keys.
- Balances: optimistic — computed delta, fetched and adjusted on inconsistency.
- Audit: `erc20_balance_adjustment` records every reconciled value for traceability.

### Interactions

- **Upstream (consumers)**: `bins/torii-erc20`, `bins/torii-erc20-synth`, `bins/torii-tokens`, `bins/torii-tokens-synth`, `bins/torii-introspect-bin`, `bins/torii-arcade` (any binary with ERC20 support).
- **Downstream deps**: `torii`, `torii-common`, `torii-types`, `sqlx`, `tonic`, `prost`, `starknet`, `starknet-types-raw`, `primitive-types`, `tokio`, `async-trait`, `chrono`, `hex`, `metrics`, `tracing`.

### Extension Points

- New event type → add a variant to `Erc20Event` + decoder branch + sink branch + proto message.
- New filter key → add to the relevant `TopicInfo.available_filters` + the matching `matches_*_filters` helper.
- New RPC → proto + `Erc20Service` + regenerate via `build.rs`.
- Different balance strategy → swap `BalanceFetcher` for a batched variant; `Erc20Sink::with_balance_tracking` is the only integration point.
- Benchmarking → compose `SyntheticErc20Extractor` with `ToriiConfigBuilder::with_synthetic_extractor` (see `bins/torii-erc20-synth`).
