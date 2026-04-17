# torii-dojo

Decoder and type library for **Dojo ECS** worlds. Translates Cairo-level
events emitted by the `dojo::world::World` contract into typed
`IntrospectMsg` payloads that downstream sinks (`introspect-sql-sink`,
`torii-ecs-sink`, `arcade-sink`) materialise. Also owns the
`ExternalContractRegistered` event + a `CommandHandler` that wires runtime
contract registrations back into the `ContractRegistry`.

## Role in Torii

`DojoDecoder` is the *one* decoder every Dojo-aware binary registers. It
listens to the world contract for schema mutations (table create/update,
record inserts/deletes, event declarations) and emits `DojoEvent`
envelopes tagged with `DOJO_TYPE_ID`. Any sink that declares
`interested_types() == [DOJO_TYPE_ID]` gets the entire Dojo event stream.

The secondary responsibility — `ExternalContractRegistered` —
is how a live Dojo world advertises that a deployed contract should be
indexed as an ERC20/721/1155 token: the sink that consumes the event
dispatches a `RegisterExternalContractCommand` through the command bus;
the paired handler updates the `SharedDecoderRegistry` so the
`ContractRegistry` immediately starts routing that contract to the right
decoder.

## Architecture

```text
+--------------------------------+
|  StarknetEvent                 |
|  (from Dojo world contract)    |
+---------------+----------------+
                |
                v
+------------------------------------------+
|            DojoDecoder<Store, F>         |
|                                          |
|  async fn decode(keys, data, ctx):       |
|    select on event selector →            |
|    ModelRegistered, ModelWithSchemaReg-  |
|    istered, ModelUpgraded,               |
|    EventRegistered, EventUpgraded,       |
|    EventEmitted, StoreSetRecord,         |
|    StoreUpdateRecord, StoreUpdateMember, |
|    StoreDelRecord, ExternalContract      |
|    Registered                            |
|                                          |
|  for table events: look up / insert      |
|    DojoTableInfo in self.tables          |
|    (RwLock<HashMap<Felt, DojoTableInfo>>)|
|                                          |
|  for record events: translate via        |
|    DojoRecordEvent::event_to_msg()       |
|    into IntrospectMsg                    |
|                                          |
|  for schema-refresh: fetch schema via    |
|    DojoSchemaFetcher (F)                 |
+--------+---------------------------------+
         |                        |
         |                        |
         v                        v
+----------------+     +------------------------+
|  DojoEvent     |     | DojoEvent::External    |
|  ::Introspect  |     |   ContractRegistered   |
|  (IntrospectMsg|     | → EventBody<DojoEvent> |
|   — 13 variants|     | → consumed by sinks    |
|   from torii-  |     |   that dispatch        |
|   introspect)  |     |   RegisterExternal-    |
+-------+--------+     |   ContractCommand      |
        |              +-----------+------------+
        |                          |
        |                          v
        |              +------------------------+
        |              | RegisterExternal       |
        |              |   ContractCommand      |
        |              |   Handler              |
        |              |   updates Shared       |
        |              |   {Contract,Decoder}   |
        |              |   Registry             |
        |              +------------------------+
        |
        v
  envelopes (DOJO_TYPE_ID) flow to:
    - introspect-sql-sink   (persists schema + records)
    - torii-ecs-sink        (entity/event-message classification)
    - arcade-sink           (Arcade projections)
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `DojoDecoder<Store, F>` | `src/decoder.rs` | 55 | Decoder generic over a `DojoStoreTrait` impl + a `DojoSchemaFetcher` |
| `DojoEvent`, `DojoBody` | `src/decoder.rs` | 35 / 40 | Envelope union: `Introspect(IntrospectMsg)` \| `ExternalContractRegistered(...)` |
| `DOJO_TYPE_ID` | `src/decoder.rs` | 32 | `TypeId::new("dojo")` — the single type id every Dojo-aware sink subscribes to |
| `DojoTableEvent<Store, F>` | `src/decoder.rs` | 79 | Async trait for model registration events |
| `DojoRecordEvent<Store, F>` | `src/decoder.rs` | 88 | Sync trait for record mutations |
| `DojoTable` | `src/table.rs` | 14 | Dojo model metadata (columns, primary key, key/value field separation) |
| `ExternalContractRegistered`, `ExternalContractRegisteredEvent`, `ExternalContractRegisteredBody` | `src/external_contract.rs` | 73 / 31 | On-chain registration of external token contracts |
| `RegisterExternalContractCommand` | `src/external_contract.rs` | — | Torii command dispatched on registration |
| `RegisterExternalContractCommandHandler` | `src/external_contract.rs` | — | `CommandHandler` that updates the shared registries |
| `SharedContractTypeRegistry`, `SharedDecoderRegistry` | `src/external_contract.rs` | 28 / 29 | `Arc<RwLock<HashMap<Felt, …>>>` handles shared between sink and handler |
| `RegisteredContractType` | `src/external_contract.rs` | 20 | `World` / `Erc20` / `Erc721` / `Erc1155` / `Other` |
| `resolve_external_contract`, `contract_type_from_decoder_ids` | `src/external_contract.rs` | — | Map a Dojo `contract_name` to `(DecoderIds, RegisteredContractType)` |
| `DojoToriiError`, `DojoToriiResult` | `src/error.rs` | — | Domain errors |

### Internal Modules

- `decoder` — `DojoDecoder` + the `Decoder` impl + `DojoEvent` union + helper macros for Cairo deserialisation.
- `event` — lightweight envelope-free wrapper used internally for deserialisation plumbing.
- `table` — `DojoTable` / `DojoTableInfo` + `sort_columns` helper.
- `external_contract` — everything around `ExternalContractRegistered`: types, command, handler, shared registries, `RegisteredContractType` + resolver.
- `error` — `DojoToriiError` (deserialisation failures, schema fetch failures, missing primary key, etc.).
- `store` — `DojoStoreTrait` + no-op SQLite/Postgres impls (retained for callers that need a pass-through store; real persistence happens in `introspect-sql-sink`).

### Interactions

- **Upstream (consumers)**: `introspect-sql-sink`, `torii-ecs-sink`, `arcade-sink`, every Dojo-capable binary (`bins/torii-introspect-bin`, `bins/torii-arcade`, `bins/torii-introspect-synth`).
- **Downstream deps**: `torii`, `torii-common`, `torii-introspect`, `torii-sql` (for the optional store impls), `torii-types`, `dojo-introspect`, `introspect-events`, `introspect-rust-macros`, `introspect-types`, `starknet`, `starknet-types-core`, `starknet-types-raw`, `async-trait`, `serde`, `tokio`, `itertools`.
- **Features**: `postgres`, `sqlite` — enable the corresponding sqlx driver in `store/{postgres,sqlite}`.

### Extension Points

- New Cairo event → add a variant to `DojoEvent` + a branch in `DojoDecoder::decode`; if it mutates a table, implement `DojoRecordEvent` / `DojoTableEvent` and translate to an `IntrospectMsg` so the existing sinks Just Work.
- New external-contract type → extend `RegisteredContractType` and `resolve_external_contract`.
- Custom store backend → implement `DojoStoreTrait`; wire it into `DojoDecoder::new`.
