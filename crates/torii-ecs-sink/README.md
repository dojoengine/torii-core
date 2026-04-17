# torii-ecs-sink

Serves the **legacy `world.World` gRPC API** on top of the data already
persisted by `introspect-sql-sink`. It is the compatibility shim that lets
existing Dojo clients (Cartridge UI, dojo.js SDK) talk to the refactored
Torii pipeline.

## Role in Torii

Dojo worlds emit introspect events that `introspect-sql-sink` turns into
relational rows. `torii-ecs-sink` consumes the same `DojoEvent` stream but
only cares about two things: classifying each table as an *entity* or
*event-message*, and handling runtime registration of external contracts
(ERC20/ERC721/ERC1155) so the `EcsService` can enrich its responses. It
does not write business data; it reads from the introspect tables populated
earlier in the sink pipeline to answer `RetrieveEntities`,
`RetrieveEventMessages`, `RetrieveEvents`, and the matching subscription
streams.

## Architecture

```text
DojoDecoder (torii-dojo)
        |
        v  DojoBody { DojoEvent::Introspect(...) | DojoEvent::External(...) }
        |
+----------------------------------------------+
|                   EcsSink                    |
|                                              |
|  process(envelopes, batch):                  |
|    - IntrospectMsg → update table_kind cache |
|      (entity | event_message)                |
|    - ExternalContractRegistered → dispatch   |
|      RegisterExternalContractCommand on the  |
|      command bus (if indexing enabled)       |
|                                              |
|  installed_external_decoders: HashSet<Id>    |
|  contract_types: SharedContractTypeRegistry  |
+----------------------+-----------------------+
                       |
                       v
+----------------------------------------------+
|                  EcsService                  |
|                                              |
|  sqlx pools: world + optional erc20/721/1155 |
|  tables: torii_dojo_manager_state,           |
|          torii_ecs_entity_meta,              |
|          torii_ecs_entity_models,            |
|          torii_ecs_events,                   |
|          torii_ecs_table_kinds               |
|                                              |
|  gRPC server: world.World                    |
|    - Worlds                                  |
|    - RetrieveEntities                        |
|    - RetrieveEventMessages                   |
|    - RetrieveEvents                          |
|    - SubscribeContracts                      |
|    - SubscribeEntities                       |
|    - SubscribeEventMessages                  |
|    - SubscribeEvents                         |
|    - UpdateEntitiesSubscription              |
|    - UpdateEventMessagesSubscription         |
+----------------------------------------------+
```

## Deep Dive

### Public API

| Item | File | Line | Purpose |
|---|---|---|---|
| `EcsSink` | `src/sink.rs` | 23 | Sink wrapping `EcsService` + the shared contract-type registry |
| `EcsSink::new(db_url, max_conns, erc20?, erc721?, erc1155?, types_registry, from_block, enable_indexing, installed_decoders)` | `src/sink.rs` | 33 | Multi-URL constructor |
| `EcsSink::get_grpc_service_impl` | `src/sink.rs` | 63 | Returns `Arc<EcsService>` for `with_grpc_router` |
| `EcsService` | `src/grpc_service.rs` | — | Tonic impl of `world.World` |
| `TableKind` | `src/grpc_service.rs` | — | `Entity` / `EventMessage` classifier |
| `FILE_DESCRIPTOR_SET` | `src/lib.rs` | — | `world.proto` + `types.proto` descriptor bytes |

### Internal Modules

- `lib.rs` — re-exports + generated `world`, `types`, reflection bytes.
- `sink.rs` — `Sink` impl; consumes `DojoEvent::Introspect` for `TableKind` classification, `DojoEvent::External` for external-contract registration (dispatched via `CommandBusSender` → `RegisterExternalContractCommand`).
- `grpc_service.rs` — `EcsService`; pagination, subscription streams, cross-DB joins between introspect + token tables.

### Sink trait wiring

| Method | Behavior |
|---|---|
| `name` | `"ecs"` |
| `interested_types` | `[DOJO_TYPE_ID]` |
| `process(envelopes, batch)` | Downcasts each envelope to `DojoBody`; for `IntrospectMsg::CreateTable/RenameTable/DropTable` updates `torii_ecs_table_kinds`; for `ExternalContractRegistered` dispatches `RegisterExternalContractCommand` through the `CommandBusSender` captured at `initialize` |
| `topics` | Empty — uses gRPC subscription streams directly, not the central EventBus |
| `build_routes` | `Router::new()` — gRPC only |
| `initialize(event_bus, ctx)` | Captures `ctx.command_bus` into its `RwLock<Option<CommandBusSender>>` so `process` can dispatch commands |

### Storage (reads only — writes via introspect-sql-sink)

- `torii_dojo_manager_state` — Dojo table metadata
- `torii_ecs_entity_meta` — entity snapshots
- `torii_ecs_entity_models` — per-entity model rows
- `torii_ecs_events` — raw events
- `torii_ecs_table_kinds` — `entity` vs `event_message` classification

**Gotcha**: `RetrieveEntities` only returns tables classified as `entity`; `RetrieveEventMessages` only returns `event_message`. A model like `ARCADE-Sale` can be a valid `event_message` and intentionally return nothing from `RetrieveEntities`.

### Interactions

- **Upstream (consumers)**: `bins/torii-introspect-bin`, `bins/torii-arcade`.
- **Downstream deps**: `torii`, `torii-dojo`, `torii-introspect`, `torii-runtime-common`, `torii-sql`, `sqlx`, `tonic`, `prost`, `starknet-types-raw`, `tokio`, `async-trait`, `tracing`, `metrics`, `hex`.

### End-to-end validation

Run the bundled script against a live `torii-server`:

```bash
./scripts/test-ecs-grpc-e2e.sh                     # auto-discovers via SQLite
DB_URL=postgres://torii:torii@localhost:5432/torii \
  ./scripts/test-ecs-grpc-e2e.sh                    # Postgres-backed
```

Overrides: `GRPC_ADDR`, `DB_PATH`, `WORLD_ADDRESS`, `ENTITY_MODEL`, `EVENT_MESSAGE_MODEL`.

Editor-driven checks: `crates/torii-ecs-sink/ecs-grpc.http` (Kulala-compatible).

### Extension Points

- New RPCs → `proto/world.proto` + `EcsService`. Keep the stable `world.World` service name — dojo.js depends on it.
- Cross-DB joins → widen `EcsService::new` with a new URL parameter and a new pool. The sink already uses one pool per token standard.
- Avoid adding writes here. Writes belong in `introspect-sql-sink`; this sink is the *read* facade.
