# Torii ECS Sink

`torii-ecs-sink` exposes the legacy `world.World` gRPC API on top of the data already indexed by `torii-introspect`.

## What It Serves

The sink mounts these legacy RPCs into `torii-introspect-bin` on the same gRPC port:

- `Worlds`
- `RetrieveEntities`
- `RetrieveEventMessages`
- `RetrieveEvents`
- `SubscribeContracts`
- `SubscribeEntities`
- `SubscribeEventMessages`
- `SubscribeEvents`
- `UpdateEntitiesSubscription`
- `UpdateEventMessagesSubscription`

The read path is backed by:

- `torii_dojo_manager_state` for Dojo table metadata
- `torii_ecs_entity_meta` and `torii_ecs_entity_models` for entity and event-message snapshots
- `torii_ecs_events` for raw events
- `torii_ecs_table_kinds` for entity vs event-message classification

## Manual Validation

With `torii-introspect-bin` running on the default port:

```bash
grpcurl -plaintext localhost:3000 list
grpcurl -plaintext localhost:3000 describe world.World
grpcurl -plaintext -d '{}' localhost:3000 world.World/Worlds
grpcurl -max-time 10 -plaintext -d '{"query":{"pagination":{"limit":1,"direction":"FORWARD"}}}' localhost:3000 world.World/RetrieveEvents
```

Important:

- `RetrieveEntities` only returns models classified as `entity`
- `RetrieveEventMessages` only returns models classified as `event_message`
- a model such as `ARCADE-Sale` can be valid for `RetrieveEventMessages` and intentionally return nothing from `RetrieveEntities`

## End-to-End Test Script

The repository ships a runnable script at `./scripts/test-ecs-grpc-e2e.sh`.

From the repository root:

```bash
./scripts/test-ecs-grpc-e2e.sh
```

By default the script:

- targets `localhost:3000`
- reads sqlite state from `./torii-data/introspect.db`
- autodiscovers one world that has both `entity` and `event_message` models
- validates reflection, `Worlds`, `RetrieveEntities`, `RetrieveEventMessages`, and `RetrieveEvents`

For Postgres-backed deployments, set `DB_URL` or `DATABASE_URL` to the same storage database used by `torii-introspect-bin`. The script will use `psql` for metadata discovery instead of `sqlite3`.

### Environment Overrides

You can override discovery when needed:

```bash
GRPC_ADDR=localhost:3000 \
DB_PATH=./torii-data/introspect.db \
WORLD_ADDRESS=0x7a079295990e43441a7389fdc3b9ba063c6cd6aee16fb846f598c42a9f04ff7 \
ENTITY_MODEL=ARCADE-Order \
EVENT_MESSAGE_MODEL=ARCADE-Sale \
./scripts/test-ecs-grpc-e2e.sh
```

If `WORLD_ADDRESS`, `ENTITY_MODEL`, and `EVENT_MESSAGE_MODEL` are provided, the script does not need sqlite introspection metadata to choose test cases.

Postgres example:

```bash
GRPC_ADDR=localhost:3000 \
DB_URL=postgres://torii:torii@localhost:5432/torii \
./scripts/test-ecs-grpc-e2e.sh
```

## Kulala

For editor-driven checks, the crate also includes `./crates/torii-ecs-sink/ecs-grpc.http`.
