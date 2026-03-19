# torii-arcade

`torii-arcade` is a Cartridge Arcade metatorii backend that runs the following in one process:

- Dojo introspect indexing for the Arcade world
- `world.World` ECS gRPC reads
- `arcade.v1.Arcade` low-latency marketplace and inventory reads
- ERC20 / ERC721 / ERC1155 token indexing and gRPC reads
- Core `torii.Torii` subscriptions and metrics endpoint

## Defaults

- RPC: `https://api.cartridge.gg/x/starknet/mainnet`
- Primary world: `0x2d26295d6c541d64740e1ae56abc079b82b22c35ab83985ef8bd15dc0f9edfb`
- Default indexing seed set: upstream `torii-arcade-metatorii.toml` worlds and token contracts
- Default Dojo introspect set: upstream `WORLD:*` contracts except the primary world contract
  `0x2d26295d6c541d64740e1ae56abc079b82b22c35ab83985ef8bd15dc0f9edfb`, which is excluded from
  introspect decoding by default because it currently emits incompatible historical Dojo record events
- Metadata mode: `inline`
- Well-known ERC20s: enabled by default
- Token URI + image cache: enabled by default in inline metadata mode, with images cached under
  `./data/image-cache`
- DB directory: `./torii-data`

SQLite local defaults:

- engine: `./torii-data/arcade-engine.db`
- dojo / ecs: `./torii-data/arcade-introspect.db`
- erc20: `./torii-data/arcade-erc20.db`
- erc721: `./torii-data/arcade-erc721.db`
- erc1155: `./torii-data/arcade-erc1155.db`

If `--database-url` or `--storage-database-url` is PostgreSQL and `--token-storage-database-url` is omitted, token storage defaults to the same PostgreSQL database.

`torii-arcade` requires one backend per runtime. Mixed SQLite/PostgreSQL configurations are rejected at startup.

## Start

From the repository root:

```bash
cargo run --bin torii-arcade -- --from-block 0
```

Useful overrides:

```bash
cargo run --bin torii-arcade -- \
  --from-block 0 \
  --port 3000 \
  --erc721 0x1e1c477f2ef896fd638b50caa31e3aa8f504d5c6cb3c09c99cd0b72523f07f7 \
  --erc20 0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7,0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D
```

If you need to opt a world back into Dojo introspect explicitly for investigation:

```bash
cargo run --bin torii-arcade -- \
  --from-block 0 \
  --introspect-contracts 0x2d26295d6c541d64740e1ae56abc079b82b22c35ab83985ef8bd15dc0f9edfb,0x8b4838140a3cbd36ebe64d4b5aaf56a30cc3753c928a79338bf56c53f506c5
```

PostgreSQL:

```bash
cargo run --bin torii-arcade -- \
  --database-url postgres://torii:torii@localhost:5432/torii \
  --storage-database-url postgres://torii:torii@localhost:5432/torii \
  --from-block 0
```

## Operational validation

The mixed backend can be checked end-to-end from the repository root:

```bash
./scripts/test-arcade-backend-e2e.sh
```

The script verifies:

- gRPC reflection
- `world.World`
- `arcade.v1.Arcade`
- `torii.sinks.erc20.Erc20`
- `torii.sinks.erc721.Erc721`
- `Worlds`, `RetrieveEntities`, `RetrieveEventMessages`, `RetrieveEvents`
- `ListGames`, `ListEditions`, `ListCollections`, `ListListings`, `ListSales`
- token service stats for the mixed runtime

## Exposed gRPC services

- `torii.Torii`
- `world.World`
- `arcade.v1.Arcade`
- `torii.sinks.erc20.Erc20` when ERC20 contracts are configured
- `torii.sinks.erc721.Erc721` when ERC721 contracts are configured
- `torii.sinks.erc1155.Erc1155` when ERC1155 contracts are configured

## Arcade service surface

`arcade.v1.Arcade` is backed by projection tables maintained in the introspect storage database:

- `ListGames`
- `ListEditions`
- `ListCollections`
- `ListListings`
- `ListSales`
- `GetPlayerInventory`

The service bootstraps its projections from existing `ARCADE-*` introspect tables on startup and then incrementally refreshes them from live introspect envelopes, so it works both on a warm database and during active indexing.

## Current scope

This binary is the mixed indexing/runtime entrypoint. It intentionally keeps the extraction model explicit and event-based for throughput:

- Dojo indexing defaults to the upstream `torii-arcade-metatorii.toml` `WORLD:*` contracts
- Dojo introspect decoding defaults to the compatible subset of those contracts; excluded contracts are blacklisted before decode so they do not trigger decoder fallback
- token indexing defaults to the upstream `torii-arcade-metatorii.toml` ERC20 / ERC721 / ERC1155 contracts
- `--include-well-known` is opt-in and disabled by default so the runtime matches the upstream seed config unless explicitly extended

Dynamic token enrollment is not part of this first cut.
