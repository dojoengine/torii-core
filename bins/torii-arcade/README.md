# torii-arcade (binary)

**Cartridge Arcade backend.** Combines Dojo introspect indexing, the
`world.World` ECS gRPC, the `arcade.v1.Arcade` projection gRPC, token
indexers (ERC20/721/1155), controller sync, and core `torii.Torii`
subscriptions into one process with sensible Arcade-specific defaults.

## Role in Torii

Production binary for the Cartridge Arcade UI. Pre-ships a curated list
of world contracts + token contracts in its CLI defaults, so operators
usually run it with `cargo run --bin torii-arcade` and override only the
backend URLs. Compared to `bins/torii-introspect-bin` it adds the
`arcade-sink` projection layer.

## Architecture

```text
+-----------------------------+
|  clap::Parser Config        |  defaults: Arcade world
|  (src/config.rs)            |    0x07a0…04ff7 + 9 more Dojo contracts,
|                             |    17 ERC721, 46 ERC20, 1 ERC1155
|                             |  --introspect-contracts override possible
|                             |  --controllers / --observability
|                             |  --pathfinder-path for bulk backfill
+-------------+---------------+
              |
              v
+------------------------------------------------------+
|                       main()                         |
|                                                      |
| Backend URL resolution (config.engine_database_url,  |
|   storage_database_url, token_storage_urls):         |
|   - single --database-url (postgres): shared pool    |
|   - --storage-database-url overrides just storage    |
|   - SQLite defaults under --db-dir:                  |
|       arcade-engine.db, arcade-introspect.db,        |
|       arcade-erc20.db, arcade-erc721.db,             |
|       arcade-erc1155.db                              |
|                                                      |
| Extractor: EventExtractor (optionally backed by      |
|   torii-pathfinder for backfill when                 |
|   --pathfinder-path is set)                          |
|                                                      |
| Decoders:                                            |
|   DojoDecoder, Erc20Decoder, Erc721Decoder,          |
|   Erc1155Decoder                                     |
|                                                      |
| Sinks (ordered — arcade projections depend on        |
|   introspect tables already being up to date):       |
|   1. IntrospectDb<Backend> (introspect-sql-sink)     |
|   2. EcsSink          (legacy world.World)           |
|   3. ArcadeSink       (Arcade projections +          |
|                          arcade.v1 gRPC)             |
|   4. ControllersSink (--controllers)                 |
|   5. Erc20Sink / Erc721Sink / Erc1155Sink            |
|                                                      |
| Contract identification:                             |
|   explicit mappings for every contract in defaults   |
|   ContractRegistry + Erc*Rule (load_from_db on       |
|   startup)                                           |
|                                                      |
| EtlConcurrencyConfig                                 |
|   max_prefetch_batches = --max-prefetch-batches      |
|                                                      |
| grpc_router: WorldServer + ArcadeServer +            |
|   Erc20Server + Erc721Server + Erc1155Server +       |
|   composed reflection over five descriptor sets      |
|                                                      |
| optional TLS                                         |
| torii::run(config)                                   |
+------------------------------------------------------+
```

## Deep Dive

### CLI highlights (most flags have Arcade-specific defaults)

| Flag | Default | Purpose |
|---|---|---|
| `--rpc-url` (env `STARKNET_RPC_URL`) | Cartridge mainnet | RPC |
| `--world-address` (env `ARCADE_WORLD_ADDRESS`) | `0x07a079…04ff7` | Primary Arcade world |
| `--dojo-contracts` | 10 curated Arcade-related worlds | Event source Dojo contracts |
| `--introspect-contracts` | falls back to `--dojo-contracts` | Override which contracts feed introspect decoding |
| `--erc20` / `--erc721` / `--erc1155` | baked-in defaults (46 / 17 / 1) | Token contracts |
| `--include-well-known` | `true` | Pre-map ETH + STRK |
| `--from-block`, `--to-block` | `0` / none | Range |
| `--db-dir` | `./torii-data` | SQLite root; files prefixed with `arcade-` |
| `--database-url` (env `DATABASE_URL`) | — | Engine DB URL (Postgres or SQLite) |
| `--storage-database-url` (env `STORAGE_DATABASE_URL`) | — | Introspect storage URL |
| `--token-storage-database-url` (env `TOKEN_STORAGE_DATABASE_URL`) | — | Per-token URL override |
| `--port` | `3000` | gRPC + HTTP |
| `--tls-cert`, `--tls-key` | — | PEM pair |
| `--controllers`, `--controllers-api-url` | off / `https://api.cartridge.gg/query` | Opt-in Cartridge controller sync |
| `--observability` | off | Metrics |
| `--event-chunk-size`, `--event-block-batch-size` | `1000` / `10000` | `starknet_getEvents` tuning |
| `--max-prefetch-batches` | `2` | ETL prefetch queue depth |
| `--rpc-parallelism` | `0` (auto) | Chunked-RPC parallelism |
| `--max-db-connections` | auto | Pool ceiling |
| `--ignore-saved-state` | off | Force reprocessing |
| `--index-external-contracts` | `true` | React to Dojo `ExternalContractRegistered` events |
| `--allow-unsafe-latest-schema-bootstrap` | off | Dev-only schema bootstrap |
| `--metadata-mode` | `inline` | `inline` or `deferred` |
| `--historical` | `[]` | `ModelName` or `0xAddr:ModelName` for `_historical` tables |
| `--pathfinder-path` | — | Optional Pathfinder SQLite DB for bulk backfill |

### Sink ordering

Arcade relies on the introspect tables being populated before the
projection step runs, so the sink pipeline is ordered (see
`src/main.rs`). A failure in an earlier sink aborts the batch; the
cursor is not committed and the batch replays on the next cycle.

### Workspace dependencies

`torii`, `torii-dojo`, `torii-introspect`, `torii-introspect-sql-sink`, `torii-ecs-sink`, `torii-arcade-sink`, `torii-erc20`, `torii-erc721`, `torii-erc1155`, `torii-controllers-sink`, `torii-common`, `torii-runtime-common`, `torii-config-common`, `torii-sql`, `torii-pathfinder` (feature `etl`), `torii-types`, plus `clap`, `tonic`, `tonic-reflection`, `tokio`, `tracing`, `starknet`, `anyhow`, `url`.

### Extension Points

- Add a new Arcade projection → extend `ArcadeSink` / `ArcadeService`, not this binary.
- Swap to Pathfinder backfill → pass `--pathfinder-path`; the extractor is swapped to `PathfinderCombinedExtractor` so it streams archival data then flips to the live RPC extractor.
- Private Arcade namespace → point `--database-url` at a dedicated Postgres + keep storage URL at the same target; `resolve_token_db_setup` will keep everything on one backend.
