# torii-introspect-bin (binary `torii-server`)

**Canonical full-stack Torii binary.** Indexes Dojo introspect events,
optional token transfers (ERC20/721/1155), and optional controller
usernames into a shared SQL backend. Exposes the legacy `world.World`
gRPC API plus every token gRPC service on one port. The most complete
example of how the pipeline fits together.

## Role in Torii

Production deployments of Dojo worlds point at this binary. It combines:

1. `torii-dojo::DojoDecoder` consuming the world contract's events,
2. `introspect-sql-sink` materialising schema + records,
3. `torii-ecs-sink` serving the legacy `world.World` gRPC,
4. Optional `torii-erc20/721/1155` sinks + decoders + rules for token
   data referenced by models,
5. Optional `torii-controllers-sink` for Cartridge controller metadata,
6. Optional TLS for the listener.

Read `bins/torii-erc20` first — this binary is the same pattern scaled
up.

## Architecture

```text
+-----------------------------+
|  clap::Parser  Config       |  argument group: --contract + --erc20 +
|  (src/config.rs)            |    --erc721 + --erc1155 (at least one)
|                             |  --database-url / --storage-database-url
|                             |  --historical (for _historical tables)
|                             |  --tls-cert / --tls-key
|                             |  --controllers (enable sync)
|                             |  --observability / --ignore-saved-state
|                             |  --index-external-contracts (default on)
+-------------+---------------+
              |
              v
+--------------------------------------------------------+
|                     main()                             |
|                                                        |
| resolve engine + storage URLs                          |
|   (Postgres uniform or SQLite under --db-dir)          |
|                                                        |
| EventExtractor over --rpc-url                          |
|   chunk=1000, block-batch=10000                        |
|   cursor-resume unless --ignore-saved-state            |
|                                                        |
| Decoders:                                              |
|   DojoDecoder (over dojo-introspect schema fetcher)    |
|   + optional Erc20Decoder / Erc721Decoder /            |
|     Erc1155Decoder                                     |
|                                                        |
| Sinks (installed by config):                           |
|   - IntrospectDb<Backend>                              |
|     (IntrospectPgDb or IntrospectSqliteDb)             |
|   - EcsSink (reads the introspect tables; serves       |
|      legacy world.World gRPC)                          |
|   - ControllersSink           (--controllers)          |
|   - Erc20Sink/Erc721Sink/Erc1155Sink + services        |
|                                                        |
| ContractRegistry with Erc*Rule registered;             |
|   load_from_db() on startup;                           |
|   handles Dojo ExternalContractRegistered via          |
|   RegisterExternalContractCommand (index_external_     |
|   contracts flag)                                      |
|                                                        |
| EtlConcurrencyConfig                                   |
|   max_prefetch_batches = --max-prefetch-batches        |
|                                                        |
| grpc_router adds all token servers + WorldServer (ECS) |
|   + reflection composing TORII + token + ecs + arcade  |
|   descriptor sets                                      |
|                                                        |
| optional TLS listener (both --tls-cert and --tls-key)  |
|                                                        |
| torii::run(config).await                               |
+--------------------------------------------------------+
```

## Deep Dive

### CLI highlights

| Flag | Default | Purpose |
|---|---|---|
| `--contract` / `--contracts` | required* | Dojo world contract addresses (`*` required if no token list) |
| `--erc20`, `--erc721`, `--erc1155` | `[]` | Token contracts to pin |
| `--rpc-url` (env `STARKNET_RPC_URL`) | Cartridge mainnet | JSON-RPC endpoint |
| `--from-block`, `--to-block` | `0` / none | Range |
| `--db-dir` | `./torii-data` | SQLite root |
| `--database-url` (env `DATABASE_URL`) | — | Engine DB URL |
| `--storage-database-url` (env `STORAGE_DATABASE_URL`) | — | Storage DB URL (**Postgres only** for introspect-pg path) |
| `--port` | `3000` | gRPC + HTTP |
| `--tls-cert`, `--tls-key` | — | PEM files; must come as a pair |
| `--controllers` | off | Install `torii-controllers-sink` |
| `--controllers-api-url` | `https://api.cartridge.gg/query` | GraphQL URL for controllers sink |
| `--observability` | off | Sets `TORII_METRICS_ENABLED` |
| `--event-chunk-size` / `--event-block-batch-size` | `1000` / `10000` | `starknet_getEvents` tuning |
| `--max-prefetch-batches` | `2` | ETL prefetch queue depth |
| `--cycle-interval` | `3` | Idle retry seconds |
| `--rpc-parallelism` | `0` (auto) | Chunked-RPC parallelism |
| `--max-db-connections` | auto | Pool size ceiling |
| `--ignore-saved-state` | off | Force reprocessing from `--from-block` |
| `--index-external-contracts` | `true` | React to Dojo `ExternalContractRegistered` events at runtime |
| `--historical` | `[]` | `ModelName` or `0xAddr:ModelName` entries to mirror into append-only `_historical` tables |

Argument group enforces that **at least one** of `--contract`,
`--erc20`, `--erc721`, `--erc1155` is provided.

### Internal modules

- `config.rs` — `Config`, `parse_historical_models`, backend resolution (`storage_backend`, `engine_database_url`, `storage_database_url`), TLS pair validation.
- `main.rs` — sink assembly, registry wiring, optional TLS, gRPC reflection composition, cursor-resume logic.

### Workspace dependencies

`torii`, `torii-dojo`, `torii-introspect`, `torii-introspect-sql-sink` (postgres + sqlite), `torii-ecs-sink`, `torii-controllers-sink`, `torii-erc20` / `torii-erc721` / `torii-erc1155`, `torii-arcade-sink` (for its descriptor set — the `ArcadeServer` is only mounted if enabled in custom builds), `torii-common`, `torii-runtime-common`, `torii-config-common`, `torii-sql` (postgres + sqlite). Plus `clap`, `tonic`, `tonic-reflection`, `tokio`, `tracing`, `starknet`, `anyhow`, `url`.

### Extension Points

- Add another sink → build it, push `Box<dyn Sink>` into the builder, add its descriptor set to the reflection composition.
- Change extractor → replace `EventExtractor` with `BlockRangeExtractor` or a custom one; the rest of the pipeline doesn't care.
- Add Postgres schemas → `introspect-sql-sink::NamespaceMode` decides the layout; flip at binary level.

Paired profiler: `bins/torii-introspect-synth`.
