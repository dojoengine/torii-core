# torii-tokens (binary)

Unified **ERC20 + ERC721 + ERC1155 indexer**. Combines all three token
sinks behind one process, one port, one engine DB. Supports three
extraction modes (`block-range`, `event`, `global-event`) so operators
can pick full-chain indexing or per-contract cursors depending on their
use case.

## Role in Torii

The production-grade consolidation of `bins/torii-erc20` + equivalent
ERC721/ERC1155 binaries. Most deployments want all three standards in
one place; this is that binary. Every token sink is optional — omit the
`--erc20` / `--erc721` / `--erc1155` / `--include-well-known` flags and
that sink is not installed. See `torii-runtime-common::token_support` for
the boolean trio that drives the install logic.

## Architecture

```text
+---------------------------+
|   clap::Parser  Config    |   --mode (block-range | event | global-event)
|   (src/config.rs)         |   --erc20, --erc721, --erc1155 lists
|                           |   --include-well-known (ETH/STRK)
|                           |   --observability (Prometheus)
|                           |   --database-url, --storage-database-url
+--------------+------------+
               |
               v
+---------------------------+
| resolve_token_db_setup    |  engine.db + erc20.db + erc721.db + erc1155.db
| (torii-runtime-common)    |  or single Postgres URL for all
+--------------+------------+
               |
               v
+------------------------------------------------+
|                  main()                        |
|                                                |
| Extractor selected by --mode:                  |
|   BlockRangeExtractor  (one global cursor)     |
|   EventExtractor       (per-contract cursors)  |
|   GlobalEventExtractor (one cursor via         |
|                         starknet_getEvents)    |
|                                                |
| Decoders registered (all three always, but     |
|   only sinks actually consuming are installed):|
|   Erc20Decoder   → Erc20Sink + service         |
|   Erc721Decoder  → Erc721Sink + service        |
|   Erc1155Decoder → Erc1155Sink + service       |
|                                                |
| Contract identification:                       |
|   - explicit mappings for each sink's list     |
|   - well-known ETH + STRK (if requested)       |
|   - ContractRegistry with Erc20Rule,           |
|     Erc721Rule, Erc1155Rule                    |
|   - rpc_parallelism = --rpc-parallelism        |
|                                                |
| Metadata pipelines:                            |
|   N workers × queue_capacity × max_retries     |
|   (parallelism=8, queue=2048, retries=5)       |
|                                                |
| EtlConcurrencyConfig {                         |
|   max_prefetch_batches: --max-prefetch-batches |
| }                                              |
|                                                |
| grpc_router = builder                          |
|   .add_service(Erc20Server)                    |
|   .add_service(Erc721Server)                   |
|   .add_service(Erc1155Server)                  |
|   + composed reflection (TORII + all 3 descr.) |
|                                                |
| torii::run(config).await                       |
+------------------------------------------------+
```

## Deep Dive

### CLI highlights (full list in `src/config.rs`)

| Flag | Default | Purpose |
|---|---|---|
| `--mode` | `block-range` | Extraction mode: `block-range` / `event` / `global-event` |
| `--rpc-url` (env `STARKNET_RPC_URL`) | Cartridge mainnet | JSON-RPC endpoint |
| `--from-block`, `--to-block` | `0` / none | Range bounds |
| `--db-dir` | `./torii-data` | Root when using SQLite (`engine.db`, `erc20.db`, `erc721.db`, `erc1155.db`) |
| `--database-url` (env `DATABASE_URL`) | — | Postgres URL or SQLite override for the engine DB |
| `--storage-database-url` (env `STORAGE_DATABASE_URL`) | — | Token-storage override (else uses `--database-url`, else `--db-dir`) |
| `--port` | `3000` | gRPC + HTTP port |
| `--observability` | off | Sets `TORII_METRICS_ENABLED=true` via `torii-config-common` |
| `--erc20` / `--erc721` / `--erc1155` | `[]` | Comma-separated contract lists |
| `--include-well-known` | off | Pre-map ETH + STRK |
| `--batch-size` | `50` | Blocks per `BlockRangeExtractor` batch |
| `--event-chunk-size` | `1000` | Events per `starknet_getEvents` page |
| `--event-block-batch-size` | `10000` | Blocks per event-mode iteration |
| `--event-bootstrap-blocks` | `20000` | Scan depth for auto-discovery in event mode |
| `--max-prefetch-batches` | `2` | ETL prefetch queue depth |
| `--cycle-interval` | `3` | Idle retry seconds |
| `--rpc-parallelism` | `0` (auto) | Max concurrent chunked RPC requests |
| `--metadata-parallelism` | `8` | Async metadata fetcher workers |
| `--metadata-queue-capacity` | `2048` | Metadata command queue |
| `--metadata-max-retries` | `5` | Metadata fetch retries |
| `--metadata-mode` | `inline` | `inline` or `deferred` |
| `--metadata-backfill-only` | off | Reserved for future metadata-only workflows |

### Internal modules

- `config.rs` — `Config`, `ExtractionMode`, `MetadataMode`, `well_known_erc20_contracts`, `has_tokens`, unit tests for flag parsing.
- `main.rs` — `resolve_token_db_setup` → per-standard storage + sink + decoder + rule → `ContractRegistry.load_from_db` → `ToriiConfig` build → `torii::run`. Composes gRPC reflection from four descriptor sets.

### Workspace dependencies

`torii`, `torii-erc20`, `torii-erc721`, `torii-erc1155`, `torii-runtime-common`, `torii-config-common`, `torii-sql` (postgres + sqlite), `starknet`, `tonic`, `tonic-reflection`, `clap`, `tokio`, `tracing`, `anyhow`, `url`.

### Extension Points

- Add another token standard → register its decoder + rule + sink + gRPC service + descriptor set; extend `InstalledTokenSupport` in `torii-runtime-common` if the install matrix gets more complex.
- Change the balance model for a single standard → modify the corresponding sink; this binary is pure wiring.

Paired profiler: `bins/torii-tokens-synth`.
