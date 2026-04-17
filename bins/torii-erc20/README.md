# torii-erc20 (binary)

Stand-alone **ERC20 indexer**. The simplest canonical wiring of the Torii
stack: one decoder, one sink, one gRPC service, one extractor. Start here
if you are learning how binaries are assembled.

## Role in Torii

Replaces a per-contract subgraph for ERC20 tokens. Runs a
`BlockRangeExtractor` over the configured RPC, feeds events through
`Erc20Decoder` / `Erc20Sink`, serves `torii.sinks.erc20.Erc20` and core
`torii.Torii` gRPC + HTTP on the same port. Uses the same `torii::run`
orchestrator as every other binary.

## Architecture

```text
+------------------------+        +--------------------------+
|  clap::Parser  Config  | -----> |  resolve_single_db_setup |
|  (src/config.rs)       |        |  (runtime-common)        |
+------------------------+        +-------------+------------+
                                                |
                                                v
+-----------------------------------------------------------+
|                 main() — build ToriiConfig                |
|                                                           |
|  - Erc20Storage::connect_pool(storage_url)                |
|  - JsonRpcClient over --rpc-url                           |
|  - BlockRangeExtractor (from_block → to_block, batch=50)  |
|  - Erc20Decoder + Erc20Sink + Erc20Service                |
|  - Erc20MetadataCommandHandler                            |
|      parallelism=1, queue=4096, max_retries=3             |
|  - auto-discovery (unless --no-auto-discovery):           |
|      ContractRegistry::new(provider, engine_db)           |
|        .with_rule(Erc20Rule) → registry.load_from_db()    |
|  - explicit mappings for well-known ETH / STRK / SURVIVOR |
|  - grpc_router = Server::builder()                        |
|       .add_service(Erc20Server::new(service))             |
|       + reflection (TORII_DESCRIPTOR_SET +                |
|          erc20 FILE_DESCRIPTOR_SET)                       |
|                                                           |
|  torii::run(config).await                                 |
+-----------------------------------------------------------+
```

## Deep Dive

### CLI (source: `src/config.rs`)

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--rpc-url` | `STARKNET_RPC_URL` | Cartridge mainnet | Starknet JSON-RPC endpoint |
| `--from-block` | — | `0` | Backfill starting block |
| `--to-block` | — | none (follow head) | Stop block for finite runs |
| `--db-path` | — | `./erc20-data.db` | SQLite storage; engine.db derives next to it |
| `--database-url` | `DATABASE_URL` | — | Postgres URL / SQLite URL override |
| `--no-auto-discovery` | — | off | Strict mode: only explicitly mapped contracts |
| `--contracts` | — | `[]` | Comma-separated hex addresses |
| `--port` | — | `3000` | TCP port for gRPC + HTTP |
| `--cycle-interval` | — | `3` | Seconds between extractor idle retries |

Pre-mapped contracts (added unless overridden):
ETH (`0x049D36…004dC7`), STRK (`0x04718f…c938D`), SURVIVOR
(`0x042DD7…2Ec86B`). See `Config::well_known_contracts` in `src/config.rs`.

### Internal modules

- `config.rs` — `Config` struct + the well-known contract list.
- `main.rs` — canonical build order: logging → config → storage → provider → decoder → rule → (optional) `ContractRegistry.load_from_db` → explicit mappings → sink → metadata command handler → gRPC router → `torii::run`.

### Workspace dependencies

`torii`, `torii-erc20`, `torii-runtime-common`, `torii-sql` (features:
`postgres`, `sqlite`), `starknet`, `tonic`, `tonic-reflection`, `clap`,
`tokio`, `tracing`, `url`, `anyhow`.

Opt-in feature: `profiling` — enables `pprof`; writes a flamegraph on
shutdown (see `PROFILING.md` in this directory for details).

### Extension Points

- **Historical only**: `--from-block N --to-block M`; extractor sets `is_finished() = true` and the ETL loop exits cleanly.
- **Live only**: start near the chain head without `--to-block`.
- **Custom tokens**: add to `Config::well_known_contracts` (pre-map) or pass `--contracts 0xaaa,0xbbb`. Combine with `--no-auto-discovery` to pin indexing.
- **Extra auto-identification**: register another `IdentificationRule` on the `ContractRegistry` before `torii::run`.
- **Skip metadata fetching**: drop the `with_metadata_pipeline` call on the sink; only the metadata enrichment is affected.

See also `bins/torii-erc20-synth` for the paired profiling harness.
