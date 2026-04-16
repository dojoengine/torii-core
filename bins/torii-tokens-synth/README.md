# torii-tokens-synth (binary)

**Deterministic multi-standard token profiler.** Drives the ERC20,
ERC721, and ERC1155 pipelines with hand-rolled synthetic extractors and
produces a uniform `RunReport` JSON, so the three sinks (and the
combination) can be benchmarked side by side. Postgres-only, like
`torii-erc20-synth`.

## Role in Torii

One-stop perf harness for all three token sinks. Same shape as
`torii-erc20-synth` but with clap subcommands so you pick which sink(s)
to exercise.

## Architecture

```text
+--------------------------+
|  clap::Subcommand Cli    |  subcommands:
|                          |    erc20    Erc20ProfileConfig
|                          |    erc721   Erc721ProfileConfig
|                          |    erc1155  Erc1155ProfileConfig
|                          |    all      AllProfileConfig
+------------+-------------+
             |
             v
+----------------------------------------+
|  CommonConfig (flattened into each)    |
|    --db-url (Postgres only)            |
|    --from-block, --block-count         |
|    --tx-per-block, --blocks-per-batch  |
|    --token-count, --wallet-count       |
|    --seed, --output-root               |
|    --reset-schema                      |
+------------+---------------------------+
             |
             +--- Erc20SpecificConfig     (--approval-ratio-bps)
             +--- Erc721SpecificConfig    (--approval-ratio-bps,
             |                             --approval-for-all-ratio-bps,
             |                             --metadata-update-ratio-bps,
             |                             --batch-metadata-update-ratio-bps,
             |                             --max-token-id)
             +--- Erc1155SpecificConfig   (--transfer-single-ratio-bps,
                                           --transfer-batch-ratio-bps,
                                           --min-batch-size, --max-batch-size,
                                           --approval-for-all-ratio-bps,
                                           --uri-ratio-bps, --token-id-count)

subcommand.run():
    drop_postgres_schemas(engine, erc20, erc721, erc1155) when --reset-schema
    open storages for installed standards
    register SyntheticErc*MetadataCommandHandler +
             SyntheticErc*TokenUriCommandHandler (hand-rolled, no HTTP)
    extractor = Synthetic{Erc20|Erc721|Erc1155}Extractor (deterministic)
    tight loop: extract → decode → sink.process → commit_cursor → StageSample
    write RunReport JSON to perf/runs/<run_id>/
```

## Deep Dive

### Subcommands & flags

| Subcommand | Exercises | Extra flags |
|---|---|---|
| `erc20` | Erc20 sink alone | `--approval-ratio-bps` |
| `erc721` | Erc721 sink alone | `--approval-ratio-bps`, `--approval-for-all-ratio-bps`, `--metadata-update-ratio-bps`, `--batch-metadata-update-ratio-bps`, `--max-token-id` |
| `erc1155` | Erc1155 sink alone | `--transfer-single-ratio-bps`, `--transfer-batch-ratio-bps`, `--min-batch-size`, `--max-batch-size`, `--approval-for-all-ratio-bps`, `--uri-ratio-bps`, `--token-id-count` |
| `all` | All three concurrently | Combines all of the above |

Common flags on every subcommand: `--db-url` (default
`postgres://torii:torii@localhost:5432/torii`), `--from-block`,
`--block-count`, `--tx-per-block`, `--blocks-per-batch`, `--token-count`,
`--wallet-count`, `--seed`, `--output-root`, `--reset-schema`.

### Internal shape

- Hand-rolled `CommandHandler` implementations for metadata + token-URI so the perf numbers reflect storage only (no reqwest, no JSON parsing):
  - `SyntheticErc20MetadataCommandHandler`
  - `SyntheticErc721MetadataCommandHandler` / `SyntheticErc721TokenUriCommandHandler`
  - `SyntheticErc1155MetadataCommandHandler` / `SyntheticErc1155TokenUriCommandHandler`
- Reuses `drop_postgres_schemas` + `initialize_sink_with_command_handlers` from `torii-runtime-common`.
- Output shape is identical to `torii-erc20-synth` (`RunReport` JSON: `config`, `totals`, `throughput`, `stage_latency_ms`, `stage_share_percent`, `slowest_cycles`, `blocking_suspects`, `db`).

### Workspace dependencies

`torii`, `torii-erc20`, `torii-erc721`, `torii-erc1155`, `torii-common`, `torii-runtime-common`, `torii-sql`, plus `clap` (with `derive`), `tokio`, `tracing`, `serde`, `serde_json`, `anyhow`. Feature `profiling` pulls `pprof`.

### Extension Points

- New rate-shape for a sink → add fields to the `*SpecificConfig` + propagate through the matching `Synthetic*Config` in the token crate.
- Cross-sink scenarios → the `all` subcommand already runs all three; for more specific combinations add a new subcommand variant.
- This binary is Postgres-only by design. For SQLite perf, fork one; do not parameterise here.
