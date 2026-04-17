# torii-erc20-synth (binary)

**Deterministic end-to-end ERC20 ingestion profiler.** Drives the ERC20
pipeline with a hand-rolled `SyntheticErc20Extractor` so the extract step
runs at local-memory speed and every other stage — decode, sink, cursor
commit — is measured cleanly. Always Postgres: the profiler assumes a
single uniform backend so results are comparable between runs.

## Role in Torii

Performance regression harness for `crates/torii-erc20`. The live binary
(`bins/torii-erc20`) has the full auto-identification + RPC path; the
synth variant replaces both with a deterministic generator so the numbers
reflect the DB path, not the RPC. Output is a machine-readable JSON
report plus (with `--features profiling`) a flamegraph, both under
`perf/runs/<run_id>/`.

## Architecture

```text
+--------------------------+
|  clap::Parser Config     |   --db-url postgres://…
|  (block_count, tx_per_   |   --from-block, --block-count
|   block, approval_ratio, |   --tx-per-block, --blocks-per-batch
|   token_count, …)        |   --approval-ratio-bps, --token-count
+------------+-------------+   --wallet-count, --seed
             |               --reset-schema (drops erc20 schema)
             v
+--------------------------+
|  EngineDb                |  postgres: DROP SCHEMA CASCADE for engine +
|  + Erc20Storage          |  erc20 (when --reset-schema), then re-init
|  + Erc20Sink             |
|  + SyntheticErc20Metadata|  names "Synthetic ERC20 AB12CD" etc.
|    CommandHandler        |  → CommandBus (parallelism=1, retries=1)
+------------+-------------+
             |
             v
+--------------------------+
|  SyntheticErc20Extractor |  deterministic RNG from --seed
|  (crates/torii-erc20:    |  emits ExtractionBatch{events, blocks, tx}
|   src/synthetic.rs)      |
+------------+-------------+
             |
             v
   Tight loop (no spawn): extractor.extract(...) →
     DecoderContext.decode_events → sink.process →
       extractor.commit_cursor → record StageSample
     (extract_ms, decode_ms, sink_ms, commit_ms, loop_total_ms)
             |
             v
+--------------------------+
|  RunReport (JSON)        |  stage p50/p95/p99/max, slow cycles,
|  perf/runs/<run_id>/     |  blocking-suspect heuristic, DB summary
|                          |  Also emits flamegraph if `profiling` feat
+--------------------------+
```

## Deep Dive

### CLI

| Flag | Default | Purpose |
|---|---|---|
| `--db-url` (env `DATABASE_URL`) | `postgres://torii:torii@localhost:5432/torii` | **Postgres only** — required for comparability |
| `--from-block` | `1_000_000` | Synthetic starting block |
| `--block-count` | `200` | Total blocks generated in this run |
| `--tx-per-block` | `1_000` | Transactions (≈ events) per block |
| `--blocks-per-batch` | `1` | Blocks in each `extract()` batch |
| `--approval-ratio-bps` | `2_000` (=20%) | Fraction of events that are approvals vs transfers |
| `--token-count` | `16` | Distinct token contracts in the workload |
| `--wallet-count` | `20_000` | Unique wallet addresses |
| `--seed` | `42` | RNG seed — same seed → same events |
| `--output-root` | `perf/runs` | Per-run output dir |
| `--reset-schema` | `true` | `DROP SCHEMA IF EXISTS engine CASCADE` + `erc20 CASCADE` before the run |

### Quick start

```bash
docker compose up -d postgres
cargo run -p torii-erc20-synth --release
```

Artifacts land in `perf/runs/<timestamp>/`:

- `report.json` — canonical stage / throughput / slow-cycle report
- `report.md` — human-readable rendering
- `context.env` — the exact env + CLI used
- `flamegraph.svg` — only when built with `--features profiling`

### Internal shape

- Uses `torii_runtime_common::sink::initialize_sink_with_command_handlers` + `drop_postgres_schemas` (this binary is the reason the schema-drop helper exists).
- `SyntheticErc20MetadataCommandHandler` generates names like `Synthetic ERC20 AB12CD` / `SAB12CD` / `decimals = 18` directly into storage, bypassing metadata fetching so measurements aren't polluted by network I/O.
- Records a `StageSample` per loop iteration; writes a `RunReport` JSON + (opt-in) flamegraph at exit.

### Workspace dependencies

`torii`, `torii-erc20`, `torii-runtime-common`, plus `clap`, `tokio`, `tracing`, `serde`, `serde_json`, `anyhow`. Feature `profiling` pulls `pprof`.

### Extension Points

- New workload shape → extend `SyntheticErc20Config` in `crates/torii-erc20/src/synthetic.rs` and add CLI flags here.
- New report field → add it to `RunReport` in `main.rs`; the JSON is the spec.
- Switch backend → not supported; use `bins/torii-erc20` for SQLite.
