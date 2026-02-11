# Torii Benchmarking Harness

This repository now includes a Criterion-based benchmark harness for `torii-core`.

## Baseline Workflow

```bash
# 1) Establish and save a baseline snapshot
./scripts/perf/run_baseline.sh

# 2) Compare current code against a saved baseline
./scripts/perf/compare_to_baseline.sh perf/baselines/<timestamp>
```

The comparison script labels only changes `>=10%` as `IMPROVED` or `REGRESSED`.

## Run

```bash
# Compile only
cargo bench -p torii --bench perf_harness --no-run

# Full run
cargo bench -p torii --bench perf_harness

# Fast local sanity run
cargo bench -p torii --bench perf_harness -- --sample-size 10 --measurement-time 0.1 --warm-up-time 0.1
```

Criterion reports are written under:

```bash
target/criterion/
```

## Current Coverage

`benches/perf_harness.rs` includes these benchmark groups:

- `common_conversions`
- `decoder_throughput`
- `decoder_context`
- `etl_envelope`
- `grpc_subscription_manager`
- `sink_fanout`
- `http_core`
- `extractor_retry_policy`
- `engine_db_core`
- `erc20_storage`

### Benchmarked modules

- `crates/torii-common/src/lib.rs`
- `crates/torii-erc20/src/decoder.rs`
- `crates/torii-erc721/src/decoder.rs`
- `crates/torii-erc1155/src/decoder.rs`
- `crates/torii-erc20/src/storage.rs`
- `src/etl/decoder/context.rs`
- `src/etl/engine_db.rs`
- `src/etl/sink/multi.rs`
- `src/etl/extractor/retry.rs`
- `src/etl/envelope.rs`
- `src/grpc.rs`
- `src/http.rs`

## Latest Baseline + Comparison

Current baseline for optimization tracking:

```bash
perf/baselines/20260211T182042Z
```

Recent retained gains versus that baseline (threshold `>=10%`):

- `engine_db_core/insert_block_timestamps/32`: `+87.25%`
- `engine_db_core/insert_block_timestamps/128`: `+94.48%`
- `engine_db_core/insert_block_timestamps/512`: `+95.71%`
- `engine_db_core/insert_block_timestamps/2048`: `+96.85%`

No retained `<= -10%` regressions in the final comparison run.

## Methodology

- Deterministic fixtures are generated in the benchmark harness.
- Async decoder benchmarks run on a dedicated current-thread Tokio runtime.
- Storage benchmarks include both write-path (`insert_transfers_batch`) and read-path (`get_transfers_filtered`).
- Throughput is tracked for batch-size variants.
- Storage write benchmarks append unique rows during measurement, so compare results across
  runs only when using consistent sample/time settings.

## Next Additions

1. Add dedicated suites for `src/etl/extractor/event.rs` and `src/etl/identification/registry.rs` using fixture/replay batches.
2. Add storage benchmarks for `crates/torii-erc721/src/storage.rs` and `crates/torii-erc1155/src/storage.rs`.
3. Add a perf regression CI job and fail threshold for critical benches.
