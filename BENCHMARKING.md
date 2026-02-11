# Torii Benchmarking Harness

This repository now includes a Criterion-based benchmark harness for `torii-core`.

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
- `etl_envelope`
- `grpc_subscription_manager`
- `erc20_storage`

### Benchmarked modules

- `crates/torii-common/src/lib.rs`
- `crates/torii-erc20/src/decoder.rs`
- `crates/torii-erc721/src/decoder.rs`
- `crates/torii-erc1155/src/decoder.rs`
- `crates/torii-erc20/src/storage.rs`
- `src/etl/envelope.rs`
- `src/grpc.rs`

## Methodology

- Deterministic fixtures are generated in the benchmark harness.
- Async decoder benchmarks run on a dedicated current-thread Tokio runtime.
- Storage benchmarks include both write-path (`insert_transfers_batch`) and read-path (`get_transfers_filtered`).
- Throughput is tracked for batch-size variants.
- Storage write benchmarks append unique rows during measurement, so compare results across
  runs only when using consistent sample/time settings.

## Next Additions

1. Add dedicated suites for `src/etl/decoder/context.rs` and `src/etl/extractor/event.rs` using fixture/replay batches.
2. Add storage benchmarks for `crates/torii-erc721/src/storage.rs` and `crates/torii-erc1155/src/storage.rs`.
3. Add a perf regression CI job and fail threshold for critical benches.
