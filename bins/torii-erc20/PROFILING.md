# Performance Profiling Guide

This guide explains how to profile the `torii-erc20` binary to identify performance bottlenecks.

## Built-in Timing Instrumentation

The binary includes built-in timing instrumentation that measures:
- **Extract time**: Time to fetch blocks and events from RPC
- **Decode time**: Time to decode events (includes contract identification)
- **Sink time**: Time to process events and update database
- **Total loop time**: End-to-end time for each batch

### Running with Timing Logs

Simply run the binary normally and watch the INFO logs:

```bash
cargo run --release -p torii-erc20 -- --from-block 600000 --to-block 600100
```

Example output:
```
📦 Batch #1: Extracted 1234 events from 10 blocks (extract_time: 450.23ms)
   ✓ Decoded into 156 envelopes (decode_time: 89.45ms)
   ✓ Processed through sink (sink_time: 23.12ms) | Total loop: 562.80ms
```

This quickly shows which phase is slow:
- High **extract_time**: RPC is slow or network latency
- High **decode_time**: Contract identification or event parsing bottleneck
- High **sink_time**: Database writes or balance calculations slow
- High **total loop** but low individual times: Other overhead

## Flamegraph Generation (Detailed CPU Profiling)

For detailed CPU profiling, you can generate flamegraphs to see exactly where CPU time is spent.

### Option 1: Using Built-in pprof (Recommended)

Build with the `profiling` feature enabled:

```bash
cargo build --release -p torii-erc20 --features profiling
```

Run the binary (it will generate `flamegraph.svg` when it exits):

```bash
./target/release/torii-erc20 --from-block 600000 --to-block 600100
```

When the indexing completes, open the flamegraph:

```bash
# macOS
open flamegraph.svg

# Linux
xdg-open flamegraph.svg
```

### Option 2: Using cargo-flamegraph

Install cargo-flamegraph:

```bash
cargo install flamegraph
```

On Linux, you may need to allow perf access:

```bash
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

Run with flamegraph:

```bash
cargo flamegraph --bin torii-erc20 -- --from-block 600000 --to-block 600100
```

This generates `flamegraph.svg` in the current directory.

## Reading Flamegraphs

Flamegraphs show CPU time from bottom (root) to top (leaves):
- **Width** = how much CPU time was spent
- **Y-axis** = call stack depth
- Hover over boxes to see function names and percentages
- Click boxes to zoom in

Look for:
- **Wide boxes**: Functions consuming lots of CPU time
- **Tall stacks**: Deep call chains that might be optimized
- **Unexpected functions**: Code paths that shouldn't be hot

### Common Bottlenecks

1. **Contract identification** (`ContractRegistry::identify_contract`)
   - Network calls to RPC for contract introspection
   - Solution: Use explicit mappings, reduce RPC calls

2. **Event decoding** (`Decoder::decode`)
   - Parsing event data
   - Solution: Optimize parsing logic, batch processing

3. **Database writes** (`rusqlite::Connection::execute`)
   - SQLite insertions and updates
   - Solution: Batch writes, transactions, indexing

4. **Mutex locking** (`std::sync::Mutex`)
   - Lock contention in registry
   - Solution: See optimization notes in `multi.rs`

## Profiling Different Scenarios

### 1. Cold Start (No Cache)

Test with fresh database to measure identification overhead:

```bash
rm erc20-data.db
cargo run --release -p torii-erc20 --features profiling -- --from-block 600000 --to-block 600010
```

This shows identification cost for new contracts.

### 2. Warm Cache

Run twice on same blocks:

```bash
rm erc20-data.db
cargo run --release -p torii-erc20 -- --from-block 600000 --to-block 600010
cargo run --release -p torii-erc20 --features profiling -- --from-block 600000 --to-block 600010
```

Second run shows cached identification performance.

### 3. Auto-Discovery vs Explicit

Compare auto-discovery overhead:

```bash
# With auto-discovery
cargo run --release -p torii-erc20 --features profiling -- --from-block 600000 --to-block 600010

# Without auto-discovery (explicit only)
rm erc20-data.db
cargo run --release -p torii-erc20 --features profiling -- --no-auto-discovery --from-block 600000 --to-block 600010
```

### 4. Large Batch

Test with many blocks to see sustained performance:

```bash
cargo run --release -p torii-erc20 --features profiling -- --from-block 600000 --to-block 610000
```

## System-Level Profiling (Advanced)

### Linux: perf

```bash
# Record
perf record -g --call-graph dwarf ./target/release/torii-erc20 --from-block 600000 --to-block 600100

# View report
perf report

# Generate flamegraph
git clone https://github.com/brendangregg/FlameGraph
perf script | FlameGraph/stackcollapse-perf.pl | FlameGraph/flamegraph.pl > flamegraph.svg
```

### macOS: Instruments

```bash
# Use Xcode's Instruments Time Profiler
instruments -t "Time Profiler" ./target/release/torii-erc20 --from-block 600000 --to-block 600100
```

## Optimizing Based on Findings

After profiling, common optimization paths:

1. **If extract is slow**:
   - Use faster RPC endpoint
   - Increase batch size
   - Add connection pooling

2. **If decode is slow**:
   - Add explicit mappings for known contracts
   - Optimize event parsing
   - See mutex optimization notes in `src/etl/decoder/multi.rs`

3. **If sink is slow**:
   - Batch database writes
   - Use transactions
   - Optimize balance calculation (use proper u256)
   - Add database indexes

4. **If total is high but phases are fast**:
   - Check tokio runtime overhead
   - Look for unnecessary async boundaries
   - Profile memory allocations

## Memory Profiling

### Using valgrind (Linux)

```bash
valgrind --tool=massif ./target/release/torii-erc20 --from-block 600000 --to-block 600100
ms_print massif.out.* > memory-profile.txt
```

### Using heaptrack (Linux)

```bash
heaptrack ./target/release/torii-erc20 --from-block 600000 --to-block 600100
heaptrack_gui heaptrack.torii-erc20.*.gz
```

## Continuous Profiling

For production monitoring, consider:
- Log timing metrics to a time-series database
- Set up alerts for slow batches
- Track performance regression in CI

Example: Export metrics to Prometheus/Grafana for visualization.
