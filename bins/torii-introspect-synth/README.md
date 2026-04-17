# torii-introspect-synth (binary)

**Deterministic Dojo introspect profiler.** Generates a stream of
synthetic `ModelWithSchemaRegistered` + `StoreSetRecord` /
`StoreUpdateMember` events, pumps them through `DojoDecoder` and
`introspect-sql-sink`, and writes a timing report. Postgres-only, same
family as `torii-erc20-synth` / `torii-tokens-synth`.

## Role in Torii

Perf regression harness for the introspect path. Keeps the decoder +
`IntrospectPgDb` hot without requiring a live Dojo world — useful when
changing schema generation, JSON serialisation, or the per-record write
path in `introspect-sql-sink`.

## Architecture

```text
+--------------------------+
|  clap::Parser  Config    |  --db-url (Postgres)
|                          |  --from-block, --block-count
|                          |  --records-per-block
|                          |  --blocks-per-batch, --seed
|                          |  --output-root, --reset-schema
+------------+-------------+
             |
             v
+--------------------------+
|  Synthetic extractor     |  bakes a single model:
|  (in main.rs)            |    NAMESPACE="synthetic"
|                          |    MODEL_NAME="position"
|                          |    TABLE_NAME="synthetic-position"
|                          |    FROM_ADDRESS=0x100
|                          |  emits ModelWithSchemaRegistered first,
|                          |  then --records-per-block StoreSetRecord /
|                          |  StoreUpdateMember events per block,
|                          |  deterministic via --seed
+------------+-------------+
             |
             v
+--------------------------+
|  DojoDecoder  +          |  fetcher = in-process DojoSchemaFetcher that
|  IntrospectPgDb          |  returns the synthetic schema (no RPC);
|                          |  DecoderContext routes to DojoDecoder
+------------+-------------+
             |
             v
   tight loop: extract → decode → sink.process → commit_cursor
     record StageSample (extract/decode/sink/commit/loop_total)
             |
             v
+--------------------------+
|  RunReport JSON          |  perf/synthetic-runs/<run_id>/
+--------------------------+
```

## Deep Dive

### CLI

| Flag | Default | Purpose |
|---|---|---|
| `--db-url` (env `DATABASE_URL`) | `postgres://torii:torii@localhost:5432/torii` | **Postgres only** |
| `--from-block` | `1_000_000` | Synthetic starting block |
| `--block-count` | `50` | Blocks produced in the run |
| `--records-per-block` | `250` | `StoreSetRecord` / `StoreUpdateMember` events per block |
| `--blocks-per-batch` | `1` | Blocks per extract cycle |
| `--seed` | `42` | Deterministic RNG seed |
| `--output-root` | `perf/synthetic-runs` | Artifact root |
| `--reset-schema` | `true` | Drop and recreate the synthetic schema before the run |

### Internal shape

- Hand-rolls a `DojoSchemaFetcher` that returns a static schema for the synthetic model — no RPC is called.
- Uses `IntrospectPgDb` directly (not `introspect-sql-sink::runtime::IntrospectDb` dispatch) to keep the critical path narrow.
- Persists extractor state under `EXTRACTOR_TYPE = "synthetic_introspect"` / `STATE_KEY = "last_block"` in `EngineDb` so restarts pick up where they stopped (unless `--reset-schema`).

### Workspace dependencies

`torii`, `torii-dojo`, `torii-introspect`, `torii-introspect-sql-sink` (feature `postgres`), `torii-types`, `dojo-introspect`, `introspect-types`, `sqlx` (postgres), `clap`, `serde`, `async-trait`, `tokio`, `starknet-types-raw`, `tracing`, `anyhow`.

### Extension Points

- New event mix → widen the synthetic extractor's per-block generator (add more `StoreUpdateRecord` variants, more models, etc.).
- Additional model → bump `NAMESPACE` / `MODEL_NAME` constants and the fetcher's schema map.
- Switch to SQLite → not supported; the binary targets `IntrospectPgDb` explicitly.
