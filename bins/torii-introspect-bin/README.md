# Torii Introspect Bin

Dojo introspect indexer backed by PostgreSQL.

## Run

```bash
cargo run -p torii-introspect-bin -- \
  --contracts 0x123...,0x456... \
  --storage-database-url postgres://torii:torii@localhost:5432/torii
```

## Notes

- `--storage-database-url` must be PostgreSQL.
- `--observability` controls `TORII_METRICS_ENABLED` through shared helpers.
- Config validation and observability wiring are shared through `torii-config-common`.
