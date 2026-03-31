# Torii Server

Dojo introspect and token indexer backed by PostgreSQL or SQLite.

## Run

```bash
cargo run --bin torii-server -- \
  --contract 0x123...,0x456... \
  --storage-database-url postgres://torii:torii@localhost:5432/torii
```

## Mixed Dojo + Token Indexing

```bash
cargo run --bin torii-server -- \
  --from-block 0 \
  --contract 0x0000 \
  --erc20 0x001,0x002 \
  --erc721 0x003,0x0045 \
  --chunk-size 1000 \
  --batch-size 10000
```

Use `--contract`/`--contracts` for Dojo introspect targets and `--erc20`, `--erc721`, `--erc1155`
for token contracts. The extractor deduplicates overlapping addresses and runs them in one ETL
pipeline.

## Throughput Tuning

The introspect indexer now exposes the same core ETL parallelism knobs as `torii-tokens`.

```bash
cargo run --bin torii-server -- \
  --contract 0x123...,0x456... \
  --storage-database-url postgres://torii:torii@localhost:5432/torii \
  --rpc-parallelism 4 \
  --max-prefetch-batches 8
```

Notes:

- `--rpc-parallelism`: concurrent chunked RPC requests (`0` = auto).
- `--chunk-size`: events per `starknet_getEvents` request.
- `--batch-size`: block range queried per iteration.
- `--max-prefetch-batches`: extracted batches buffered ahead of decode/store.

## Local TLS + ALPN

For browser-compatible local HTTPS, use `mkcert` instead of a raw self-signed certificate.
Modern browsers require a trusted local CA and `subjectAltName` entries for `localhost`.

Install and trust the local development CA:

```bash
brew install mkcert nss
mkcert -install
mkdir -p certs
mkcert -cert-file certs/dev-cert.pem -key-file certs/dev-key.pem localhost 127.0.0.1 ::1
```

Start `torii-server` with TLS enabled:

```bash
cargo run --bin torii-server -- \
  --contract 0x123...,0x456... \
  --tls-cert ./certs/dev-cert.pem \
  --tls-key ./certs/dev-key.pem
```

The local listener advertises ALPN for `h2` and `http/1.1`. Native gRPC clients will negotiate
HTTP/2 automatically; HTTPS health and metrics endpoints remain available on the same port.

Verify the local HTTPS listener:

```bash
curl https://localhost:3000/health
grpcurl -insecure localhost:3000 list
```

## Notes

- `--storage-database-url` must be PostgreSQL when provided.
- If `--storage-database-url` is omitted, SQLite files are created under `--db-dir` for introspect and token storages.
- `--observability` controls `TORII_METRICS_ENABLED` through shared helpers.
- Config validation and observability wiring are shared through `torii-config-common`.
