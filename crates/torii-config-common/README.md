# torii-config-common

Shared **CLI configuration** helpers for Torii binaries. A tiny crate whose
only purpose is to keep clap parsers honest: same flags → same behaviour
across every binary under `bins/`.

## Role in Torii

Every binary uses `clap` to build its own `Config` struct, then calls into
this crate to (a) turn observability flags into environment variables the
core library reads (`TORII_METRICS_ENABLED`) and (b) validate that strings
purporting to be PostgreSQL URLs actually are. Everything that needs a real
database URL lives in `torii-runtime-common`; this crate is pure validation
and env plumbing.

## Architecture

```text
  clap parser (in each bin)
        |
        v
  +-----------------------------+
  |    torii-config-common      |
  |                             |
  |  apply_observability_env()  |---> sets TORII_METRICS_ENABLED
  |  is_postgres_url()          |
  |  require_postgres_url()     |---> anyhow::Error on bad URL
  +-----------------------------+
        |
        v
  torii-runtime-common::resolve_*_db_setup
        |
        v
  torii::ToriiConfigBuilder
```

## Deep Dive

### Public API

| Fn | File | Purpose |
|---|---|---|
| `apply_observability_env(enabled: bool)` | `src/lib.rs` | Sets `TORII_METRICS_ENABLED=true/false` so the metrics subsystem in `src/metrics.rs::init_from_env` picks it up |
| `is_postgres_url(url: &str) -> bool` | `src/lib.rs` | Prefix check for `postgres://` / `postgresql://` |
| `require_postgres_url(url: &str, arg_name: &str)` | `src/lib.rs` | Returns `anyhow::Error` with a user-facing message if the URL is not PostgreSQL; used by binaries that only support PG (synthetic profilers) |

### Internal Modules

- `lib.rs` — single file, < 100 LoC, no sub-modules.

### Interactions

- **Upstream (consumers)**: `bins/torii-arcade`, `bins/torii-introspect-bin`, `bins/torii-tokens`, the three `-synth` profilers.
- **Downstream deps**: `anyhow` only.
- **Workspace deps**: none.

### Extension Points

- New cross-binary CLI rules (e.g., conflicting-flag validation) belong here so every binary inherits them.
- Resist adding DB-setup logic — that belongs in `torii-runtime-common`.
