# torii-config-common

Shared CLI configuration helpers for Torii binaries.

## What it provides

- `apply_observability_env(enabled: bool)`
  - Sets `TORII_METRICS_ENABLED` from CLI flags so runtime behavior is explicit.
- `require_postgres_url(url, arg_name)`
  - Validates that a URL is PostgreSQL and returns a user-facing error if not.

## Example

```rust
use torii_config_common::{apply_observability_env, require_postgres_url};

apply_observability_env(config.observability);
let storage_url = require_postgres_url(&config.storage_database_url, "--storage-database-url")?;
```
