//! Shared CLI configuration helpers for Torii binaries.

use anyhow::{bail, Result};

/// Applies the CLI-level observability toggle used by Torii.
///
/// Torii reads `TORII_METRICS_ENABLED` at runtime. Binaries can call this once
/// after argument parsing to ensure CLI flags are authoritative.
pub fn apply_observability_env(enabled: bool) {
    let value = if enabled { "true" } else { "false" };
    std::env::set_var("TORII_METRICS_ENABLED", value);
}

/// Ensures a database URL uses a PostgreSQL scheme and returns it unchanged.
///
/// `arg_name` should match the CLI flag name for ergonomic error messages.
pub fn require_postgres_url<'a>(url: &'a str, arg_name: &str) -> Result<&'a str> {
    if is_postgres_url(url) {
        Ok(url)
    } else {
        bail!("{arg_name} must be a PostgreSQL URL")
    }
}

/// Returns true when the string looks like a PostgreSQL URL.
pub fn is_postgres_url(url: &str) -> bool {
    url.starts_with("postgres://") || url.starts_with("postgresql://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_url_is_accepted() {
        assert!(require_postgres_url("postgres://localhost/torii", "--database-url").is_ok());
        assert!(require_postgres_url("postgresql://localhost/torii", "--database-url").is_ok());
    }

    #[test]
    fn non_postgres_url_is_rejected() {
        assert!(require_postgres_url("sqlite://torii.db", "--database-url").is_err());
    }
}
