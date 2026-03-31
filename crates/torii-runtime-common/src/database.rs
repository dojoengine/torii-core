use anyhow::{bail, Result as AnyhowResult};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use torii_sql::connection::DbBackend;

pub const DEFAULT_SQLITE_MAX_CONNECTIONS: u32 = 500;

pub fn validate_uniform_backends(
    named_urls: &[(&str, &str)],
    _mixed_backend_message: &str,
) -> Result<DbBackend, String> {
    let ((_, first_url), rest) = named_urls
        .split_first()
        .ok_or_else(|| "at least one database URL must be provided".to_string())?;
    let expected_backend = DbBackend::from_str(first_url).map_err(|_| {
        format!("Error parsing database URL '{first_url}': Unsupported database type")
    })?;
    for (name, url) in rest {
        let backend = DbBackend::from_str(url).map_err(|_| {
            format!("Error validating database {name} URL '{url}': Unsupported database type")
        })?;
        if backend != expected_backend {
            return Err(format!("{name}={backend}({url})"));
        }
    }
    Ok(expected_backend)
}

#[derive(Debug, Clone)]
pub struct SingleDbSetup {
    pub storage_url: String,
    pub engine_url: String,
    pub database_root: PathBuf,
}

pub fn resolve_single_db_setup(db_path: &str, database_url: Option<&str>) -> SingleDbSetup {
    let database_root = Path::new(db_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();

    let storage_url = database_url.unwrap_or(db_path).to_string();
    let engine_url = database_url.map_or_else(
        || {
            database_root
                .join("engine.db")
                .to_string_lossy()
                .to_string()
        },
        ToOwned::to_owned,
    );

    SingleDbSetup {
        storage_url,
        engine_url,
        database_root,
    }
}

#[derive(Debug, Clone)]
pub struct TokenDbSetup {
    pub engine_url: String,
    pub erc20_url: String,
    pub erc721_url: String,
    pub erc1155_url: String,
    pub engine_backend: DbBackend,
    pub erc20_backend: DbBackend,
    pub erc721_backend: DbBackend,
    pub erc1155_backend: DbBackend,
}

pub fn resolve_token_db_setup(
    db_dir: &Path,
    engine_database_url: Option<&str>,
    storage_database_url: Option<&str>,
) -> AnyhowResult<TokenDbSetup> {
    let engine_url = engine_database_url.map_or_else(
        || db_dir.join("engine.db").to_string_lossy().to_string(),
        ToOwned::to_owned,
    );
    let erc20_url = resolve_storage_url(
        storage_database_url,
        engine_database_url,
        db_dir,
        "erc20.db",
    );
    let erc721_url = resolve_storage_url(
        storage_database_url,
        engine_database_url,
        db_dir,
        "erc721.db",
    );
    let erc1155_url = resolve_storage_url(
        storage_database_url,
        engine_database_url,
        db_dir,
        "erc1155.db",
    );

    let engine_backend = DbBackend::from_str(&engine_url).map_err(|_| {
        anyhow::anyhow!(
            "Error parsing engine database URL '{engine_url}': Unsupported database type"
        )
    })?;
    let erc20_backend = DbBackend::from_str(&erc20_url).map_err(|_| {
        anyhow::anyhow!("Error parsing ERC20 database URL '{erc20_url}': Unsupported database type")
    })?;

    let erc721_backend = DbBackend::from_str(&erc721_url).map_err(|_| {
        anyhow::anyhow!(
            "Error parsing ERC721 database URL '{erc721_url}': Unsupported database type"
        )
    })?;
    let erc1155_backend = DbBackend::from_str(&erc1155_url).map_err(|_| {
        anyhow::anyhow!(
            "Error parsing ERC1155 database URL '{erc1155_url}': Unsupported database type"
        )
    })?;

    if let Some(url) = storage_database_url {
        let backend: DbBackend = DbBackend::from_str(url).map_err(|_| {
            anyhow::anyhow!("Error parsing storage database URL '{url}': Unsupported database type")
        })?;
        if backend == DbBackend::Postgres
            && (erc20_backend != DbBackend::Postgres
                || erc721_backend != DbBackend::Postgres
                || erc1155_backend != DbBackend::Postgres)
        {
            bail!("Engine is configured for Postgres but one or more token storages resolved to SQLite. Set --storage-database-url to the same Postgres URL.");
        }
    }

    Ok(TokenDbSetup {
        engine_url,
        erc20_url,
        erc721_url,
        erc1155_url,
        engine_backend,
        erc20_backend,
        erc721_backend,
        erc1155_backend,
    })
}

fn resolve_storage_url(
    storage_database_url: Option<&str>,
    engine_database_url: Option<&str>,
    db_dir: &Path,
    fallback_file: &str,
) -> String {
    storage_database_url
        .map(ToOwned::to_owned)
        .or_else(|| engine_database_url.map(ToOwned::to_owned))
        .unwrap_or_else(|| db_dir.join(fallback_file).to_string_lossy().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_sqlite_defaults() {
        let db_dir = Path::new("./torii-data");
        let setup = resolve_token_db_setup(db_dir, None, None).unwrap();
        assert_eq!(setup.engine_backend, DbBackend::Sqlite);
        assert_eq!(setup.erc20_backend, DbBackend::Sqlite);
        assert!(setup.engine_url.ends_with("engine.db"));
        assert!(setup.erc20_url.ends_with("erc20.db"));
    }

    #[test]
    fn rejects_mixed_engine_and_storage_backends() {
        let db_dir = Path::new("./torii-data");
        let err = resolve_token_db_setup(
            db_dir,
            Some("postgres://localhost/torii"),
            Some("./torii-data"),
        )
        .expect_err("expected mixed backend validation error");
        assert!(err
            .to_string()
            .contains("Engine is configured for Postgres"));
    }

    #[test]
    fn resolves_postgres_for_all_targets() {
        let db_dir = Path::new("./torii-data");
        let setup = resolve_token_db_setup(
            db_dir,
            Some("postgres://localhost/torii"),
            Some("postgres://localhost/torii"),
        )
        .unwrap();
        assert_eq!(setup.engine_backend, DbBackend::Postgres);
        assert_eq!(setup.erc721_backend, DbBackend::Postgres);
    }

    #[test]
    fn validate_uniform_backends_accepts_matching_backends() {
        let backend = validate_uniform_backends(
            &[
                ("engine", "postgres://localhost/torii"),
                ("storage", "postgres://localhost/torii"),
            ],
            "mixed backends",
        )
        .unwrap();
        assert_eq!(backend, DbBackend::Postgres);
    }

    #[test]
    fn validate_uniform_backends_rejects_mixed_backends() {
        let err = validate_uniform_backends(
            &[
                ("engine", "postgres://localhost/torii"),
                ("storage", "./torii-data/introspect.db"),
            ],
            "mixed backends",
        )
        .expect_err("expected mixed backend error");
        assert!(err.contains("mixed backends"));
        assert!(err.contains("engine=Postgres"));
        assert!(err.contains("storage=Sqlite"));
    }
}
