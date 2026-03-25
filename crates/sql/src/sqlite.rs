use crate::types::SqlFelt;
use crate::{AcquiredSchema, DbConnection, FlexQuery};
use futures::future::BoxFuture;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::migrate::{AppliedMigration, Migrate, MigrateError, Migration};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{query, query_as, Acquire, Decode, Encode, Executor, SqliteConnection, Type};
use std::borrow::Cow;
use std::str::FromStr;
use std::time::{Duration, Instant};

pub use sqlx::sqlite::SqliteArguments;
pub use sqlx::{Sqlite, SqlitePool};

pub type SqliteQuery = FlexQuery<Sqlite>;
pub trait SqliteDbConnection: DbConnection<Sqlite> {}
impl<T: DbConnection<Sqlite>> SqliteDbConnection for T {}

impl AcquiredSchema<Sqlite, SqliteConnection> {
    fn table_name(&self) -> Cow<'static, str> {
        Cow::Owned(format!("_sqlx_migrations_{}", self.schema))
    }
}

impl Migrate for AcquiredSchema<Sqlite, SqliteConnection> {
    fn ensure_migrations_table(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            let table_name = self.table_name();
            self.connection
                .execute(
                    format!(
                        r#"
CREATE TABLE IF NOT EXISTS "{table_name}" (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL
);
                        "#
                    )
                    .as_str(),
                )
                .await?;

            Ok(())
        })
    }

    fn dirty_version(&mut self) -> BoxFuture<'_, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            let table_name = self.table_name();
            let row: Option<(i64,)> = query_as(
                format!(
                    r#"SELECT version FROM "{table_name}" WHERE success = false ORDER BY version LIMIT 1"#
                )
                .as_str(),
            )
            .fetch_optional(&mut self.connection)
            .await?;

            Ok(row.map(|row| row.0))
        })
    }

    fn list_applied_migrations(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            let table_name = self.table_name();
            let rows: Vec<(i64, Vec<u8>)> = query_as(
                format!(r#"SELECT version, checksum FROM "{table_name}" ORDER BY version"#)
                    .as_str(),
            )
            .fetch_all(&mut self.connection)
            .await?;

            Ok(rows
                .into_iter()
                .map(|(version, checksum)| AppliedMigration {
                    version,
                    checksum: checksum.into(),
                })
                .collect())
        })
    }

    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move { Ok(()) })
    }

    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move { Ok(()) })
    }

    fn apply<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m Migration,
    ) -> BoxFuture<'m, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let table_name = self.table_name();
            let mut tx = self.begin().await?;
            let start = Instant::now();

            let _ = tx
                .execute(&*migration.sql)
                .await
                .map_err(|e| MigrateError::ExecuteMigration(e, migration.version))?;

            let _ = query(
                format!(
                    r#"
INSERT INTO "{table_name}" (version, description, success, checksum, execution_time)
VALUES (?1, ?2, TRUE, ?3, -1)
                    "#
                )
                .as_str(),
            )
            .bind(migration.version)
            .bind(&*migration.description)
            .bind(&*migration.checksum)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;

            let elapsed = start.elapsed();

            #[allow(clippy::cast_possible_truncation)]
            let _ = query(
                format!(
                    r#"
UPDATE "{table_name}"
SET execution_time = ?1
WHERE version = ?2
                    "#
                )
                .as_str(),
            )
            .bind(elapsed.as_nanos() as i64)
            .bind(migration.version)
            .execute(&mut self.connection)
            .await?;

            Ok(elapsed)
        })
    }

    fn revert<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m Migration,
    ) -> BoxFuture<'m, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let table_name = self.table_name();
            let mut tx = self.begin().await?;
            let start = Instant::now();

            let _ = tx.execute(&*migration.sql).await?;

            let _ = query(format!(r#"DELETE FROM "{table_name}" WHERE version = ?1"#).as_str())
                .bind(migration.version)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            Ok(start.elapsed())
        })
    }
}

pub fn is_sqlite_memory_path(path: &str) -> bool {
    path == ":memory:"
        || path == "sqlite::memory:"
        || path == "sqlite://:memory:"
        || path.contains("mode=memory")
}

pub fn sqlite_connect_options(path: &str) -> Result<SqliteConnectOptions, sqlx::Error> {
    if path == ":memory:" || path == "sqlite::memory:" {
        return SqliteConnectOptions::from_str("sqlite::memory:");
    }

    let options = if path.starts_with("sqlite:") {
        SqliteConnectOptions::from_str(path)?
    } else {
        SqliteConnectOptions::new().filename(path)
    };

    if path.starts_with("sqlite:") && path.contains("mode=") {
        Ok(options)
    } else {
        Ok(options.create_if_missing(true))
    }
}

impl Type<Sqlite> for SqlFelt {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <String as Type<Sqlite>>::type_info()
    }
}

impl<'q> Encode<'q, Sqlite> for SqlFelt {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, BoxDynError> {
        let mut hex = String::with_capacity(66);
        hex.push_str("0x");
        for byte in &self.0 {
            use std::fmt::Write;
            write!(hex, "{byte:02x}").unwrap();
        }
        Encode::<Sqlite>::encode(hex, buf)
    }
}

impl Decode<'_, Sqlite> for SqlFelt {
    fn decode(value: <Sqlite as sqlx::Database>::ValueRef<'_>) -> Result<Self, BoxDynError> {
        let s = <String as Decode<Sqlite>>::decode(value)?;
        let s = s.strip_prefix("0x").unwrap_or(&s);
        if s.len() != 64 {
            return Err(format!("expected 64 hex chars for felt252, got {}", s.len()).into());
        }
        let mut arr = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hi = hex_nibble(chunk[0])?;
            let lo = hex_nibble(chunk[1])?;
            arr[i] = (hi << 4) | lo;
        }
        Ok(SqlFelt(arr))
    }
}

fn hex_nibble(c: u8) -> Result<u8, BoxDynError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(format!("invalid hex char: {}", c as char).into()),
    }
}
