use futures::future::BoxFuture;
use sqlx::migrate::{
    AppliedMigration, Migrate, MigrateError, Migration, MigrationSource, Migrator,
};
use sqlx::sqlite::SqliteConnection;
use sqlx::{query, query_as, Acquire, Executor, Sqlite};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::slice;
use std::time::{Duration, Instant};

pub struct NamespaceMigrator {
    pub migrator: Migrator,
    pub namespace: &'static str,
}

pub struct SqliteAcquiredNamespace<'a, A>
where
    A: Acquire<'a>,
{
    pub connection: <A as sqlx::Acquire<'a>>::Connection,
    pub namespace: &'static str,
}

impl<'a, A> Deref for SqliteAcquiredNamespace<'a, A>
where
    A: Acquire<'a, Database = Sqlite>,
{
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<'a, A> DerefMut for SqliteAcquiredNamespace<'a, A>
where
    A: Acquire<'a, Database = Sqlite>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl<'a, A> SqliteAcquiredNamespace<'a, A>
where
    A: Acquire<'a, Database = Sqlite>,
{
    fn table_name(&self) -> Cow<'static, str> {
        Cow::Owned(format!("_sqlx_migrations_{}", self.namespace))
    }
}

impl<'a, A> Migrate for SqliteAcquiredNamespace<'a, A>
where
    A: Acquire<'a, Database = Sqlite> + Executor<'a, Database = Sqlite>,
{
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
            .fetch_optional(&mut *self.connection)
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
            .fetch_all(&mut *self.connection)
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
            .execute(&mut *self.connection)
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

impl NamespaceMigrator {
    pub const fn new(namespace: &'static str, migrator: Migrator) -> Self {
        Self {
            migrator,
            namespace,
        }
    }

    pub async fn new_from_source<'s, S>(
        namespace: &'static str,
        source: S,
    ) -> Result<Self, MigrateError>
    where
        S: MigrationSource<'s>,
    {
        Migrator::new(source).await.map(|migrator| Self {
            migrator,
            namespace,
        })
    }

    pub fn set_ignore_missing(&mut self, ignore_missing: bool) -> &Self {
        self.migrator.ignore_missing = ignore_missing;
        self
    }

    pub fn set_locking(&mut self, locking: bool) -> &Self {
        self.migrator.locking = locking;
        self
    }

    pub fn iter(&self) -> slice::Iter<'_, Migration> {
        self.migrator.migrations.iter()
    }

    pub fn version_exists(&self, version: i64) -> bool {
        self.iter().any(|migration| migration.version == version)
    }

    pub async fn run<'a, A>(&self, migrator: A) -> Result<(), MigrateError>
    where
        A: Acquire<'a, Database = Sqlite>,
        SqliteAcquiredNamespace<'a, A>: Migrate,
    {
        let mut conn = SqliteAcquiredNamespace {
            connection: migrator.acquire().await?,
            namespace: self.namespace,
        };
        self.migrator.run_direct(&mut conn).await
    }

    pub async fn undo<'a, A>(&self, migrator: A, target: i64) -> Result<(), MigrateError>
    where
        A: Acquire<'a, Database = Sqlite>,
        SqliteAcquiredNamespace<'a, A>: Migrate,
    {
        let mut conn = SqliteAcquiredNamespace {
            connection: migrator.acquire().await?,
            namespace: self.namespace,
        };

        if self.migrator.locking {
            conn.lock().await?;
        }

        conn.ensure_migrations_table().await?;

        if let Some(version) = conn.dirty_version().await? {
            return Err(MigrateError::Dirty(version));
        }

        let applied_migrations = conn.list_applied_migrations().await?;
        validate_applied_migrations(&applied_migrations, &self.migrator)?;

        let applied_migrations: HashMap<_, _> = applied_migrations
            .into_iter()
            .map(|migration| (migration.version, migration))
            .collect();

        for migration in self
            .iter()
            .rev()
            .filter(|migration| migration.migration_type.is_down_migration())
            .filter(|migration| applied_migrations.contains_key(&migration.version))
            .filter(|migration| migration.version > target)
        {
            conn.revert(migration).await?;
        }

        if self.migrator.locking {
            conn.unlock().await?;
        }

        Ok(())
    }
}

fn validate_applied_migrations(
    applied_migrations: &[AppliedMigration],
    migrator: &Migrator,
) -> Result<(), MigrateError> {
    if migrator.ignore_missing {
        return Ok(());
    }

    let migrations: HashSet<_> = migrator.iter().map(|migration| migration.version).collect();

    for applied_migration in applied_migrations {
        if !migrations.contains(&applied_migration.version) {
            return Err(MigrateError::VersionMissing(applied_migration.version));
        }
    }

    Ok(())
}
