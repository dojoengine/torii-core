use futures::future::BoxFuture;
use sqlx::{
    migrate::{AppliedMigration, Migrate, MigrateError, Migration, MigrationSource, Migrator},
    query, query_scalar, Acquire, PgConnection, Postgres,
};
use sqlx::{query_as, Executor};
use std::{
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
    slice,
    time::{Duration, Instant},
};

pub struct SchemaMigrator {
    pub migrator: Migrator,
    pub schema: &'static str,
}

pub struct PgAcquiredSchema<'a, A>
where
    A: Acquire<'a>,
{
    pub connection: <A as sqlx::Acquire<'a>>::Connection,
    pub schema: &'static str,
}

impl<'a, A> Deref for PgAcquiredSchema<'a, A>
where
    A: Acquire<'a, Database = Postgres>,
{
    type Target = PgConnection;
    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<'a, A> DerefMut for PgAcquiredSchema<'a, A>
where
    A: Acquire<'a, Database = Postgres>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl<'a, A> Migrate for PgAcquiredSchema<'a, A>
where
    A: Acquire<'a, Database = Postgres>,
    A: Executor<'a, Database = Postgres>,
{
    fn ensure_migrations_table(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            // language=SQL
            self.connection
                .execute(
                    format!(
                        r#"
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE TABLE IF NOT EXISTS {schema}._sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    success BOOLEAN NOT NULL,
    checksum BYTEA NOT NULL,
    execution_time BIGINT NOT NULL
);
                "#,
                        schema = self.schema
                    )
                    .as_str(),
                )
                .await?;

            Ok(())
        })
    }

    fn dirty_version(&mut self) -> BoxFuture<'_, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            // language=SQL

            let row: Option<(i64,)> = query_as(
                    format!("SELECT version FROM {schema}._sqlx_migrations WHERE success = false ORDER BY version LIMIT 1", schema = self.schema).as_str()
            )
            .fetch_optional(&mut *self.connection)
            .await?;

            Ok(row.map(|r: (i64,)| r.0))
        })
    }

    fn list_applied_migrations(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            // language=SQL
            let rows: Vec<(i64, Vec<u8>)> = query_as(
                format!(
                    "SELECT version, checksum FROM {schema}._sqlx_migrations ORDER BY version",
                    schema = self.schema
                )
                .as_str(),
            )
            .fetch_all(&mut *self.connection)
            .await?;

            let migrations = rows
                .into_iter()
                .map(|(version, checksum)| AppliedMigration {
                    version,
                    checksum: checksum.into(),
                })
                .collect();

            Ok(migrations)
        })
    }

    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            let database_name = current_database(&mut self.connection).await?;
            let lock_id = generate_lock_id(&database_name);

            // create an application lock over the database
            // this function will not return until the lock is acquired

            // https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS
            // https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS-TABLE

            // language=SQL
            let _ = query("SELECT pg_advisory_lock($1)")
                .bind(lock_id)
                .execute(&mut *self.connection)
                .await?;

            Ok(())
        })
    }

    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            let database_name = current_database(self).await?;
            let lock_id = generate_lock_id(&database_name);

            // language=SQL
            let _ = query("SELECT pg_advisory_unlock($1)")
                .bind(lock_id)
                .execute(&mut *self.connection)
                .await?;

            Ok(())
        })
    }

    fn apply<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m Migration,
    ) -> BoxFuture<'m, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let start = Instant::now();
            let schema = self.schema;
            // execute migration queries
            if migration.no_tx {
                execute_migration(self, schema, migration).await?;
            } else {
                // Use a single transaction for the actual migration script and the essential bookeeping so we never
                // execute migrations twice. See https://github.com/launchbadge/sqlx/issues/1966.
                // The `execution_time` however can only be measured for the whole transaction. This value _only_ exists for
                // data lineage and debugging reasons, so it is not super important if it is lost. So we initialize it to -1
                // and update it once the actual transaction completed.
                let mut tx = self.begin().await?;
                execute_migration(&mut tx, schema, migration).await?;
                tx.commit().await?;
            }

            // Update `elapsed_time`.
            // NOTE: The process may disconnect/die at this point, so the elapsed time value might be lost. We accept
            //       this small risk since this value is not super important.
            let elapsed = start.elapsed();

            // language=SQL
            #[allow(clippy::cast_possible_truncation)]
            let _ = query(&format!(
                r#"
    UPDATE {schema}._sqlx_migrations
    SET execution_time = $1
    WHERE version = $2
                "#,
                schema = self.schema
            ))
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
            let start = Instant::now();
            let schema = self.schema;
            // execute migration queries
            if migration.no_tx {
                revert_migration(&mut self.connection, schema, migration).await?;
            } else {
                // Use a single transaction for the actual migration script and the essential bookeeping so we never
                // execute migrations twice. See https://github.com/launchbadge/sqlx/issues/1966.
                let mut tx = self.begin().await?;
                revert_migration(&mut tx, schema, migration).await?;
                tx.commit().await?;
            }

            let elapsed = start.elapsed();

            Ok(elapsed)
        })
    }
}

async fn current_database(conn: &mut PgConnection) -> Result<String, MigrateError> {
    // language=SQL
    Ok(query_scalar("SELECT current_database()")
        .fetch_one(conn)
        .await?)
}

fn generate_lock_id(database_name: &str) -> i64 {
    const CRC_IEEE: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
    // 0x3d32ad9e chosen by fair dice roll
    0x3d32ad9e * (CRC_IEEE.checksum(database_name.as_bytes()) as i64)
}

async fn execute_migration(
    conn: &mut PgConnection,
    schema: &'static str,
    migration: &Migration,
) -> Result<(), MigrateError> {
    let _ = conn
        .execute(&*migration.sql)
        .await
        .map_err(|e| MigrateError::ExecuteMigration(e, migration.version))?;

    // language=SQL
    let _ = query(
        format!(r#"
    INSERT INTO {schema}._sqlx_migrations ( version, description, success, checksum, execution_time )
    VALUES ( $1, $2, TRUE, $3, -1 )
                "#).as_str()
    )
    .bind(migration.version)
    .bind(&*migration.description)
    .bind(&*migration.checksum)
    .execute(conn)
    .await?;

    Ok(())
}

async fn revert_migration(
    conn: &mut PgConnection,
    schema: &'static str,
    migration: &Migration,
) -> Result<(), MigrateError> {
    let _ = conn
        .execute(&*migration.sql)
        .await
        .map_err(|e| MigrateError::ExecuteMigration(e, migration.version))?;

    // language=SQL
    let _ = query(format!(r#"DELETE FROM {schema}._sqlx_migrations WHERE version = $1"#).as_str())
        .bind(migration.version)
        .execute(conn)
        .await?;

    Ok(())
}

impl SchemaMigrator {
    pub const fn new(schema: &'static str, migrator: Migrator) -> Self {
        Self { migrator, schema }
    }
    pub async fn new_from_source<'s, S>(
        schema: &'static str,
        source: S,
    ) -> Result<Self, MigrateError>
    where
        S: MigrationSource<'s>,
    {
        Migrator::new(source)
            .await
            .map(|migrator| Self { migrator, schema })
    }

    /// Specify whether applied migrations that are missing from the resolved migrations should be ignored.
    pub fn set_ignore_missing(&mut self, ignore_missing: bool) -> &Self {
        self.migrator.ignore_missing = ignore_missing;
        self
    }

    /// Specify whether or not to lock the database during migration. Defaults to `true`.
    ///
    /// ### Warning
    /// Disabling locking can lead to errors or data loss if multiple clients attempt to apply migrations simultaneously
    /// without some sort of mutual exclusion.
    ///
    /// This should only be used if the database does not support locking, e.g. CockroachDB which talks the Postgres
    /// protocol but does not support advisory locks used by SQLx's migrations support for Postgres.
    pub fn set_locking(&mut self, locking: bool) -> &Self {
        self.migrator.locking = locking;
        self
    }

    /// Get an iterator over all known migrations.
    pub fn iter(&self) -> slice::Iter<'_, Migration> {
        self.migrator.migrations.iter()
    }

    /// Check if a migration version exists.
    pub fn version_exists(&self, version: i64) -> bool {
        self.iter().any(|m| m.version == version)
    }

    /// Run any pending migrations against the database; and, validate previously applied migrations
    /// against the current migration source to detect accidental changes in previously-applied migrations.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use sqlx::migrate::MigrateError;
    /// # fn main() -> Result<(), MigrateError> {
    /// #     sqlx::__rt::test_block_on(async move {
    /// use sqlx::migrate::Migrator;
    /// use sqlx::sqlite::SqlitePoolOptions;
    ///
    /// let m = Migrator::new(std::path::Path::new("./migrations")).await?;
    /// let pool = SqlitePoolOptions::new().connect("sqlite::memory:").await?;
    /// m.run(&pool).await
    /// #     })
    /// # }
    /// ```
    pub async fn run<'a, A>(&self, migrator: A) -> Result<(), MigrateError>
    where
        A: Acquire<'a, Database = Postgres>,
        PgAcquiredSchema<'a, A>: Migrate,
    {
        let mut conn = PgAcquiredSchema {
            connection: migrator.acquire().await?,
            schema: self.schema,
        };
        self.migrator.run_direct(&mut conn).await
    }

    /// Run down migrations against the database until a specific version.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use sqlx::migrate::MigrateError;
    /// # fn main() -> Result<(), MigrateError> {
    /// #     sqlx::__rt::test_block_on(async move {
    /// use sqlx::migrate::Migrator;
    /// use sqlx::sqlite::SqlitePoolOptions;
    ///
    /// let m = Migrator::new(std::path::Path::new("./migrations")).await?;
    /// let pool = SqlitePoolOptions::new().connect("sqlite::memory:").await?;
    /// m.undo(&pool, 4).await
    /// #     })
    /// # }
    /// ```
    pub async fn undo<'a, A>(&self, migrator: A, target: i64) -> Result<(), MigrateError>
    where
        A: Acquire<'a, Database = Postgres>,
        PgAcquiredSchema<'a, A>: Migrate,
    {
        let mut conn = PgAcquiredSchema {
            connection: migrator.acquire().await?,
            schema: self.schema,
        };
        // lock the database for exclusive access by the migrator
        if self.migrator.locking {
            conn.lock().await?;
        }

        // creates [_migrations] table only if needed
        // eventually this will likely migrate previous versions of the table
        conn.ensure_migrations_table().await?;

        let version = conn.dirty_version().await?;
        if let Some(version) = version {
            return Err(MigrateError::Dirty(version));
        }

        let applied_migrations = conn.list_applied_migrations().await?;
        validate_applied_migrations(&applied_migrations, &self.migrator)?;

        let applied_migrations: HashMap<_, _> = applied_migrations
            .into_iter()
            .map(|m| (m.version, m))
            .collect();

        for migration in self
            .iter()
            .rev()
            .filter(|m| m.migration_type.is_down_migration())
            .filter(|m| applied_migrations.contains_key(&m.version))
            .filter(|m| m.version > target)
        {
            conn.revert(migration).await?;
        }

        // unlock the migrator to allow other migrators to run
        // but do nothing as we already migrated
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

    let migrations: HashSet<_> = migrator.iter().map(|m| m.version).collect();

    for applied_migration in applied_migrations {
        if !migrations.contains(&applied_migration.version) {
            return Err(MigrateError::VersionMissing(applied_migration.version));
        }
    }

    Ok(())
}
