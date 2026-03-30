use crate::query::Executable;
use crate::{AcquiredSchema, SqlxResult};
use async_trait::async_trait;
use log::LevelFilter;
use sqlx::migrate::{Migrate, Migrator};
use sqlx::pool::PoolOptions;
use sqlx::{Database, Pool, Transaction};
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

const DEFAULT_TEST_BEFORE_ACQUIRE: bool = true;
const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_MIN_CONNECTIONS: u32 = 0;
const DEFAULT_ACQUIRE_TIME_LEVEL: LevelFilter = LevelFilter::Off;
const DEFAULT_ACQUIRE_SLOW_LEVEL: LevelFilter = LevelFilter::Warn;
const DEFAULT_ACQUIRE_SLOW_THRESHOLD: Duration = Duration::from_secs(2);
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_IDLE_TIMEOUT: Option<Duration> = Some(Duration::from_secs(10 * 60));
const DEFAULT_MAX_LIFETIME: Option<Duration> = Some(Duration::from_secs(30 * 60));
const DEFAULT_FAIR: bool = true;

#[async_trait]
pub trait PoolExt<DB: Database> {
    fn pool(&self) -> &Pool<DB>;
    async fn begin(&self) -> SqlxResult<Transaction<'_, DB>> {
        Ok(self.pool().begin().await?)
    }
    async fn migrate(&self, schema: Option<&'static str>, migrator: Migrator) -> SqlxResult<()>
    where
        <DB as Database>::Connection: Migrate,
        AcquiredSchema<DB, <DB as Database>::Connection>: Migrate,
    {
        let result = match schema {
            Some(schema) => {
                let mut conn: AcquiredSchema<DB, <DB as Database>::Connection> = AcquiredSchema {
                    connection: self.pool().acquire().await?.detach(),
                    schema,
                };
                migrator.run_direct(&mut conn).await
            }
            None => migrator.run(self.pool()).await,
        };
        Ok(result?)
    }
    async fn execute_queries<E: Executable<DB> + Send>(&self, queries: E) -> SqlxResult<()> {
        let mut transaction: Transaction<'_, DB> = self.begin().await?;
        queries.execute(&mut transaction).await?;
        transaction.commit().await
    }
}

impl<DB: Database> PoolExt<DB> for Pool<DB> {
    fn pool(&self) -> &Pool<DB> {
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbType {
    Postgres,
    Sqlite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbUrl {
    Postgres(String),
    Sqlite(String),
}

pub struct DbOption<T> {
    postgres: T,
    sqlite: T,
}

impl<T> DbOption<T> {
    pub fn new(postgres: T, sqlite: T) -> Self {
        Self { postgres, sqlite }
    }

    pub fn value(self, db_type: &DbType) -> T {
        match db_type {
            DbType::Postgres => self.postgres,
            DbType::Sqlite => self.sqlite,
        }
    }
}

impl FromStr for DbUrl {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("postgres") || s.starts_with("postgresql") {
            Ok(DbUrl::Postgres(s.to_string()))
        } else if s == ":memory:" || s == "memory" {
            Ok(DbUrl::Sqlite("sqlite::memory:".to_string()))
        } else if s.starts_with("sqlite") {
            Ok(DbUrl::Sqlite(s.to_string()))
        } else {
            Err(format!("Unsupported database url: {s}"))
        }
    }
}

impl DbUrl {
    pub fn from_url(url: String) -> Option<Self> {
        if url.starts_with("postgres") || url.starts_with("postgresql") {
            Some(DbUrl::Postgres(url))
        } else if url == ":memory:" || url == "memory" {
            Some(DbUrl::Sqlite("sqlite::memory:".to_string()))
        } else if url.starts_with("sqlite") {
            Some(DbUrl::Sqlite(url))
        } else {
            None
        }
    }
    pub fn as_type(&self) -> DbType {
        match self {
            DbUrl::Postgres(_) => DbType::Postgres,
            DbUrl::Sqlite(_) => DbType::Sqlite,
        }
    }
}

impl From<DbUrl> for (DbType, String) {
    fn from(value: DbUrl) -> Self {
        match value {
            DbUrl::Postgres(url) => (DbType::Postgres, url),
            DbUrl::Sqlite(url) => (DbType::Sqlite, url),
        }
    }
}

impl Display for DbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbType::Postgres => write!(f, "PostgreSQL"),
            DbType::Sqlite => write!(f, "SQLite"),
        }
    }
}

impl DbType {
    pub fn from_url(url: &str) -> Option<Self> {
        if url.starts_with("postgres") || url.starts_with("postgresql") {
            Some(DbType::Postgres)
        } else if url.starts_with("sqlite") || url == ":memory:" || url == "memory" {
            Some(DbType::Sqlite)
        } else {
            None
        }
    }
}

impl TryFrom<&str> for DbType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        DbType::from_url(value).ok_or_else(|| {
            format!(
                "Unsupported database type in connection string: {value}. Supported types are PostgreSQL and SQLite."
            )
        })
    }
}

impl TryFrom<String> for DbType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        DbType::try_from(value.as_str())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DbPoolOptions {
    pub test_before_acquire: bool,
    pub max_connections: u32,
    pub acquire_time_level: LevelFilter,
    pub acquire_slow_level: LevelFilter,
    pub acquire_slow_threshold: Duration,
    pub acquire_timeout: Duration,
    pub min_connections: u32,
    pub max_lifetime: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub fair: bool,
}

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub url: String,
    pub options: DbPoolOptions,
}

impl PoolConfig {
    pub fn new(url: String) -> Self {
        Self {
            url,
            options: DbPoolOptions::new(),
        }
    }
    pub async fn connect<DB: Database>(&self) -> SqlxResult<Pool<DB>> {
        self.options.connect(&self.url).await
    }
    pub fn options<DB: Database>(&self) -> PoolOptions<DB> {
        self.options.options()
    }
    pub fn max_connections(mut self, max: u32) -> Self {
        self.options.max_connections = max;
        self
    }

    pub fn get_max_connections(&self) -> u32 {
        self.options.max_connections
    }

    pub fn min_connections(mut self, min: u32) -> Self {
        self.options.min_connections = min;
        self
    }

    pub fn get_min_connections(&self) -> u32 {
        self.options.min_connections
    }

    pub fn acquire_time_level(mut self, level: LevelFilter) -> Self {
        self.options.acquire_time_level = level;
        self
    }

    pub fn acquire_slow_level(mut self, level: LevelFilter) -> Self {
        self.options.acquire_slow_level = level;
        self
    }

    pub fn acquire_slow_threshold(mut self, threshold: Duration) -> Self {
        self.options.acquire_slow_threshold = threshold;
        self
    }

    pub fn get_acquire_slow_threshold(&self) -> Duration {
        self.options.acquire_slow_threshold
    }

    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.options.acquire_timeout = timeout;
        self
    }

    pub fn get_acquire_timeout(&self) -> Duration {
        self.options.acquire_timeout
    }

    pub fn max_lifetime(mut self, lifetime: impl Into<Option<Duration>>) -> Self {
        self.options.max_lifetime = lifetime.into();
        self
    }

    pub fn get_max_lifetime(&self) -> Option<Duration> {
        self.options.max_lifetime
    }

    pub fn idle_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.options.idle_timeout = timeout.into();
        self
    }

    pub fn get_idle_timeout(&self) -> Option<Duration> {
        self.options.idle_timeout
    }

    pub fn test_before_acquire(mut self, test: bool) -> Self {
        self.options.test_before_acquire = test;
        self
    }

    pub fn get_test_before_acquire(&self) -> bool {
        self.options.test_before_acquire
    }

    pub fn fair(mut self, fair: bool) -> Self {
        self.options.fair = fair;
        self
    }

    pub fn get_fair(&self) -> bool {
        self.options.fair
    }
}

impl Default for DbPoolOptions {
    fn default() -> Self {
        DbPoolOptions::new()
    }
}

impl DbPoolOptions {
    pub fn new() -> Self {
        Self {
            test_before_acquire: DEFAULT_TEST_BEFORE_ACQUIRE,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            acquire_time_level: DEFAULT_ACQUIRE_TIME_LEVEL,
            acquire_slow_level: DEFAULT_ACQUIRE_SLOW_LEVEL,
            acquire_slow_threshold: DEFAULT_ACQUIRE_SLOW_THRESHOLD,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            min_connections: DEFAULT_MIN_CONNECTIONS,
            max_lifetime: DEFAULT_MAX_LIFETIME,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            fair: DEFAULT_FAIR,
        }
    }

    pub async fn connect<DB: Database>(&self, url: &str) -> SqlxResult<Pool<DB>> {
        self.options::<DB>().connect(url).await
    }

    pub fn options<DB: Database>(&self) -> PoolOptions<DB> {
        PoolOptions::<DB>::new()
            .test_before_acquire(self.test_before_acquire)
            .max_connections(self.max_connections)
            .acquire_time_level(self.acquire_time_level)
            .acquire_slow_level(self.acquire_slow_level)
            .acquire_slow_threshold(self.acquire_slow_threshold)
            .acquire_timeout(self.acquire_timeout)
            .min_connections(self.min_connections)
            .max_lifetime(self.max_lifetime)
            .idle_timeout(self.idle_timeout)
            .__fair(self.fair)
    }

    pub fn max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    pub fn get_max_connections(&self) -> u32 {
        self.max_connections
    }

    pub fn min_connections(mut self, min: u32) -> Self {
        self.min_connections = min;
        self
    }

    pub fn get_min_connections(&self) -> u32 {
        self.min_connections
    }

    pub fn acquire_time_level(mut self, level: LevelFilter) -> Self {
        self.acquire_time_level = level;
        self
    }

    pub fn acquire_slow_level(mut self, level: LevelFilter) -> Self {
        self.acquire_slow_level = level;
        self
    }

    pub fn acquire_slow_threshold(mut self, threshold: Duration) -> Self {
        self.acquire_slow_threshold = threshold;
        self
    }

    pub fn get_acquire_slow_threshold(&self) -> Duration {
        self.acquire_slow_threshold
    }

    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    pub fn get_acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }

    pub fn max_lifetime(mut self, lifetime: impl Into<Option<Duration>>) -> Self {
        self.max_lifetime = lifetime.into();
        self
    }

    pub fn get_max_lifetime(&self) -> Option<Duration> {
        self.max_lifetime
    }

    pub fn idle_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.idle_timeout = timeout.into();
        self
    }

    pub fn get_idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }

    pub fn test_before_acquire(mut self, test: bool) -> Self {
        self.test_before_acquire = test;
        self
    }

    pub fn get_test_before_acquire(&self) -> bool {
        self.test_before_acquire
    }

    pub fn fair(mut self, fair: bool) -> Self {
        self.fair = fair;
        self
    }

    pub fn get_fair(&self) -> bool {
        self.fair
    }
}
