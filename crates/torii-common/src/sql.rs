use async_trait::async_trait;
use itertools::Itertools;
use sqlx::{Database, Executor, Postgres};
pub use sqlx::{PgPool, Transaction};
use std::borrow::Cow;
use std::sync::Arc;

pub use sqlx::Error as SqlxError;

pub type SqlxResult<T> = std::result::Result<T, SqlxError>;

#[async_trait]
pub trait Executable<DB: sqlx::Database> {
    async fn execute(self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()>;
}

#[async_trait]
impl<DB: sqlx::Database> Executable<DB> for &str
where
    for<'c> &'c mut <DB as sqlx::Database>::Connection: Executor<'c, Database = DB>,
{
    async fn execute(self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()> {
        transaction.execute(self).await?;
        Ok(())
    }
}

#[async_trait]
impl<DB: sqlx::Database> Executable<DB> for &String
where
    for<'c> &'c mut <DB as sqlx::Database>::Connection: Executor<'c, Database = DB>,
{
    async fn execute(self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()> {
        transaction.execute(self.as_str()).await?;
        Ok(())
    }
}

#[async_trait]
impl<DB: sqlx::Database> Executable<DB> for String
where
    for<'c> &'c mut <DB as sqlx::Database>::Connection: Executor<'c, Database = DB>,
{
    async fn execute(self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()> {
        transaction.execute(self.as_str()).await?;
        Ok(())
    }
}

#[async_trait]
impl<DB: sqlx::Database> Executable<DB> for Cow<'_, str>
where
    for<'c> &'c mut <DB as sqlx::Database>::Connection: Executor<'c, Database = DB>,
{
    async fn execute(self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()> {
        transaction.execute(self.as_ref()).await?;
        Ok(())
    }
}

pub struct QueryLike<S: AsRef<str>, DB: Database> {
    sql: S,
    args: Option<<DB as Database>::Arguments<'static>>,
}

impl<S: AsRef<str>, DB: Database> QueryLike<S, DB> {
    pub fn new(sql: impl Into<S>, args: <DB as Database>::Arguments<'static>) -> Self {
        Self {
            sql: sql.into(),
            args: Some(args),
        }
    }

    pub fn from_sql(sql: S) -> Self {
        Self { sql, args: None }
    }
}

#[async_trait]
impl<DB: sqlx::Database> Executable<DB> for FlexStr
where
    for<'c> &'c mut <DB as sqlx::Database>::Connection: Executor<'c, Database = DB>,
{
    async fn execute(self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()> {
        transaction.execute(self.as_ref()).await?;
        Ok(())
    }
}

pub enum FlexStr {
    Owned(String),
    Static(&'static str),
    Shared(Arc<str>),
}

impl AsRef<str> for FlexStr {
    fn as_ref(&self) -> &str {
        match self {
            FlexStr::Owned(s) => s.as_str(),
            FlexStr::Static(s) => s,
            FlexStr::Shared(s) => s,
        }
    }
}

impl PartialEq<str> for FlexStr {
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}

impl<S: AsRef<str>, DB: Database> PartialEq<str> for QueryLike<S, DB> {
    fn eq(&self, other: &str) -> bool {
        self.sql.as_ref() == other
    }
}

impl From<String> for FlexStr {
    fn from(s: String) -> Self {
        FlexStr::Owned(s)
    }
}

impl From<&'static str> for FlexStr {
    fn from(s: &'static str) -> Self {
        FlexStr::Static(s)
    }
}

impl From<Arc<str>> for FlexStr {
    fn from(s: Arc<str>) -> Self {
        FlexStr::Shared(s)
    }
}

impl<A: Database> From<FlexStr> for QueryLike<FlexStr, A> {
    fn from(sql: FlexStr) -> Self {
        QueryLike::from_sql(sql)
    }
}

impl<A: Database> From<&'static str> for QueryLike<FlexStr, A> {
    fn from(sql: &'static str) -> Self {
        QueryLike::from_sql(sql.into())
    }
}

impl<A: Database> From<String> for QueryLike<FlexStr, A> {
    fn from(sql: String) -> Self {
        QueryLike::from_sql(sql.into())
    }
}

impl<A: Database> From<Arc<str>> for QueryLike<FlexStr, A> {
    fn from(sql: Arc<str>) -> Self {
        QueryLike::from_sql(sql.into())
    }
}

pub trait Queries<DB: Database> {
    fn add(&mut self, query: impl Into<QueryLike<FlexStr, DB>>);
    fn adds(&mut self, queries: impl IntoIterator<Item = impl Into<QueryLike<FlexStr, DB>>>);
}

impl<DB: Database> Queries<DB> for Vec<QueryLike<FlexStr, DB>> {
    fn add(&mut self, query: impl Into<QueryLike<FlexStr, DB>>) {
        self.push(query.into());
    }
    fn adds(&mut self, queries: impl IntoIterator<Item = impl Into<QueryLike<FlexStr, DB>>>) {
        self.extend(queries.into_iter().map_into());
    }
}

#[async_trait]
impl<DB: sqlx::Database, const N: usize> Executable<DB> for &[String; N]
where
    for<'c> &'c mut <DB as sqlx::Database>::Connection: Executor<'c, Database = DB>,
{
    async fn execute(self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()> {
        for query in self {
            transaction.execute(query.as_str()).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<DB: sqlx::Database, T: Executable<DB> + Send> Executable<DB> for Vec<T> {
    async fn execute(self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()> {
        for item in self {
            item.execute(transaction).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<'a, DB: sqlx::Database, T> Executable<DB> for &'a Vec<T>
where
    &'a T: Executable<DB> + Send,
    T: Send + Sync,
{
    async fn execute(self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()> {
        for item in self {
            item.execute(transaction).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<'a, DB: sqlx::Database, T> Executable<DB> for &'a [T]
where
    &'a T: Executable<DB> + Send,
    T: Send + Sync,
{
    async fn execute(self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()> {
        for item in self {
            item.execute(transaction).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<S: AsRef<str> + Send, DB: sqlx::Database> Executable<DB> for QueryLike<S, DB>
where
    for<'c> &'c mut <DB as sqlx::Database>::Connection: Executor<'c, Database = DB>,
    for<'q> <DB as sqlx::Database>::Arguments<'static>: sqlx::IntoArguments<'q, DB>,
{
    async fn execute(mut self, transaction: &mut Transaction<'_, DB>) -> SqlxResult<()> {
        let sql = self.sql.as_ref();
        let args = self.args.take().unwrap_or_default();
        transaction.execute(sqlx::query_with(sql, args)).await?;
        Ok(())
    }
}

pub type PgQuery = QueryLike<FlexStr, Postgres>;
pub type MySqlQuery = QueryLike<FlexStr, sqlx::MySql>;
pub type SqliteQuery = QueryLike<FlexStr, sqlx::Sqlite>;
