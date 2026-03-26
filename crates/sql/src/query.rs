use futures::future::BoxFuture;
use itertools::Itertools;
use sqlx::{Database, Executor, Transaction};
use std::sync::Arc;

use crate::SqlxResult;

pub trait Executable<DB: Database> {
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't;
}

impl<DB: Database> Executable<DB> for &str
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            transaction.execute(self).await?;
            Ok(())
        })
    }
}

impl<DB: Database> Executable<DB> for &String
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            transaction.execute(self.as_str()).await?;
            Ok(())
        })
    }
}

impl<DB: Database> Executable<DB> for String
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            transaction.execute(self.as_str()).await?;
            Ok(())
        })
    }
}

impl<DB: Database> Executable<DB> for FlexStr
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            transaction.execute(self.as_ref()).await?;
            Ok(())
        })
    }
}

pub enum FlexStr {
    Owned(String),
    Static(&'static str),
    Shared(Arc<str>),
}

impl FlexStr {
    pub fn as_str(&self) -> &str {
        match self {
            FlexStr::Owned(s) => s.as_str(),
            FlexStr::Shared(s) => s,
            FlexStr::Static(s) => s,
        }
    }
}

impl AsRef<str> for FlexStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for FlexStr {
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}

/// A query with optional bind arguments.
///
/// SQL can be any `FlexStr` (String, Arc<str>, &'static str).
/// When bind arguments are present, the SQL is stored separately as `&'static str`
/// because SQLite's `Arguments<'q>` requires the SQL lifetime to match exactly.
/// Dynamic SQL (DDL, schema operations) never has bind parameters.
pub enum FlexQuery<DB: Database> {
    Sql(FlexStr),
    Bound {
        sql: &'static str,
        args: <DB as Database>::Arguments<'static>,
    },
}

impl<DB: Database> FlexQuery<DB> {
    pub fn new(sql: &'static str, args: <DB as Database>::Arguments<'static>) -> Self {
        FlexQuery::Bound { sql, args }
    }

    pub fn from_sql(sql: impl Into<FlexStr>) -> Self {
        FlexQuery::Sql(sql.into())
    }
}

impl<DB: Database> PartialEq<str> for FlexQuery<DB> {
    fn eq(&self, other: &str) -> bool {
        match self {
            FlexQuery::Sql(s) => s.as_ref() == other,
            FlexQuery::Bound { sql, .. } => *sql == other,
        }
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

impl<A: Database> From<FlexStr> for FlexQuery<A> {
    fn from(sql: FlexStr) -> Self {
        FlexQuery::from_sql(sql)
    }
}

impl<A: Database> From<&'static str> for FlexQuery<A> {
    fn from(sql: &'static str) -> Self {
        FlexQuery::from_sql(sql)
    }
}

impl<A: Database> From<String> for FlexQuery<A> {
    fn from(sql: String) -> Self {
        FlexQuery::from_sql(sql)
    }
}

impl<A: Database> From<Arc<str>> for FlexQuery<A> {
    fn from(sql: Arc<str>) -> Self {
        FlexQuery::from_sql(sql)
    }
}

pub trait Queries<DB: Database> {
    fn add(&mut self, query: impl Into<FlexQuery<DB>>);
    fn adds(&mut self, queries: impl IntoIterator<Item = impl Into<FlexQuery<DB>>>);
}

impl<DB: Database> Queries<DB> for Vec<FlexQuery<DB>> {
    fn add(&mut self, query: impl Into<FlexQuery<DB>>) {
        self.push(query.into());
    }
    fn adds(&mut self, queries: impl IntoIterator<Item = impl Into<FlexQuery<DB>>>) {
        self.extend(queries.into_iter().map_into());
    }
}

impl<DB: Database, const N: usize> Executable<DB> for &[String; N]
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            for query in self {
                transaction.execute(query.as_str()).await?;
            }
            Ok(())
        })
    }
}

impl<DB: Database, T: Executable<DB> + Send> Executable<DB> for Vec<T> {
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            for item in self {
                item.execute(transaction).await?;
            }
            Ok(())
        })
    }
}

impl<'a, DB: Database, T> Executable<DB> for &'a Vec<T>
where
    &'a T: Executable<DB> + Send,
    T: Send + Sync,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            for item in self {
                item.execute(transaction).await?;
            }
            Ok(())
        })
    }
}

impl<'a, DB: Database, T> Executable<DB> for &'a [T]
where
    &'a T: Executable<DB> + Send,
    T: Send + Sync,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            for item in self {
                item.execute(transaction).await?;
            }
            Ok(())
        })
    }
}

impl<DB: Database> Executable<DB> for FlexQuery<DB>
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments<'static>: sqlx::IntoArguments<'static, DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            match self {
                FlexQuery::Bound { sql, args } => {
                    transaction.execute(sqlx::query_with(sql, args)).await?;
                }
                FlexQuery::Sql(sql) => {
                    transaction.execute(sql.as_ref()).await?;
                }
            }
            Ok(())
        })
    }
}
