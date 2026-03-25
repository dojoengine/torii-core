use super::insert::insert_record_queries;
use super::query::{insert_columns_query, insert_table_query, CreatePgTable};
use super::upgrade::PgTableUpgrade;
use crate::processor::IntrospectDb;
use crate::{
    IntrospectQueryMaker, IntrospectSqlSink, RecordResult, Table, TableError, TableResult,
};
use async_trait::async_trait;
use introspect_types::schema::{Names, TypeDefs};
use introspect_types::{ColumnDef, FeltIds, PrimaryDef};
use starknet_types_core::felt::Felt;
use torii_introspect::tables::RecordSchema;
use torii_introspect::Record;
use torii_sql::{DbConnection, PgPool, PgQuery, Postgres};

pub type IntrospectPgDb<T> = IntrospectDb<PostgresBackend<T>>;

pub struct PostgresBackend<T: DbConnection<Postgres>>(T);

impl<T: DbConnection<Postgres>> From<T> for PostgresBackend<T> {
    fn from(value: T) -> Self {
        PostgresBackend(value)
    }
}

impl<T: DbConnection<Postgres>> DbConnection<Postgres> for PostgresBackend<T> {
    fn pool(&self) -> &PgPool {
        self.0.pool()
    }
}

#[async_trait]
impl<T: DbConnection<Postgres> + Send + Sync> IntrospectQueryMaker for PostgresBackend<T> {
    type DB = Postgres;

    fn create_table_queries(
        namespace: &str,
        id: &Felt,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> TableResult<()> {
        let ns = namespace.into();
        CreatePgTable::new(&ns, id, name, primary, columns)?.make_queries(queries);
        store_table_queries(
            namespace,
            id,
            name,
            primary,
            columns,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
    }
    fn update_table_queries(
        table: &mut Table,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> TableResult<()> {
        let upgrades = table.upgrade_table(name, primary, columns)?;
        upgrades.to_queries(block_number, transaction_hash, queries)?;
        let columns = table.columns_with_ids(&upgrades.columns_upgraded)?;
        store_table_queries(
            &table.namespace,
            &table.id,
            name,
            primary,
            columns,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
    }
    fn insert_record_queries(
        namespace: &str,
        table_name: &str,
        schema: &RecordSchema<'_>,
        records: &[Record],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> RecordResult<()> {
        insert_record_queries(
            namespace,
            table_name,
            schema,
            records,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
    }
}

fn store_table_queries<CS>(
    schema: &str,
    id: &Felt,
    name: &str,
    primary: &PrimaryDef,
    columns: CS,
    from_address: &Felt,
    block_number: u64,
    transaction_hash: &Felt,
    queries: &mut Vec<PgQuery>,
) -> TableResult<()>
where
    CS: Names + FeltIds + TypeDefs,
{
    queries.push(
        insert_table_query(
            &schema,
            &id,
            &name,
            &primary,
            from_address,
            block_number,
            transaction_hash,
        )
        .map_err(TableError::Encode)?,
    );

    queries.push(
        insert_columns_query(&schema, &id, columns, block_number, transaction_hash)
            .map_err(TableError::Encode)?,
    );
    Ok(())
}

impl<T: DbConnection<Postgres>> IntrospectSqlSink for PostgresBackend<T> {
    const NAME: &'static str = "Introspect Postgres";
}
