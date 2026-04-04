use crate::processor::{DbColumn, DbDeadField, DbTable, IntrospectTxEvents, COMMIT_CMD};
use crate::table::Table;
use crate::tables::Tables;
use crate::{DbResult, NamespaceMode, RecordResult, TableResult};
use async_trait::async_trait;
use introspect_types::{ColumnDef, PrimaryDef};
use sqlx::{Database, Pool};
use starknet_types_core::felt::Felt;
use torii::etl::envelope::TypedTransactionsMsgs;
use torii_introspect::events::IntrospectBody;
use torii_introspect::tables::RecordSchema;
use torii_introspect::Record;
use torii_sql::{Executable, FlexQuery, PoolExt};

#[async_trait]
pub trait IntrospectProcessor {
    async fn process_batch(
        &self,
        tables: &Tables,
        namespaces: &NamespaceMode,
        batch: &[IntrospectTxEvents],
    ) -> DbResult<Vec<DbResult<()>>>;
}

#[async_trait]
pub trait IntrospectInitialize {
    async fn initialize(&self) -> DbResult<()>;
    async fn load_tables(&self, namespaces: &Option<Vec<String>>) -> DbResult<Vec<DbTable>>;
    async fn load_columns(&self, namespaces: &Option<Vec<String>>) -> DbResult<Vec<DbColumn>>;
    async fn load_dead_fields(
        &self,
        namespaces: &Option<Vec<String>>,
    ) -> DbResult<Vec<DbDeadField>>;
}

#[allow(clippy::too_many_arguments)]
pub trait IntrospectQueryMaker: Database {
    fn create_table_queries(
        namespace: &str,
        id: &Felt,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<Self>>,
    ) -> TableResult<()>;
    fn update_table_queries(
        table: &mut Table,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<Self>>,
    ) -> TableResult<()>;
    fn insert_record_queries(
        namespace: &str,
        table_name: &str,
        schema: &RecordSchema<'_>,
        records: &[Record],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<Self>>,
    ) -> RecordResult<()>;

    fn transaction_to_queries(
        tables: &Tables,
        namespaces: &NamespaceMode,
        transaction: &IntrospectTxEvents,
        queries: &mut Vec<FlexQuery<Self>>,
        results: &mut Vec<DbResult>,
    ) -> DbResult {
        for msg in &transaction.msgs {
            results.push(tables.handle_message::<Self>(
                namespaces.to_namespace(&transaction.from_address)?,
                msg,
                &transaction.from_address,
                transaction.block_number,
                &transaction.transaction_hash,
                queries,
            ));
        }
        Ok(())
    }
    fn msgs_to_queries(
        tables: &Tables,
        namespaces: &NamespaceMode,
        transactions: &[IntrospectTxEvents],
        queries: &mut Vec<FlexQuery<Self>>,
    ) -> DbResult<Vec<DbResult>> {
        let mut results: Vec<DbResult> = Vec::with_capacity(transactions.event_count());
        for transaction in transactions {
            Self::transaction_to_queries(tables, namespaces, transaction, queries, &mut results)?;
        }
        Ok(results)
    }
}

#[async_trait]
pub trait IntrospectPool<DB: IntrospectQueryMaker> {
    async fn process_queries(&self, queries: Vec<FlexQuery<DB>>) -> DbResult<()>;
    async fn execute_msgs(
        &self,
        tables: &Tables,
        namespaces: &NamespaceMode,
        batch: &[IntrospectTxEvents],
    ) -> DbResult<Vec<DbResult<()>>> {
        let mut queries = Vec::new();
        let results = DB::msgs_to_queries(tables, namespaces, msgs, &mut queries)?;
        self.process_queries(queries).await?;
        Ok(results)
    }
}

#[async_trait]
impl<DB: IntrospectQueryMaker> IntrospectPool<DB> for Pool<DB>
where
    Vec<FlexQuery<DB>>: Executable<DB>,
{
    async fn process_queries(&self, queries: Vec<FlexQuery<DB>>) -> DbResult<()> {
        let mut batch = Vec::new();
        for query in queries {
            if query == *COMMIT_CMD {
                self.execute_queries(std::mem::take(&mut batch)).await?;
            } else {
                batch.push(query);
            }
        }
        if !batch.is_empty() {
            self.execute_queries(batch).await?;
        }
        Ok(())
    }
}
