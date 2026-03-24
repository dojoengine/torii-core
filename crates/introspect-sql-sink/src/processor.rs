use crate::error::{SchemaError, SchemaResult, TableLoadError};
use crate::tables::DbTables;
use crate::utils::felt_to_schema;
use introspect_types::ResultInto;
use itertools::Itertools;
use sqlx::{Database, PgPool};
use starknet_types_core::felt::Felt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use torii_common::sql::PgQuery;
use torii_introspect::events::IntrospectBody;
use torii_postgres::PostgresConnection;

pub struct IntrospectDb<T, DB> {
    tables: DbTables<DB>,
    schemas: SchemaMode,
    pool: T,
}

impl<T: PostgresConnection> PostgresConnection for IntrospectDb<T> {
    fn pool(&self) -> &PgPool {
        self.pool.pool()
    }
}

// TODO: Add errors for unknown schema
impl<T: PostgresConnection + Send + Sync> IntrospectDb<T> {
    pub fn new<S: Into<SchemaMode>>(pool: T, schemas: S) -> Self {
        Self {
            tables: PostgresTables::default(),
            schemas: schemas.into(),
            pool,
        }
    }
    pub async fn load_store_data(&self) -> PgDbResult<Vec<TableLoadError>> {
        let mut errors = Vec::new();
        let schemas = self.schemas.schemas();
        let schemas = schemas
            .as_ref()
            .map(|s| s.iter().map(AsRef::as_ref).collect());
        let table_rows = fetch_tables(self.pool(), &schemas).await?;
        let mut tables = HashMap::with_capacity(table_rows.len());
        for table in table_rows {
            let (id, table) = table.into();
            tables.insert((table.schema.clone(), id), table);
        }
        for (schema, table_id, id, column_info) in fetch_columns(self.pool(), &schemas).await? {
            if let Some(table) = tables.get_mut(&(schema.clone(), table_id)) {
                table.columns.insert(id, column_info);
            } else {
                errors.push(TableLoadError::ColumnTableNotFound(
                    schema,
                    table_id,
                    column_info.name,
                    id,
                ));
            }
        }
        for (schema, table_id, id, field) in fetch_dead_fields(self.pool(), &schemas).await? {
            if let Some(table) = tables.get_mut(&(schema.clone(), table_id)) {
                table.dead.insert(id, field);
            } else {
                errors.push(TableLoadError::TableDeadNotFound(
                    schema, table_id, field.name, id,
                ));
            }
        }
        let mut map = self.tables.write()?;
        for ((schema, id), table) in tables {
            match self.schemas.get_key(schema, id, &table.owner) {
                Ok(key) => {
                    map.insert(key, table);
                }
                Err(err) => errors.push(TableLoadError::SchemaError(err)),
            }
        }
        Ok(errors)
    }

    pub async fn initialize_introspect_pg_sink(&self) -> PgDbResult<Vec<TableLoadError>> {
        self.migrate(Some("introspect"), INTROSPECT_PG_SINK_MIGRATIONS)
            .await?;
        self.load_store_data().await
    }

    pub async fn process_messages(
        &self,
        msgs: Vec<&IntrospectBody>,
    ) -> PgDbResult<Vec<PgDbResult<()>>> {
        let mut queries = Vec::new();
        let mut results = Vec::with_capacity(msgs.len());

        for body in msgs {
            let (msg, metadata) = body.into();
            results.push(self.tables.handle_message(
                self.schemas.to_schema(&metadata.from_address)?,
                msg,
                &metadata.from_address,
                metadata.block_number.unwrap_or(u64::MAX),
                &metadata.transaction_hash,
                &mut queries,
            ));
        }
        self.process_queries(queries).await?;
        Ok(results)
    }

    pub async fn process_queries(&self, queries: Vec<PgQuery>) -> PgDbResult<()> {
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
