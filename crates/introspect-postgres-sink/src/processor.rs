use crate::error::{SchemaError, SchemaResult, TableLoadError};
use crate::query::{fetch_columns, fetch_dead_fields, fetch_tables, COMMIT_CMD};
use crate::tables::{PgSchema, PostgresTables, TableKey};
use crate::utils::felt_to_schema;
use crate::{PgDbError, PgDbResult, INTROSPECT_PG_SINK_MIGRATIONS};
use introspect_types::ResultInto;
use itertools::Itertools;
use sqlx::PgPool;
use starknet_types_core::felt::Felt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use torii_common::sql::PgQuery;
use torii_introspect::events::IntrospectBody;
use torii_postgres::PostgresConnection;

pub struct IntrospectPgDb<T> {
    tables: PostgresTables,
    schemas: SchemaMode,
    pool: T,
}

pub enum SchemaMode {
    Single(Arc<str>),
    Address,
    Named(HashMap<Felt, Arc<str>>),
    Addresses(HashSet<Felt>),
}

impl From<String> for SchemaMode {
    fn from(value: String) -> Self {
        SchemaMode::Single(value.into())
    }
}

impl From<&str> for SchemaMode {
    fn from(value: &str) -> Self {
        SchemaMode::Single(value.into())
    }
}

impl From<HashMap<Felt, String>> for SchemaMode {
    fn from(value: HashMap<Felt, String>) -> Self {
        SchemaMode::Named(
            value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
        )
    }
}

impl<const N: usize> From<[(Felt, &str); N]> for SchemaMode {
    fn from(value: [(Felt, &str); N]) -> Self {
        SchemaMode::Named(
            value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
        )
    }
}

impl<const N: usize> From<[(Felt, String); N]> for SchemaMode {
    fn from(value: [(Felt, String); N]) -> Self {
        SchemaMode::Named(
            value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
        )
    }
}

impl<const N: usize> From<[Felt; N]> for SchemaMode {
    fn from(value: [Felt; N]) -> Self {
        SchemaMode::Addresses(value.into_iter().collect())
    }
}

impl From<Vec<Felt>> for SchemaMode {
    fn from(value: Vec<Felt>) -> Self {
        SchemaMode::Addresses(value.into_iter().collect())
    }
}

fn felt_try_from_schema(schema: &str) -> SchemaResult<Felt> {
    match schema.len() == 63 {
        true => Felt::from_hex(schema).err_into(),
        false => Err(SchemaError::InvalidAddressLength(schema.to_string())),
    }
}

impl SchemaMode {
    pub fn schemas(&self) -> Option<Vec<Arc<str>>> {
        match self {
            SchemaMode::Single(name) => Some(vec![name.clone()]),
            SchemaMode::Address => None,
            SchemaMode::Named(map) => Some(map.values().cloned().collect()),
            SchemaMode::Addresses(set) => Some(set.iter().map(felt_to_schema).map_into().collect()),
        }
    }

    pub fn get_schema_key(&self, schema: String, owner: &Felt) -> SchemaResult<PgSchema> {
        match self {
            SchemaMode::Single(s) => match **s == *schema {
                true => Ok(PgSchema::Single(s.clone())),
                false => Err(SchemaError::SchemaMismatch(schema, s.to_string())),
            },
            SchemaMode::Address => felt_try_from_schema(&schema).map(PgSchema::Address),
            SchemaMode::Named(map) => match map.get(owner) {
                Some(s) if **s == *schema => Ok(PgSchema::Named(s.clone())),
                Some(s) => Err(SchemaError::SchemaMismatch(schema, s.to_string())),
                None => Err(SchemaError::AddressNotFound(*owner, schema)),
            },
            SchemaMode::Addresses(set) => {
                let address = felt_try_from_schema(&schema)?;
                match set.contains(&address) {
                    true => Ok(PgSchema::Address(address)),
                    false => Err(SchemaError::AddressNotFound(address, schema)),
                }
            }
        }
    }

    pub fn get_key(&self, schema: String, id: Felt, owner: &Felt) -> SchemaResult<TableKey> {
        self.get_schema_key(schema, owner)
            .map(|k| TableKey::new(k, id))
    }

    pub fn to_schema(&self, from_address: &Felt) -> PgDbResult<PgSchema> {
        match self {
            SchemaMode::Single(name) => Ok(PgSchema::Single(name.clone())),
            SchemaMode::Address => Ok(PgSchema::Address(*from_address)),
            SchemaMode::Named(map) => match map.get(from_address) {
                Some(schema) => Ok(PgSchema::Named(schema.clone())),
                None => Err(PgDbError::SchemaNotFound(*from_address)),
            },
            SchemaMode::Addresses(set) => match set.contains(from_address) {
                true => Ok(PgSchema::Address(*from_address)),
                false => Err(PgDbError::SchemaNotFound(*from_address)),
            },
        }
    }
}

impl<T: PostgresConnection> PostgresConnection for IntrospectPgDb<T> {
    fn pool(&self) -> &PgPool {
        self.pool.pool()
    }
}

// TODO: Add errors for unknown schema
impl<T: PostgresConnection + Send + Sync> IntrospectPgDb<T> {
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
