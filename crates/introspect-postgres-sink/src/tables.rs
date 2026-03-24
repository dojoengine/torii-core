use crate::table::PgTable;
use crate::{PgDbError, PgDbResult};
use introspect_types::FeltIds;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use torii_common::sql::PgQuery;
use torii_introspect::events::IntrospectMsg;
use torii_introspect::schema::TableSchema;
use torii_introspect::InsertsFields;

pub const DEAD_MEMBERS_TABLE: &str = "__introspect_dead_fields";
pub const TABLES_TABLE: &str = "__introspect_tables";
pub const COLUMNS_TABLE: &str = "__introspect_columns";

#[derive(Debug, Default)]
pub struct PostgresTables(pub RwLock<HashMap<TableKey, PgTable>>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableKey {
    schema: PgSchema,
    id: Felt,
}

impl TableKey {
    pub fn new(schema: PgSchema, id: Felt) -> Self {
        Self { schema, id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgSchema {
    Single(Arc<str>),
    Address(Felt),
    Named(Arc<str>),
}

impl Display for TableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !matches!(self.schema, PgSchema::Single(_)) {
            write!(f, "{} ", self.schema)?;
        }
        write!(f, "{:#063x}", self.id)
    }
}

impl Display for PgSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgSchema::Address(addr) => write!(f, "{addr:063x}"),
            PgSchema::Named(name) | PgSchema::Single(name) => name.fmt(f),
        }
    }
}

impl Hash for TableKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.id.hash(state);
    }
}

impl Hash for PgSchema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            PgSchema::Address(addr) => addr.hash(state),
            PgSchema::Named(name) => name.hash(state),
            PgSchema::Single(_) => {}
        }
    }
}

impl Deref for PostgresTables {
    type Target = RwLock<HashMap<TableKey, PgTable>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PostgresTables {
    pub fn create_table(
        &self,
        schema_key: PgSchema,
        to_table: impl Into<TableSchema>,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> PgDbResult<()> {
        let (id, info) = Into::<TableSchema>::into(to_table).into();
        let schema = schema_key.to_string();
        let key = TableKey::new(schema_key, id);
        self.assert_table_not_exists(&key, &info.name)?;
        let order = info.columns.ids();
        let table = PgTable::new(schema, *from_address, info, None);
        table.create_table_queries(
            &id,
            &order,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )?;
        let mut tables = self.write()?;
        tables.insert(key, table);
        Ok(())
    }

    pub fn update_table(
        &self,
        schema_key: PgSchema,
        to_table: impl Into<TableSchema>,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> PgDbResult<()> {
        let (id, table) = Into::<TableSchema>::into(to_table).into();
        let mut tables = self.write()?;
        let key = TableKey::new(schema_key, id);
        let existing = tables
            .get_mut(&key)
            .ok_or_else(|| PgDbError::TableNotFound(key.clone()))?;
        let upgrades = existing.update_from_info(&id, &table)?;
        upgrades.to_queries(&id, block_number, transaction_hash, queries)?;
        existing.insert_queries(
            &id,
            Some(&upgrades.columns_upgraded),
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
    }

    pub fn assert_table_not_exists(&self, id: &TableKey, name: &str) -> PgDbResult<()> {
        match self.read()?.get(id) {
            Some(existing) => Err(PgDbError::TableAlreadyExists(
                id.clone(),
                name.to_string(),
                existing.name.to_string(),
            )),
            None => Ok(()),
        }
    }

    pub fn set_table_dead(&self, schema: PgSchema, id: Felt) -> PgDbResult<()> {
        let mut tables = self.write()?;
        let key = TableKey::new(schema, id);
        match tables.get_mut(&key) {
            Some(table) => {
                table.alive = false;
                Ok(())
            }
            None => Err(PgDbError::TableNotFound(key)),
        }
    }

    pub fn insert_fields(
        &self,
        schema: PgSchema,
        event: &InsertsFields,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> PgDbResult<()> {
        let tables = self.read().unwrap();
        let key = TableKey::new(schema, event.table);
        let table = match tables.get(&key) {
            Some(table) => Ok(table),
            None => Err(PgDbError::TableNotFound(key)),
        }?;
        if !table.alive {
            return Ok(());
        }
        table.insert_fields(event, block_number, transaction_hash, from_address, queries)
    }

    pub fn handle_message(
        &self,
        schema: PgSchema,
        msg: &IntrospectMsg,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> PgDbResult<()> {
        match msg {
            IntrospectMsg::CreateTable(event) => self.create_table(
                schema,
                event.clone(),
                from_address,
                block_number,
                transaction_hash,
                queries,
            ),
            IntrospectMsg::UpdateTable(event) => self.update_table(
                schema,
                event.clone(),
                from_address,
                block_number,
                transaction_hash,
                queries,
            ),
            IntrospectMsg::AddColumns(event) => self.set_table_dead(schema, event.table),
            IntrospectMsg::DropColumns(event) => self.set_table_dead(schema, event.table),
            IntrospectMsg::RetypeColumns(event) => self.set_table_dead(schema, event.table),
            IntrospectMsg::RetypePrimary(event) => self.set_table_dead(schema, event.table),
            IntrospectMsg::RenameTable(_)
            | IntrospectMsg::DropTable(_)
            | IntrospectMsg::RenameColumns(_)
            | IntrospectMsg::RenamePrimary(_) => Ok(()),
            IntrospectMsg::InsertsFields(event) => self.insert_fields(
                schema,
                event,
                from_address,
                block_number,
                transaction_hash,
                queries,
            ),
            IntrospectMsg::DeleteRecords(_) | IntrospectMsg::DeletesFields(_) => Ok(()),
        }
    }
}
