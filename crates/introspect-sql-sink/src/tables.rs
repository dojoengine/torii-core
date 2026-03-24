use crate::schema::{SchemaKey, TableKey};
use crate::table::{DbTable, DbTableTrait};
use crate::{DbError, DbResult};
use introspect_types::FeltIds;
use sqlx::Database;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::RwLock;
use torii_common::sql::FlexQuery;
use torii_introspect::events::IntrospectMsg;
use torii_introspect::schema::TableSchema;
use torii_introspect::InsertsFields;

pub const DEAD_MEMBERS_TABLE: &str = "__introspect_dead_fields";
pub const TABLES_TABLE: &str = "__introspect_tables";
pub const COLUMNS_TABLE: &str = "__introspect_columns";

#[derive(Debug, Default)]
pub struct DbTables<DB>(pub RwLock<HashMap<TableKey, DbTable>>, PhantomData<DB>);

impl<DB: Database> Deref for DbTables<DB> {
    type Target = RwLock<HashMap<TableKey, DbTable>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<DB> DbTables<DB>
where
    DB: Database,
    DbTable: DbTableTrait<DB>,
{
    pub fn create_table(
        &self,
        schema_key: SchemaKey,
        to_table: impl Into<TableSchema>,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<DB>>,
    ) -> DbResult<()> {
        let (id, info) = Into::<TableSchema>::into(to_table).into();
        let schema = schema_key.to_string();
        let key = TableKey::new(schema_key, id);
        self.assert_table_not_exists(&key, &info.name)?;
        let order = info.columns.ids();
        let table = DbTable::new(schema, *from_address, info, None);
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
        schema_key: SchemaKey,
        to_table: impl Into<TableSchema>,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<DB>>,
    ) -> DbResult<()> {
        let (id, table) = Into::<TableSchema>::into(to_table).into();
        let mut tables = self.write()?;
        let key = TableKey::new(schema_key, id);
        let existing = tables
            .get_mut(&key)
            .ok_or_else(|| DbError::TableNotFound(key.clone()))?;
        existing.update_table_queries(
            &table.name,
            &table.primary,
            &table.columns,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
    }

    pub fn assert_table_not_exists(&self, id: &TableKey, name: &str) -> DbResult<()> {
        match self.read()?.get(id) {
            Some(existing) => Err(DbError::TableAlreadyExists(
                id.clone(),
                name.to_string(),
                existing.name.to_string(),
            )),
            None => Ok(()),
        }
    }

    pub fn set_table_dead(&self, schema: SchemaKey, id: Felt) -> DbResult<()> {
        let mut tables = self.write()?;
        let key = TableKey::new(schema, id);
        match tables.get_mut(&key) {
            Some(table) => {
                table.alive = false;
                Ok(())
            }
            None => Err(DbError::TableNotFound(key)),
        }
    }

    pub fn insert_fields(
        &self,
        schema: SchemaKey,
        event: &InsertsFields,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<DB>>,
    ) -> DbResult<()> {
        let tables = self.read().unwrap();
        let key = TableKey::new(schema, event.table);
        let table = match tables.get(&key) {
            Some(table) => Ok(table),
            None => Err(DbError::TableNotFound(key)),
        }?;
        if !table.alive {
            return Ok(());
        }
        table.insert_fields_queries(
            &event.columns,
            &event.records,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
    }

    pub fn handle_message(
        &self,
        schema: SchemaKey,
        msg: &IntrospectMsg,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<DB>>,
    ) -> DbResult<()> {
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
