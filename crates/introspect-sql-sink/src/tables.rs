use crate::backend::IntrospectQueryMaker;
use crate::error::RecordResultExt;
use crate::namespace::{NamespaceKey, TableKey};
use crate::table::Table;
use crate::{DbError, DbResult};
use introspect_types::ResultInto;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::RwLock;
use torii_introspect::events::IntrospectMsg;
use torii_introspect::schema::TableSchema;
use torii_introspect::InsertsFields;
use torii_sql::FlexQuery;

#[derive(Debug, Default)]
pub struct Tables(pub RwLock<HashMap<TableKey, Table>>);

impl Deref for Tables {
    type Target = RwLock<HashMap<TableKey, Table>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Tables {
    pub fn create_table<Backend: IntrospectQueryMaker>(
        &self,
        namespace_key: NamespaceKey,
        to_table: impl Into<TableSchema>,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<Backend::DB>>,
    ) -> DbResult<()> {
        let (id, info) = Into::<TableSchema>::into(to_table).into();
        let namespace = namespace_key.to_string();

        let key = TableKey::new(namespace_key, id);
        self.assert_table_not_exists(&key, &info.name)?;
        Backend::create_table_queries(
            &namespace,
            &id,
            &info.name,
            &info.primary,
            &info.columns,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )?;
        let mut tables = self.write()?;
        tables.insert(key, Table::new(id, namespace, *from_address, info, None));
        Ok(())
    }

    pub fn update_table<Backend: IntrospectQueryMaker>(
        &self,
        namespace_key: NamespaceKey,
        to_table: impl Into<TableSchema>,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<Backend::DB>>,
    ) -> DbResult<()> {
        let (id, new) = Into::<TableSchema>::into(to_table).into();
        let mut tables = self.write()?;
        let key = TableKey::new(namespace_key, id);
        let table = tables
            .get_mut(&key)
            .ok_or_else(|| DbError::TableNotFound(key.clone()))?;
        Backend::update_table_queries(
            table,
            &new.name,
            &new.primary,
            &new.columns,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
        .err_into()
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

    pub fn set_table_dead(&self, namespace: NamespaceKey, id: Felt) -> DbResult<()> {
        let mut tables = self.write()?;
        let key = TableKey::new(namespace, id);
        match tables.get_mut(&key) {
            Some(table) => {
                table.alive = false;
                Ok(())
            }
            None => Err(DbError::TableNotFound(key)),
        }
    }

    pub fn insert_fields<Backend: IntrospectQueryMaker>(
        &self,
        namespace: NamespaceKey,
        event: &InsertsFields,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<Backend::DB>>,
    ) -> DbResult<()> {
        let tables = self.read().unwrap();
        let key = TableKey::new(namespace, event.table);
        let table = match tables.get(&key) {
            Some(table) => Ok(table),
            None => Err(DbError::TableNotFound(key)),
        }?;
        if !table.alive {
            return Ok(());
        }
        let schema = table.get_record_schema(&event.columns)?;
        Backend::insert_record_queries(
            &table.namespace,
            &table.name,
            &schema,
            &event.records,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
        .to_db_result(&table.name)
    }

    pub fn handle_message<Backend: IntrospectQueryMaker>(
        &self,
        namespace: NamespaceKey,
        msg: &IntrospectMsg,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<Backend::DB>>,
    ) -> DbResult<()> {
        match msg {
            IntrospectMsg::CreateTable(event) => self.create_table::<Backend>(
                namespace,
                event.clone(),
                from_address,
                block_number,
                transaction_hash,
                queries,
            ),
            IntrospectMsg::UpdateTable(event) => self.update_table::<Backend>(
                namespace,
                event.clone(),
                from_address,
                block_number,
                transaction_hash,
                queries,
            ),
            IntrospectMsg::AddColumns(event) => self.set_table_dead(namespace, event.table),
            IntrospectMsg::DropColumns(event) => self.set_table_dead(namespace, event.table),
            IntrospectMsg::RetypeColumns(event) => self.set_table_dead(namespace, event.table),
            IntrospectMsg::RetypePrimary(event) => self.set_table_dead(namespace, event.table),
            IntrospectMsg::RenameTable(_)
            | IntrospectMsg::DropTable(_)
            | IntrospectMsg::RenameColumns(_)
            | IntrospectMsg::RenamePrimary(_) => Ok(()),
            IntrospectMsg::InsertsFields(event) => self.insert_fields::<Backend>(
                namespace,
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
