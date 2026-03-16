use crate::json::PostgresJsonSerializer;
use crate::query::CreatePgTable;
use crate::table::PgTable;
use crate::{PgDbError, PgDbResult, PgSchema, INTROSPECT_PG_SINK_MIGRATIONS};
use introspect_types::ResultInto;
use serde_json::Serializer as JsonSerializer;
use sqlx::{query, PgPool};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::io::Write;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::RwLock;
use torii::etl::EventContext;
use torii_introspect::events::IntrospectMsg;
use torii_introspect::schema::TableSchema;
use torii_introspect::InsertsFields;
use torii_postgres::PostgresConnection;

#[derive(Debug, Default)]
pub struct PostgresTables(pub RwLock<HashMap<Felt, PgTable>>);

impl Deref for PostgresTables {
    type Target = RwLock<HashMap<Felt, PgTable>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Option<String>> for PgSchema {
    fn from(value: Option<String>) -> Self {
        match value {
            Some(s) => Self::Custom(s),
            None => PgSchema::Public,
        }
    }
}

impl From<String> for PgSchema {
    fn from(value: String) -> Self {
        Self::Custom(value)
    }
}

impl From<&str> for PgSchema {
    fn from(value: &str) -> Self {
        Self::Custom(value.to_string())
    }
}

impl From<Option<&str>> for PgSchema {
    fn from(value: Option<&str>) -> Self {
        match value {
            Some(s) => Self::Custom(s.to_string()),
            None => PgSchema::Public,
        }
    }
}

impl PostgresTables {
    pub fn create_table(
        &self,
        schema: &Rc<PgSchema>,
        to_table: impl Into<TableSchema>,
        queries: &mut Vec<String>,
    ) -> PgDbResult<()> {
        let (id, table) = Into::<TableSchema>::into(to_table).into();
        self.assert_table_not_exists(&id, &table.name)?;
        CreatePgTable::new(schema, &id, &table)?.make_queries(queries);
        let mut tables: std::sync::RwLockWriteGuard<'_, HashMap<Felt, PgTable>> = self.write()?;
        tables.insert(id, PgTable::new(schema, table));
        Ok(())
    }

    pub fn update_table(
        &self,
        to_table: impl Into<TableSchema>,
        queries: &mut Vec<String>,
    ) -> PgDbResult<()> {
        let (id, table) = Into::<TableSchema>::into(to_table).into();
        let mut tables = self.write()?;
        let existing = tables
            .get_mut(&id)
            .ok_or_else(|| PgDbError::TableNotFound(id))?;
        let upgrades = existing.update_from_info(&id, &table.into())?;
        upgrades.to_queries(queries);
        Ok(())
    }

    pub fn assert_table_not_exists(&self, id: &Felt, name: &str) -> PgDbResult<()> {
        match self.read()?.get(id) {
            Some(existing) => Err(PgDbError::TableAlreadyExists(
                *id,
                name.to_string(),
                existing.name.to_string(),
            )),
            None => Ok(()),
        }
    }

    pub fn set_table_dead(&self, id: &Felt) -> PgDbResult<()> {
        let mut tables = self.write()?;
        match tables.get_mut(id) {
            Some(table) => {
                table.alive = false;
                Ok(())
            }
            None => Err(PgDbError::TableNotFound(*id)),
        }
    }

    pub fn insert_fields(
        &self,
        event: &InsertsFields,
        _context: &EventContext,
        queries: &mut Vec<String>,
    ) -> PgDbResult<()> {
        let tables = self.read().unwrap();
        let table = match tables.get(&event.table) {
            Some(table) => Ok(table),
            None => Err(PgDbError::TableNotFound(event.table)),
        }?;
        if !table.alive {
            return Ok(());
        }
        let record = table.get_record_schema(&event.columns)?;
        let table_name = &table.name;
        let mut writer = Vec::new();
        let schema = &table.schema;
        write!(
            writer,
            r#"INSERT INTO "{schema}"."{table_name}" SELECT * FROM jsonb_populate_recordset(NULL::"{schema}"."{table_name}", $$"#
        )
        .unwrap();
        record.parse_records_with_metadata(
            &event.records,
            &(),
            &mut JsonSerializer::new(&mut writer),
            &PostgresJsonSerializer,
        )?;
        write!(
            writer,
            r#"$$) ON CONFLICT ("{}") DO UPDATE SET "#,
            record.primary().name
        )
        .unwrap();
        if let Some((coln, cols)) = record.columns().split_last() {
            for column in cols {
                write_conflict_res::<true, _>(&mut writer, table_name, &column.name)?;
            }
            write_conflict_res::<false, _>(&mut writer, table_name, &coln.name)?;
        }
        let string = unsafe { String::from_utf8_unchecked(writer) };
        queries.push(string);
        Ok(())
    }

    pub fn handle_message(
        &self,
        schema: &Rc<PgSchema>,
        msg: &IntrospectMsg,
        context: &EventContext,
        queries: &mut Vec<String>,
    ) -> PgDbResult<()> {
        match msg {
            IntrospectMsg::CreateTable(event) => self.create_table(schema, event.clone(), queries),
            IntrospectMsg::UpdateTable(event) => self.update_table(event.clone(), queries),
            IntrospectMsg::AddColumns(event) => self.set_table_dead(&event.table),
            IntrospectMsg::DropColumns(event) => self.set_table_dead(&event.table),
            IntrospectMsg::RetypeColumns(event) => self.set_table_dead(&event.table),
            IntrospectMsg::RetypePrimary(event) => self.set_table_dead(&event.table),
            IntrospectMsg::RenameTable(_)
            | IntrospectMsg::DropTable(_)
            | IntrospectMsg::RenameColumns(_)
            | IntrospectMsg::RenamePrimary(_) => Ok(()),
            IntrospectMsg::InsertsFields(event) => self.insert_fields(event, context, queries),
            IntrospectMsg::DeleteRecords(_) | IntrospectMsg::DeletesFields(_) => Ok(()),
        }
    }
}

pub fn write_conflict_res<const DELIMINATOR: bool, W: Write>(
    writer: &mut W,
    table: &str,
    column: &str,
) -> std::io::Result<()> {
    let separator = if DELIMINATOR { ", " } else { "" };
    write!(
        writer,
        r#""{column}" = COALESCE(EXCLUDED."{column}", "{table}"."{column}"){separator}"#,
    )
}

pub struct PostgresSimpleDb<T> {
    tables: PostgresTables,
    schema: PgSchema,
    pool: T,
}

impl<T: PostgresConnection> PostgresConnection for PostgresSimpleDb<T> {
    fn pool(&self) -> &PgPool {
        self.pool.pool()
    }
}

impl<T: PostgresConnection + Send + Sync> PostgresSimpleDb<T> {
    pub fn new(pool: T, schema: impl Into<PgSchema>) -> Self {
        Self {
            tables: PostgresTables::default(),
            schema: schema.into(),
            pool,
        }
    }

    pub async fn initialize_introspect_pg_sink(&self) -> PgDbResult<()> {
        self.migrate(Some("introspect"), INTROSPECT_PG_SINK_MIGRATIONS)
            .await?;
        let mut tx = self.begin().await?;
        sqlx::query(format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema).as_str())
            .execute(&mut *tx)
            .await?;
        tx.commit().await.err_into()
    }
    pub fn load_tables_no_commit(&self, table_schemas: Vec<TableSchema>) -> PgDbResult<()> {
        let mut tables = self.tables.write()?;
        for table in table_schemas {
            let (id, table) = table.into();
            tables.insert(id, PgTable::new(&self.schema, table));
        }
        Ok(())
    }

    pub async fn process_message(
        &self,
        msg: &IntrospectMsg,
        context: &EventContext,
    ) -> PgDbResult<()> {
        let mut queries = Vec::new();
        {
            let schema = Rc::new(self.schema.clone());
            self.tables
                .handle_message(&schema, msg, context, &mut queries)?;
        }
        self.execute_queries(&queries).await?;
        Ok(())
    }

    pub async fn process_messages(
        &self,
        msgs: Vec<(&IntrospectMsg, &EventContext)>,
    ) -> PgDbResult<Vec<PgDbResult<()>>> {
        let mut queries = Vec::new();
        let mut results = Vec::with_capacity(msgs.len());
        {
            let schema = Rc::new(self.schema.clone());
            for (msg, context) in msgs {
                results.push(
                    self.tables
                        .handle_message(&schema, msg, context, &mut queries),
                );
            }
        }
        self.execute_queries(&queries).await?;
        Ok(results)
    }
}

pub struct MessageWithContext<'a, M> {
    pub msg: &'a M,
    pub context: &'a EventContext,
}

#[cfg(test)]
mod tests {
    use super::*;
    use introspect_types::{ColumnDef, PrimaryDef, PrimaryTypeDef, TypeDef};
    use torii_introspect::{CreateTable, UpdateTable};

    fn primary() -> PrimaryDef {
        PrimaryDef {
            name: "entity_id".to_string(),
            attributes: vec![],
            type_def: PrimaryTypeDef::Felt252,
        }
    }

    fn column(id: u64, name: &str) -> ColumnDef {
        ColumnDef {
            id: Felt::from(id),
            name: name.to_string(),
            attributes: vec![],
            type_def: TypeDef::U32,
        }
    }

    fn create_table(id: u64, columns: Vec<ColumnDef>) -> CreateTable {
        CreateTable {
            id: Felt::from(id),
            name: "duel-state".to_string(),
            attributes: vec![],
            primary: primary(),
            columns,
        }
    }

    fn update_table(id: u64, columns: Vec<ColumnDef>) -> UpdateTable {
        UpdateTable {
            id: Felt::from(id),
            name: "duel-state".to_string(),
            attributes: vec![],
            primary: primary(),
            columns,
        }
    }

    // #[test]
    // fn update_table_creates_unknown_table() {
    //     let mut tables = PostgresTables::default();
    //     let mut queries = Vec::new();
    //     let schema = PgSchema::Public;
    //     tables
    //         .update_table(
    //             &schema,
    //             update_table(1, vec![column(10, "duelist_count")]),
    //             &mut queries,
    //         )
    //         .unwrap();

    //     assert!(!queries.is_empty());
    //     assert!(queries
    //         .iter()
    //         .any(|query| query.contains("CREATE TABLE IF NOT EXISTS")));
    //     assert!(tables.read().unwrap().contains_key(&Felt::from(1_u64)));
    // }

    // #[test]
    // fn update_table_emits_alter_queries_for_new_columns() {
    //     let mut tables = PostgresTables::default();
    //     let mut create_queries = Vec::new();
    //     let schema = PgSchema::Public;
    //     tables
    //         .create_table(
    //             &schema,
    //             create_table(1, vec![column(10, "duelist_count")]),
    //             &mut create_queries,
    //         )
    //         .unwrap();

    //     let mut update_queries = Vec::new();
    //     tables
    //         .update_table(
    //             &schema,
    //             update_table(
    //                 1,
    //                 vec![
    //                     column(10, "duelist_count"),
    //                     column(11, "alive_duelist_count"),
    //                 ],
    //             ),
    //             &EventContext::default(),
    //             &mut update_queries,
    //         )
    //         .unwrap();

    //     assert!(update_queries
    //         .iter()
    //         .any(|query| query.contains("ALTER TABLE") && query.contains("alive_duelist_count")));
    //     assert!(tables.read().unwrap()[&Felt::from(1_u64)]
    //         .columns
    //         .values()
    //         .any(|column| column.name == "alive_duelist_count"));
    // }
}
