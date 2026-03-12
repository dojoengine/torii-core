use crate::json::PostgresJsonSerializer;
use crate::sql::write_conflict_res;
use crate::table::{PgTable, PgTableError};
use crate::types::PgTypeError;
use crate::INTROSPECT_PG_SINK_MIGRATIONS;
use introspect_types::ResultInto;
use serde_json::Serializer as JsonSerializer;
use sqlx::PgPool;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::Write;
use std::ops::Deref;
use std::sync::{PoisonError, RwLock};
use torii::etl::EventContext;
use torii_introspect::events::IntrospectMsg;
use torii_introspect::schema::TableSchema;
use torii_introspect::InsertsFields;
use torii_postgres::{PostgresConnection, SqlxError};

#[derive(Debug, thiserror::Error)]
pub enum PgDbError {
    #[error(transparent)]
    DatabaseError(#[from] SqlxError),
    #[error("Invalid event format: {0}")]
    InvalidEventFormat(String),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    TableError(#[from] PgTableError),
    #[error(transparent)]
    TypeError(#[from] PgTypeError),
    #[error("Table with id: {0} already exists, incoming name: {1}, existing name: {2}")]
    TableAlreadyExists(Felt, String, String),
    #[error("Table not found with id: {0}")]
    TableNotFound(Felt),
    #[error("Table not alive - id: {0}, name: {1}")]
    TableNotAlive(Felt, String),
    #[error("Manager does not support updating")]
    UpdateNotSupported,
    #[error("Table poison error: {0}")]
    PoisonError(String),
}

type PgDbResult<T> = std::result::Result<T, PgDbError>;

impl<T> From<PoisonError<T>> for PgDbError {
    fn from(err: PoisonError<T>) -> Self {
        Self::PoisonError(err.to_string())
    }
}

#[derive(Debug, Default)]
pub struct PostgresTables(pub RwLock<HashMap<Felt, PgTable>>);

impl Deref for PostgresTables {
    type Target = RwLock<HashMap<Felt, PgTable>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub enum PgSchema {
    Public,
    Custom(String),
}

impl PgSchema {
    pub fn qualify(&self, name: &str) -> String {
        format!("{self}.{name}")
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

impl Display for PgSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgSchema::Custom(namespace) => write!(f, "{namespace}",),
            PgSchema::Public => write!(f, "public"),
        }
    }
}

impl PostgresTables {
    pub fn create_table(
        &self,
        schema: &PgSchema,
        to_table: impl Into<TableSchema>,
        queries: &mut Vec<String>,
    ) -> PgDbResult<()> {
        let table = to_table.into();
        self.assert_table_not_exists(&table.id, &table.name)?;
        let (id, table) = PgTable::new_from_table(schema, table, queries)?;
        println!("Creating table with id: {id}, name: {}", table.name());
        let mut tables = self.0.write().unwrap();
        println!("locked");
        tables.insert(id, table);
        Ok(())
    }

    pub fn assert_table_not_exists(&self, id: &Felt, name: &str) -> PgDbResult<()> {
        match self.read()?.get(id) {
            Some(existing) => Err(PgDbError::TableAlreadyExists(
                *id,
                name.to_string(),
                existing.name().to_string(),
            )),
            None => Ok(()),
        }
    }

    // pub fn update_table(
    //     &self,
    //     schema: &PgSchema,
    //     event: UpdateTable,
    //     context: &EventContext,
    //     queries: &mut Vec<String>,
    // ) -> PgDbResult<()> {
    //     let mut tables = self.write()?;
    //     let Some(existing) = tables.get_mut(&event.id) else {
    //         return self.create_table(schema, event, queries);
    //     };

    //     let mut type_queries = Vec::new();
    //     let mut type_schema = PgTableStructure::new_empty(schema);
    //     let branch = xxhash_rust::xxh3::Xxh3::new_based(existing.name());
    //     let mut alterations = Vec::new();
    //     for column in &event.columns {
    //         let pg_type = column.extract_type(&mut type_schema, &branch, &mut type_queries)?;
    //         match existing.columns.get(&column.id) {
    //             None => alterations.push(add_column_query(&column.name, &pg_type)),
    //             Some(old) if old.type_def != column.type_def => {
    //                 alterations.push(modify_column_query(&column.name, &pg_type));
    //             }
    //             _ => {}
    //         }
    //     }
    //     queries.extend(type_queries);
    //     if !alterations.is_empty() {
    //         queries.push(alter_table_query(schema, existing.name(), &alterations));
    //     }

    //     let mut ignored_queries = Vec::new();
    //     let (_, replacement_table) =
    //         PgTable::new_from_table(&schema, event.clone(), &mut ignored_queries)?;
    //     let mut tables = self.write()?;

    //     tables.insert(event.id, replacement_table);
    //     Ok(())
    // }

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
        let record = table.get_schema(&event.columns)?;
        let table_name = table.name();

        let mut writer = Vec::new();
        let schema = table.schema();
        write!(
            writer,
            r#"INSERT INTO "{schema}"."{table_name}"
            SELECT * FROM jsonb_populate_recordset(NULL::"{schema}"."{table_name}", $$"#
        )?;
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
        )?;
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
        schema: &PgSchema,
        msg: &IntrospectMsg,
        context: &EventContext,
        queries: &mut Vec<String>,
    ) -> PgDbResult<()> {
        match msg {
            IntrospectMsg::CreateTable(event) => self.create_table(schema, event.clone(), queries),
            IntrospectMsg::UpdateTable(_) => Ok(()),
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

    pub async fn migrate_introspect_sink(&self) -> PgDbResult<()> {
        self.migrate(Some("introspect"), INTROSPECT_PG_SINK_MIGRATIONS)
            .await
            .err_into()
    }
    pub fn load_tables_no_commit(&self, table_schemas: Vec<TableSchema>) -> PgDbResult<()> {
        let mut tables = self.tables.write()?;
        let mut queries = Vec::new();
        for table in table_schemas {
            let (id, table) = PgTable::new_from_table(&self.schema, table, &mut queries)?;
            tables.insert(id, table);
        }
        Ok(())
    }

    pub async fn process_message(
        &self,
        msg: &IntrospectMsg,
        context: &EventContext,
    ) -> PgDbResult<()> {
        let mut queries = Vec::new();
        self.tables
            .handle_message(&self.schema, msg, context, &mut queries)?;
        self.execute_queries(&queries).await?;
        Ok(())
    }

    pub async fn process_messages(
        &self,
        msgs: Vec<(&IntrospectMsg, &EventContext)>,
    ) -> PgDbResult<()> {
        let mut queries = Vec::new();
        for (msg, context) in msgs {
            self.tables
                .handle_message(&self.schema, msg, context, &mut queries)?;
        }
        self.execute_queries(&queries).await?;
        Ok(())
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
