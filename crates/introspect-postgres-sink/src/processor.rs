use crate::json::PostgresJsonSerializer;
use crate::query::CreatePgTable;
use crate::table::{DeadField, PgTable};
use crate::{PgDbError, PgDbResult, PgSchema, INTROSPECT_PG_SINK_MIGRATIONS};
use introspect_types::{ColumnInfo, ResultInto, TypeDef};
use serde_json::Serializer as JsonSerializer;
use sqlx::types::Json;
use sqlx::{FromRow, PgPool};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::io::Write;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::RwLock;
use torii::etl::envelope::MetaData;
use torii_introspect::events::{IntrospectBody, IntrospectMsg};
use torii_introspect::postgres::types::Uint128;
use torii_introspect::postgres::PgFelt;
use torii_introspect::schema::TableSchema;
use torii_introspect::InsertsFields;
use torii_postgres::PostgresConnection;

pub const COMMIT_CMD: &str = "--COMMIT";
pub const DEAD_MEMBERS_TABLE: &str = "__torii_dead_fields";
pub const METADATA_CONFLICTS: &str = "__updated_at = NOW(), __updated_block = EXCLUDED.__updated_block, __updated_tx = EXCLUDED.__updated_tx";

#[derive(Debug, Default)]
pub struct PostgresTables(pub RwLock<HashMap<Felt, PgTable>>);

#[derive(Debug, Default)]
pub struct DeadFields(pub RwLock<HashMap<Felt, Vec<(u128, DeadField)>>>);

impl Deref for PostgresTables {
    type Target = RwLock<HashMap<Felt, PgTable>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for DeadFields {
    type Target = RwLock<HashMap<Felt, Vec<(u128, DeadField)>>>;

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

impl From<()> for PgSchema {
    fn from(_: ()) -> Self {
        PgSchema::Public
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
        tables.insert(id, PgTable::new(schema, table, None));
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
        let upgrades = existing.update_from_info(&id, &table)?;
        upgrades.to_queries(&id, queries);
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
        context: &MetaData,
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
            context,
            &mut JsonSerializer::new(&mut writer),
            &PostgresJsonSerializer,
        )?;
        write!(
            writer,
            r#"$$) ON CONFLICT ("{}") DO UPDATE SET {METADATA_CONFLICTS}"#,
            record.primary().name
        )
        .unwrap();
        for ColumnInfo { name, .. } in record.columns() {
            write!(
                writer,
                r#", "{name}" = COALESCE(EXCLUDED."{name}", "{table_name}"."{name}")"#,
                name = name
            )
            .unwrap();
        }
        let string = unsafe { String::from_utf8_unchecked(writer) };
        queries.push(string);
        Ok(())
    }

    pub fn handle_message(
        &self,
        schema: &Rc<PgSchema>,
        msg: &IntrospectMsg,
        metadata: &MetaData,
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
            IntrospectMsg::InsertsFields(event) => self.insert_fields(event, metadata, queries),
            IntrospectMsg::DeleteRecords(_) | IntrospectMsg::DeletesFields(_) => Ok(()),
        }
    }
}

fn make_dead_members_table_query(schema: &PgSchema) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS "{schema}"."{DEAD_MEMBERS_TABLE}" (
            "table" public.felt252,
            id public.uint128,
            name TEXT,
            type_def JSONB,
            PRIMARY KEY ("table", id)
        )"#,
    )
}

fn make_schema_query(schema: &PgSchema) -> String {
    format!(r#"CREATE SCHEMA IF NOT EXISTS "{schema}""#)
}

#[derive(Debug, FromRow)]
pub struct DeadFieldRow {
    pub table: PgFelt,
    pub id: Uint128,
    pub name: String,
    pub type_def: Json<TypeDef>,
}

impl From<DeadFieldRow> for (Felt, u128, DeadField) {
    fn from(value: DeadFieldRow) -> Self {
        (
            value.table.into(),
            value.id.into(),
            DeadField {
                name: value.name,
                type_def: value.type_def.0,
            },
        )
    }
}

pub struct IntrospectPgDb<T> {
    tables: PostgresTables,
    schema: PgSchema,
    pool: T,
    dead_fields: DeadFields,
}

impl<T: PostgresConnection> PostgresConnection for IntrospectPgDb<T> {
    fn pool(&self) -> &PgPool {
        self.pool.pool()
    }
}

impl<T: PostgresConnection + Send + Sync> IntrospectPgDb<T> {
    pub fn new(pool: T, schema: impl Into<PgSchema>) -> Self {
        Self {
            tables: PostgresTables::default(),
            schema: schema.into(),
            pool,
            dead_fields: DeadFields::default(),
        }
    }

    pub async fn read_dead_field_rows(&self) -> PgDbResult<Vec<DeadFieldRow>> {
        let schema = &self.schema;
        let query =
            format!(r#"SELECT "table", id, name, type_def FROM "{schema}"."{DEAD_MEMBERS_TABLE}""#);
        sqlx::query_as::<_, DeadFieldRow>(&query)
            .fetch_all(self.pool())
            .await
            .err_into()
    }

    pub async fn load_dead_fields(&self) -> PgDbResult<()> {
        let mut dead_fields_map: HashMap<Felt, Vec<(u128, DeadField)>> = HashMap::new();
        for row in self.read_dead_field_rows().await? {
            let (table, id, dead_field) = row.into();
            dead_fields_map
                .entry(table)
                .or_default()
                .push((id, dead_field));
        }
        let mut tables = self.tables.write()?;
        let mut dead_fields = self.dead_fields.write()?;
        for (table, fields) in dead_fields_map {
            match tables.get_mut(&table) {
                Some(table) => table.dead.extend(fields),
                None => dead_fields.entry(table).or_default().extend(fields),
            }
        }
        Ok(())
    }

    pub async fn initialize_introspect_pg_sink(&self) -> PgDbResult<()> {
        self.migrate(Some("introspect"), INTROSPECT_PG_SINK_MIGRATIONS)
            .await?;
        let queries = [
            make_schema_query(&self.schema),
            make_dead_members_table_query(&self.schema),
        ];
        self.execute_queries(&queries).await?;
        self.load_dead_fields().await
    }

    pub fn load_tables_no_commit(&self, table_schemas: Vec<TableSchema>) -> PgDbResult<()> {
        let mut tables: std::sync::RwLockWriteGuard<'_, HashMap<Felt, PgTable>> =
            self.tables.write()?;
        let mut deads = self.dead_fields.write()?;
        for table in table_schemas {
            let (id, table) = table.into();
            tables.insert(id, PgTable::new(&self.schema, table, deads.remove(&id)));
        }
        Ok(())
    }

    pub async fn process_message(
        &self,
        msg: &IntrospectMsg,
        metadata: &MetaData,
    ) -> PgDbResult<()> {
        let mut queries = Vec::new();
        {
            let schema = Rc::new(self.schema.clone());
            self.tables
                .handle_message(&schema, msg, metadata, &mut queries)?;
        }
        self.execute_queries(&queries).await?;
        Ok(())
    }

    pub async fn process_messages(
        &self,
        msgs: Vec<&IntrospectBody>,
    ) -> PgDbResult<Vec<PgDbResult<()>>> {
        let mut queries = Vec::new();
        let mut results = Vec::with_capacity(msgs.len());
        {
            let schema = Rc::new(self.schema.clone());
            for body in msgs {
                let (msg, metadata) = body.into();
                results.push(
                    self.tables
                        .handle_message(&schema, msg, metadata, &mut queries),
                );
            }
        }
        let mut start = 0;
        for (n, query) in queries.iter().enumerate() {
            if query == COMMIT_CMD {
                self.execute_queries(&queries[start..n]).await?;
                start = n + 1;
            }
        }
        self.execute_queries(&queries[start..]).await?;
        Ok(results)
    }
}

pub struct MessageWithContext<'a, M> {
    pub msg: &'a M,
    pub context: &'a MetaData,
}
