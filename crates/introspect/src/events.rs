//! @ben: in introspect case, we may want to not define everything, since types will be mostly
//! taken from introspect crate. We may not need it, or only the URL declaration and the ID
//! and then using the new type pattern, using the `impl_event!` on the struct.

use std::collections::HashMap;

use introspect_events::database::{IdName, IdTypeDef};
use introspect_types::{
    Attribute, ColumnDef, FeltIds, PrimaryDef, PrimaryTypeDef, PrimaryValue, RecordValues,
    TableSchema, TypeDef, Value,
};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use torii::etl::{Envelope, TypedBody};
use torii::typed_body_impl;

pub const CREATE_TABLE_URL: &str = "introspect.CreateTable";
pub const UPDATE_TABLE_URL: &str = "introspect.UpdateTable";
pub const RENAME_TABLE_URL: &str = "introspect.RenameTable";
pub const RENAME_PRIMARY_URL: &str = "introspect.RenamePrimary";
pub const RETYPE_PRIMARY_URL: &str = "introspect.RetypePrimary";
pub const RENAME_COLUMNS_URL: &str = "introspect.RenameColumns";
pub const ADD_COLUMNS_URL: &str = "introspect.AddColumns";
pub const RETYPE_COLUMNS_URL: &str = "introspect.RetypeColumns";
pub const DROP_TABLE_URL: &str = "introspect.DropTable";
pub const DROP_COLUMNS_URL: &str = "introspect.DropColumns";

pub const INSERTS_FIELDS_URL: &str = "introspect.InsertsFields";
pub const DELETE_RECORDS_URL: &str = "introspect.DeleteRecords";
pub const DELETES_FIELDS_URL: &str = "introspect.DeletesFields";
pub const CREATE_FIELD_GROUP_URL: &str = "introspect.CreateFieldGroup";
pub const CREATE_TYPE_DEF_URL: &str = "introspect.CreateTypeDef";

enum IntrospectBody {
    CreateTable(CreateTable),
    UpdateTable(UpdateTable),
    RenameTable(RenameTable),
    RenamePrimary(RenamePrimary),
    RetypePrimary(RetypePrimary),
    RenameColumns(RenameColumns),
    RetypeColumns(RetypeColumns),
    AddColumns(AddColumns),
    DropTable(DropTable),
    DropColumns(DropColumns),
    InsertsFields(InsertsFields),
    DeleteRecords(DeleteRecords),
    DeletesFields(DeletesFields),
    CreateFieldGroup(CreateFieldGroup),
    CreateTypeDef(CreateTypeDef),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTable {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTable {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameTable {
    pub id: Felt,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenamePrimary {
    pub table: Felt,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetypePrimary {
    pub table: Felt,
    pub attributes: Vec<Attribute>,
    pub type_def: PrimaryTypeDef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameColumns {
    pub table: Felt,
    pub columns: Vec<IdName>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetypeColumns {
    pub table: Felt,
    pub columns: Vec<IdTypeDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddColumns {
    pub table: Felt,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTable {
    pub id: Felt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropColumns {
    pub table: Felt,
    pub columns: Vec<Felt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertsFields {
    pub table: Felt,
    pub columns: Vec<Felt>,
    pub records: Vec<RecordValues>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRecords {
    pub table: Felt,
    pub rows: Vec<PrimaryValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletesFields {
    pub table: Felt,
    pub rows: Vec<PrimaryValue>,
    pub columns: Vec<Felt>,
}

pub struct CreateFieldGroup {
    pub id: Felt,
    pub columns: Vec<Felt>,
}

pub struct CreateTypeDef {
    pub id: Felt,
    pub type_def: TypeDef,
}

typed_body_impl!(CreateTable, CREATE_TABLE_URL);
typed_body_impl!(UpdateTable, UPDATE_TABLE_URL);
typed_body_impl!(RenameTable, RENAME_TABLE_URL);
typed_body_impl!(RenamePrimary, RENAME_PRIMARY_URL);
typed_body_impl!(RetypePrimary, RETYPE_PRIMARY_URL);
typed_body_impl!(RenameColumns, RENAME_COLUMNS_URL);
typed_body_impl!(RetypeColumns, RETYPE_COLUMNS_URL);
typed_body_impl!(AddColumns, ADD_COLUMNS_URL);
typed_body_impl!(DropTable, DROP_TABLE_URL);
typed_body_impl!(DropColumns, DROP_COLUMNS_URL);
typed_body_impl!(InsertsFields, INSERTS_FIELDS_URL);
// typed_body_impl!(UpdatesFields, UPDATES_FIELDS_URL);
typed_body_impl!(DeleteRecords, DELETE_RECORDS_URL);
typed_body_impl!(DeletesFields, DELETES_FIELDS_URL);
typed_body_impl!(CreateFieldGroup, CREATE_FIELD_GROUP_URL);
typed_body_impl!(CreateTypeDef, CREATE_TYPE_DEF_URL);

pub trait IntrospectEvent: TypedBody {
    fn event_id(&self) -> String;
    fn to_envelope(self, metadata: HashMap<String, String>) -> Envelope
    where
        Self: Sized + 'static,
    {
        Envelope::new(self.event_id(), Box::new(self), metadata)
    }
}

impl IntrospectEvent for CreateTable {
    fn event_id(&self) -> String {
        format!("introspect.create_table.{:064x}", self.id)
    }
}
impl IntrospectEvent for UpdateTable {
    fn event_id(&self) -> String {
        format!("introspect.update_table.{:064x}", self.id)
    }
}
impl IntrospectEvent for RenameTable {
    fn event_id(&self) -> String {
        format!("introspect.rename_table.{:064x}", self.id)
    }
}
impl IntrospectEvent for RenamePrimary {
    fn event_id(&self) -> String {
        format!("introspect.rename_primary.{:064x}", self.table)
    }
}
impl IntrospectEvent for RetypePrimary {
    fn event_id(&self) -> String {
        format!("introspect.retype_primary.{:064x}", self.table)
    }
}
impl IntrospectEvent for RenameColumns {
    fn event_id(&self) -> String {
        format!(
            "introspect.rename_columns.{:064x}.{}",
            self.table,
            self.columns.hash()
        )
    }
}
impl IntrospectEvent for RetypeColumns {
    fn event_id(&self) -> String {
        format!(
            "introspect.retype_columns.{:064x}.{}",
            self.table,
            self.columns.hash()
        )
    }
}
impl IntrospectEvent for AddColumns {
    fn event_id(&self) -> String {
        format!(
            "introspect.add_columns.{:064x}.{}",
            self.table,
            self.columns.hash()
        )
    }
}
impl IntrospectEvent for DropTable {
    fn event_id(&self) -> String {
        format!("introspect.drop_table.{:064x}", self.id)
    }
}
impl IntrospectEvent for DropColumns {
    fn event_id(&self) -> String {
        format!(
            "introspect.drop_columns.{:064x}.{}",
            self.table,
            self.columns.hash()
        )
    }
}
impl IntrospectEvent for InsertsFields {
    fn event_id(&self) -> String {
        format!(
            "introspect.insert_fields.{:064x}.{:064x}",
            self.table,
            self.columns.to_felt()
        )
    }
}
// impl UpdatesFields {
//     pub fn id(&self) -> String {
//         format!("introspect.updates_fields.{:064x}", self.table)
//     }
// }
impl IntrospectEvent for DeleteRecords {
    fn event_id(&self) -> String {
        format!(
            "introspect.delete_records.{:064x}.{}",
            self.table,
            self.rows.hash()
        )
    }
}
impl IntrospectEvent for DeletesFields {
    fn event_id(&self) -> String {
        format!(
            "introspect.deletes_fields.{:064x}.{}.{}",
            self.table,
            self.rows.hash(),
            self.columns.hash()
        )
    }
}
impl IntrospectEvent for CreateFieldGroup {
    fn event_id(&self) -> String {
        format!("introspect.create_field_group.{:064x}", self.id)
    }
}
impl IntrospectEvent for CreateTypeDef {
    fn event_id(&self) -> String {
        format!("introspect.create_type_def.{:064x}", self.id)
    }
}

impl From<TableSchema> for CreateTable {
    fn from(schema: TableSchema) -> Self {
        Self {
            id: schema.id,
            name: schema.name,
            attributes: schema.attributes,
            primary: schema.primary,
            columns: schema.columns,
        }
    }
}

impl From<CreateTable> for TableSchema {
    fn from(schema: CreateTable) -> Self {
        TableSchema {
            id: schema.id,
            name: schema.name,
            attributes: schema.attributes,
            primary: schema.primary,
            columns: schema.columns,
        }
    }
}

impl From<TableSchema> for UpdateTable {
    fn from(schema: TableSchema) -> Self {
        Self {
            id: schema.id,
            name: schema.name,
            attributes: schema.attributes,
            primary: schema.primary,
            columns: schema.columns,
        }
    }
}

impl From<UpdateTable> for TableSchema {
    fn from(schema: UpdateTable) -> Self {
        TableSchema {
            id: schema.id,
            name: schema.name,
            attributes: schema.attributes,
            primary: schema.primary,
            columns: schema.columns,
        }
    }
}

impl InsertFields {
    pub fn new(table: Felt, primary: PrimaryValue, fields: Vec<IdValue>) -> Self {
        Self {
            table,
            primary,
            fields,
        }
    }
}

impl DeleteRecords {
    pub fn new(table: Felt, rows: Vec<PrimaryValue>) -> Self {
        Self { table, rows }
    }
}
