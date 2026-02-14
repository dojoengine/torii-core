//! @ben: in introspect case, we may want to not define everything, since types will be mostly
//! taken from introspect crate. We may not need it, or only the URL declaration and the ID
//! and then using the new type pattern, using the `impl_event!` on the struct.

use introspect_events::database::{IdName, IdTypeDef};
use introspect_types::{
    Attribute, ColumnDef, PrimaryDef, PrimaryTypeDef, PrimaryValue, RecordValues, TableSchema,
    TypeDef, Value,
};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use torii::typed_body_impl;

pub const CREATE_TABLE_URL: &str = "introspect.CreateTable";

pub const RENAME_TABLE_URL: &str = "introspect.RenameTable";
pub const RENAME_PRIMARY_URL: &str = "introspect.RenamePrimary";
pub const RETYPE_PRIMARY_URL: &str = "introspect.RetypePrimary";
pub const RENAME_COLUMNS_URL: &str = "introspect.RenameColumns";
pub const ADD_COLUMNS_URL: &str = "introspect.AddColumns";
pub const RETYPE_COLUMNS_URL: &str = "introspect.RetypeColumns";
pub const DROP_TABLE_URL: &str = "introspect.DropTable";
pub const DROP_COLUMNS_URL: &str = "introspect.DropColumns";

pub const UPDATE_FIELDS_URL: &str = "introspect.UpdateFields";
pub const UPDATES_FIELDS_URL: &str = "introspect.UpdatesFields";
pub const DELETE_RECORDS_URL: &str = "introspect.DeleteRecords";
pub const DELETES_FIELDS_URL: &str = "introspect.DeletesFields";
pub const CREATE_FIELD_GROUP_URL: &str = "introspect.CreateFieldGroup";
pub const CREATE_TYPE_DEF_URL: &str = "introspect.CreateTypeDef";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdValue {
    pub id: Felt,
    pub value: Value,
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
pub struct RenameTable {
    pub id: Felt,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenamePrimary {
    pub table: Felt,
    pub name: String,
}

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

pub struct InsertFields {
    pub table: Felt,
    pub primary: PrimaryValue,
    pub fields: Vec<IdValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatesFields {
    pub table: Felt,
    pub columns: Vec<Felt>,
    pub records: Vec<RecordValues>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRecords {
    pub table: Felt,
    pub records: Vec<PrimaryValue>,
}

pub struct DeletesFields {
    pub table: Felt,
    pub records: Vec<PrimaryValue>,
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

impl IdValue {
    pub fn new(id: Felt, value: Value) -> Self {
        Self { id, value }
    }
}

typed_body_impl!(CreateTable, CREATE_TABLE_URL);
typed_body_impl!(RenameTable, RENAME_TABLE_URL);
typed_body_impl!(RenamePrimary, RENAME_PRIMARY_URL);
typed_body_impl!(RetypePrimary, RETYPE_PRIMARY_URL);
typed_body_impl!(RenameColumns, RENAME_COLUMNS_URL);
typed_body_impl!(RetypeColumns, RETYPE_COLUMNS_URL);
typed_body_impl!(AddColumns, ADD_COLUMNS_URL);
typed_body_impl!(DropTable, DROP_TABLE_URL);
typed_body_impl!(DropColumns, DROP_COLUMNS_URL);
typed_body_impl!(InsertFields, UPDATE_FIELDS_URL);
typed_body_impl!(UpdatesFields, UPDATES_FIELDS_URL);
typed_body_impl!(DeleteRecords, DELETE_RECORDS_URL);
typed_body_impl!(DeletesFields, DELETES_FIELDS_URL);
typed_body_impl!(CreateFieldGroup, CREATE_FIELD_GROUP_URL);
typed_body_impl!(CreateTypeDef, CREATE_TYPE_DEF_URL);

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
    pub fn new(table: Felt, records: Vec<PrimaryValue>) -> Self {
        Self { table, records }
    }
}
