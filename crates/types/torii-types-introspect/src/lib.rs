//! @ben: in introspect case, we may want to not define everything, since types will be mostly
//! taken from introspect crate. We may not need it, or only the URL declaration and the ID
//! and then using the new type pattern, using the `impl_event!` on the struct.

use introspect_events::database::{IdName, IdTypeAttributes};
use introspect_types::{
    Attribute, ColumnDef, PrimaryDef, PrimaryTypeDef, PrimaryValue, RecordValues, TableSchema,
    TypeDef, Value,
};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use torii_core::impl_event;
pub const CREATE_TABLE_URL: &str = "torii.introspect/CreateTable@1";

pub const UPDATE_TABLE_URL: &str = "torii.introspect/UpdateTable@1";

pub const RENAME_TABLE_URL: &str = "torii.introspect/RenameTable@1";
pub const RENAME_PRIMARY_URL: &str = "torii.introspect/RenamePrimary@1";
pub const RETYPE_PRIMARY_URL: &str = "torii.introspect/RetypePrimary@1";
pub const RENAME_COLUMNS_URL: &str = "torii.introspect/RenameColumns@1";
pub const ADD_COLUMNS_URL: &str = "torii.introspect/AddColumns@1";
pub const RETYPE_COLUMNS_URL: &str = "torii.introspect/RetypeColumns@1";
pub const DROP_TABLE_URL: &str = "torii.introspect/DropTable@1";
pub const DROP_COLUMNS_URL: &str = "torii.introspect/DropColumns@1";

pub const UPDATE_FIELDS_URL: &str = "torii.introspect/UpdateFields@1";
pub const UPDATES_FIELDS_URL: &str = "torii.introspect/UpdatesFields@1";
pub const DELETE_RECORDS_URL: &str = "torii.introspect/DeleteRecords@1";
pub const DELETES_FIELDS_URL: &str = "torii.introspect/DeletesFields@1";
pub const CREATE_FIELD_GROUP_URL: &str = "torii.introspect/CreateFieldGroup@1";
pub const CREATE_TYPE_DEF_URL: &str = "torii.introspect/CreateTypeDef@1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdValue {
    pub id: Felt,
    pub value: Value,
}

impl IdValue {
    pub fn new(id: Felt, value: Value) -> Self {
        Self { id, value }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableV1 {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

impl From<TableSchema> for CreateTableV1 {
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

impl Into<TableSchema> for CreateTableV1 {
    fn into(self) -> TableSchema {
        TableSchema {
            id: self.id,
            name: self.name,
            attributes: self.attributes,
            primary: self.primary,
            columns: self.columns,
        }
    }
}

impl_event!(CreateTableV1, CREATE_TABLE_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTableV1 {
    pub id: Felt,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

impl From<TableSchema> for UpdateTableV1 {
    fn from(schema: TableSchema) -> Self {
        Self {
            id: schema.id,
            attributes: schema.attributes,
            primary: schema.primary,
            columns: schema.columns,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameTableV1 {
    pub id: Felt,
    pub name: String,
}

impl_event!(RenameTableV1, RENAME_TABLE_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenamePrimaryV1 {
    pub table: Felt,
    pub name: String,
}

impl_event!(RenamePrimaryV1, RENAME_PRIMARY_URL);

pub struct RetypePrimaryV1 {
    pub table: Felt,
    pub attributes: Vec<Attribute>,
    pub type_def: PrimaryTypeDef,
}

impl_event!(RetypePrimaryV1, RETYPE_PRIMARY_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameColumnsV1 {
    pub table: Felt,
    pub columns: Vec<IdName>,
}

impl_event!(RenameColumnsV1, RENAME_COLUMNS_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetypeColumnsV1 {
    pub table: Felt,
    pub columns: Vec<IdTypeAttributes>,
}

impl_event!(RetypeColumnsV1, RETYPE_COLUMNS_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddColumnsV1 {
    pub table: Felt,
    pub columns: Vec<ColumnDef>,
}

impl_event!(AddColumnsV1, ADD_COLUMNS_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTableV1 {
    pub id: Felt,
}

impl_event!(DropTableV1, DROP_TABLE_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropColumnsV1 {
    pub table: Felt,
    pub columns: Vec<Felt>,
}

impl_event!(DropColumnsV1, DROP_COLUMNS_URL);

pub struct InsertFieldsV1 {
    pub table: Felt,
    pub primary: PrimaryValue,
    pub fields: Vec<IdValue>,
}

impl_event!(UpdateTableV1, UPDATE_TABLE_URL);

impl InsertFieldsV1 {
    pub fn new(table: Felt, primary: PrimaryValue, fields: Vec<IdValue>) -> Self {
        Self {
            table,
            primary,
            fields,
        }
    }
}

impl_event!(InsertFieldsV1, UPDATE_FIELDS_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatesFieldsV1 {
    pub table: Felt,
    pub columns: Vec<Felt>,
    pub records: Vec<RecordValues>,
}

impl_event!(UpdatesFieldsV1, UPDATES_FIELDS_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRecordsV1 {
    pub table: Felt,
    pub records: Vec<PrimaryValue>,
}

impl DeleteRecordsV1 {
    pub fn new(table: Felt, records: Vec<PrimaryValue>) -> Self {
        Self { table, records }
    }
}

impl_event!(DeleteRecordsV1, DELETE_RECORDS_URL);

pub struct DeletesFieldsV1 {
    pub table: Felt,
    pub records: Vec<PrimaryValue>,
    pub columns: Vec<Felt>,
}

impl_event!(DeletesFieldsV1, DELETES_FIELDS_URL);

pub struct CreateFieldGroupV1 {
    pub id: Felt,
    pub columns: Vec<Felt>,
}
impl_event!(CreateFieldGroupV1, CREATE_FIELD_GROUP_URL);

pub struct CreateTypeDefV1 {
    pub id: Felt,
    pub type_def: TypeDef,
}
impl_event!(CreateTypeDefV1, CREATE_TYPE_DEF_URL);
