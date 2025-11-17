//! @ben: in introspect case, we may want to not define everything, since types will be mostly
//! taken from introspect crate. We may not need it, or only the URL declaration and the ID
//! and then using the new type pattern, using the `impl_event!` on the struct.

use introspect_types::schema::PrimaryInfo;
use introspect_types::{
    Attribute, ColumnDef, ColumnInfo, Field, Primary, PrimaryDef, PrimaryValue, RecordValues,
    TableSchema,
};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use torii_core::impl_event;
pub const DECLARE_TABLE_URL: &str = "torii.introspect/DeclareTable@1";

pub const UPDATE_TABLE_URL: &str = "torii.introspect/UpdateTable@1";

pub const UPDATE_RECORD_FIELDS_URL: &str = "torii.introspect/UpdateRecordFields@1";

pub const UPDATE_FIELDS_URL: &str = "torii.introspect/UpdateFields@1";
pub const UPDATES_FIELDS_URL: &str = "torii.introspect/UpdatesFields@1";
pub const DELETE_RECORDS_URL: &str = "torii.introspect/DeleteRecords@1";
pub const DELETES_FIELDS_URL: &str = "torii.introspect/DeletesFields@1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareTableV1 {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

impl From<TableSchema> for DeclareTableV1 {
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

impl Into<TableSchema> for DeclareTableV1 {
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

impl_event!(DeclareTableV1, DECLARE_TABLE_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTableV1 {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}
impl From<TableSchema> for UpdateTableV1 {
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

impl Into<TableSchema> for UpdateTableV1 {
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

impl_event!(UpdateTableV1, UPDATE_TABLE_URL);

pub struct UpdateFieldsV1 {
    pub table_id: Felt,
    pub table_name: String,
    pub primary: Primary,
    pub fields: Vec<Field>,
}

impl UpdateFieldsV1 {
    pub fn new(table_id: Felt, table_name: String, primary: Primary, fields: Vec<Field>) -> Self {
        Self {
            table_id,
            table_name,
            primary,
            fields,
        }
    }
}

impl_event!(UpdateFieldsV1, UPDATE_FIELDS_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatesFieldsV1 {
    pub table_id: Felt,
    pub table_name: String,
    pub primary: PrimaryInfo,
    pub columns: Vec<ColumnInfo>,
    pub records: Vec<RecordValues>,
}

impl_event!(UpdatesFieldsV1, UPDATES_FIELDS_URL);
#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct DeleteRecordsV1 {
    pub table_id: Felt,
    pub table_name: String,
    pub primary: PrimaryInfo,
    pub values: Vec<PrimaryValue>,
}

impl DeleteRecordsV1 {
    pub fn new(
        table_id: Felt,
        table_name: String,
        primary: PrimaryInfo,
        values: Vec<PrimaryValue>,
    ) -> Self {
        Self {
            table_id,
            table_name,
            primary,
            values,
        }
    }
}

impl_event!(DeleteRecordsV1, DELETE_RECORDS_URL);

pub struct DeletesFieldsV1 {
    pub table_id: Felt,
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
    pub records: Vec<PrimaryValue>,
}

impl_event!(DeletesFieldsV1, DELETES_FIELDS_URL);
