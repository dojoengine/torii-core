//! @ben: in introspect case, we may want to not define everything, since types will be mostly
//! taken from introspect crate. We may not need it, or only the URL declaration and the ID
//! and then using the new type pattern, using the `impl_event!` on the struct.

use introspect_types::{ColumnDef, Field, Primary, PrimaryDef, PrimaryValue};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use torii_core::{impl_event, type_id_from_url};
pub const DECLARE_TABLE_URL: &str = "torii.introspect/DeclareTable@1";
pub const DECLARE_TABLE_ID: u64 = type_id_from_url(DECLARE_TABLE_URL);

pub const UPDATE_TABLE_URL: &str = "torii.introspect/UpdateTable@1";
pub const UPDATE_TABLE_ID: u64 = type_id_from_url(UPDATE_TABLE_URL);

pub const UPDATE_RECORD_FIELDS_URL: &str = "torii.introspect/UpdateRecordFields@1";
pub const UPDATE_RECORD_FIELDS_ID: u64 = type_id_from_url(UPDATE_RECORD_FIELDS_URL);

pub const DELETE_RECORDS_URL: &str = "torii.introspect/DeleteRecords@1";
pub const DELETE_RECORDS_ID: u64 = type_id_from_url(DELETE_RECORDS_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareTableV1 {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<String>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

impl_event!(DeclareTableV1, DECLARE_TABLE_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTableV1 {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<String>,
    pub primary: PrimaryDef,
    pub fields: Vec<ColumnDef>,
}

impl_event!(UpdateTableV1, UPDATE_TABLE_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateRecordFieldsV1 {
    pub table_id: Felt,
    pub table_name: String,
    pub primary: Primary,
    pub fields: Vec<Field>,
}

impl UpdateRecordFieldsV1 {
    pub fn new(table_id: Felt, table_name: String, primary: Primary, fields: Vec<Field>) -> Self {
        Self {
            table_id,
            table_name,
            primary,
            fields,
        }
    }
}

impl_event!(UpdateRecordFieldsV1, UPDATE_RECORD_FIELDS_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRecordsV1 {
    pub table_id: Felt,
    pub table_name: String,
    pub primary_name: String,
    pub values: Vec<PrimaryValue>,
}

impl DeleteRecordsV1 {
    pub fn new(table_id: Felt, table_name: String, primary_name: String, values: Vec<PrimaryValue>) -> Self {
        Self {
            table_id,
            table_name,
            primary_name,
            values,
        }
    }
}

impl_event!(DeleteRecordsV1, DELETE_RECORDS_URL);
