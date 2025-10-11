//! @ben: in introspect case, we may want to not define everything, since types will be mostly
//! taken from introspect crate. We may not need it, or only the URL declaration and the ID
//! and then using the new type pattern, using the `impl_event!` on the struct.

use introspect_types::{FieldDef, TypeDef};
use introspect_value::Field;
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use torii_core::{impl_event, type_id_from_url};
pub const DECLARE_TABLE_URL: &str = "torii.introspect/DeclareTable@1";
pub const DECLARE_TABLE_ID: u64 = type_id_from_url(DECLARE_TABLE_URL);

pub const SET_RECORD_URL: &str = "torii.introspect/SetRecord@1";
pub const SET_RECORD_ID: u64 = type_id_from_url(SET_RECORD_URL);
pub const UPDATE_RECORD_FIELDS_URL: &str = "torii.introspect/UpdateRecordFields@1";
pub const UPDATE_RECORD_FIELDS_ID: u64 = type_id_from_url(UPDATE_RECORD_FIELDS_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareTableV1 {
    pub name: String,
    pub fields: Vec<FieldDef>,
    pub primary_key: (String, TypeDef),
}

impl_event!(DeclareTableV1, DECLARE_TABLE_ID);

pub struct UpdateRecordFieldsV1 {
    pub table_id: Felt,
    pub table_name: String,
    pub row_id: Felt,
    pub fields: Vec<Field>,
}

impl UpdateRecordFieldsV1 {
    pub fn new(table_id: Felt, table_name: String, row_id: Felt, fields: Vec<Field>) -> Self {
        Self {
            table_id,
            table_name,
            row_id,
            fields,
        }
    }
}

impl_event!(UpdateRecordFieldsV1, UPDATE_RECORD_FIELDS_ID);

pub struct DeleteRecordsV1 {
    pub table_id: Felt,
    pub table_name: String,
    pub row_ids: Vec<Felt>,
}
