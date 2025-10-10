//! @ben: in introspect case, we may want to not define everything, since types will be mostly
//! taken from introspect crate. We may not need it, or only the URL declaration and the ID
//! and then using the new type pattern, using the `impl_event!` on the struct.

use serde::{Deserialize, Serialize};
use torii_core::{impl_event, type_id_from_url, FieldElement};

pub const DECLARE_TABLE_URL: &str = "torii.introspect/DeclareTable@1";
pub const DECLARE_TABLE_ID: u64 = type_id_from_url(DECLARE_TABLE_URL);

pub const SET_RECORD_URL: &str = "torii.introspect/SetRecord@1";
pub const SET_RECORD_ID: u64 = type_id_from_url(SET_RECORD_URL);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareTableV1 {
    pub name: String,
    pub fields: Vec<(String, String)>,
    pub primary_key: Vec<String>,
}

impl_event!(DeclareTableV1, DECLARE_TABLE_ID);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetRecordV1 {
    pub table: String,
    pub cols: Vec<String>,
    pub vals: Vec<Vec<FieldElement>>,
    pub key_cols: Vec<String>,
}

impl_event!(SetRecordV1, SET_RECORD_ID);
