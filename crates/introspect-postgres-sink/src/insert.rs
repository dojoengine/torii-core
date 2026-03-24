use crate::{json::PostgresJsonSerializer, table::PgTable, PgDbResult};
use introspect_types::ColumnInfo;
use serde::ser::SerializeMap;
use serde_json::Serializer as JsonSerializer;
use starknet_types_core::felt::Felt;
use std::io::Write;
use torii_common::sql::{PgQuery, Queries};
use torii_introspect::{tables::SerializeEntries, InsertsFields};

pub const METADATA_CONFLICTS: &str = "__updated_at = NOW(), __updated_block = EXCLUDED.__updated_block, __updated_tx = EXCLUDED.__updated_tx";

pub struct MetaData<'a> {
    pub block_number: u64,
    pub transaction_hash: &'a Felt,
}

impl<'a> MetaData<'a> {
    pub fn new(block_number: u64, transaction_hash: &'a Felt) -> Self {
        Self {
            block_number,
            transaction_hash,
        }
    }
}

pub fn pg_json_felt252(value: &Felt) -> String {
    format!("\\x{}", hex::encode(value.to_bytes_be()))
}

impl SerializeEntries for MetaData<'_> {
    fn entry_count(&self) -> usize {
        4
    }
    fn serialize_entries<S: serde::Serializer>(
        &self,
        map: &mut <S as serde::Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        let tx_hash = pg_json_felt252(&self.transaction_hash);
        map.serialize_entry("__created_block", &self.block_number)?;
        map.serialize_entry("__updated_block", &self.block_number)?;
        map.serialize_entry("__created_tx", &tx_hash)?;
        map.serialize_entry("__updated_tx", &tx_hash)
    }
}

impl PgTable {
    pub fn insert_fields(
        &self,
        event: &InsertsFields,
        block_number: u64,
        transaction_hash: &Felt,
        _from_address: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> PgDbResult<()> {
        let record = self.get_record_schema(&event.columns)?;
        let table_name = &self.name;
        let mut writer = Vec::new();
        let schema = &self.schema;
        let metadata = MetaData::new(block_number, transaction_hash);
        write!(
            writer,
            r#"INSERT INTO "{schema}"."{table_name}" SELECT * FROM jsonb_populate_recordset(NULL::"{schema}"."{table_name}", $$"#
        )
        .unwrap();
        record.parse_records_with_metadata(
            &event.records,
            &metadata,
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
        queries.add(string);
        Ok(())
    }
}
