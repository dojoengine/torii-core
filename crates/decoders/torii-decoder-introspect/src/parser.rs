use crate::IntrospectEventReader;
use crate::manager::{SchemaRef, StoreTrait};
use introspect_events::database::{
    AddColumn, AddColumns, CreateColumnGroup, CreateTable, CreateTableFromClassHash,
    CreateTableWithColumns, DeleteField, DeleteFieldGroup, DeleteFieldGroups, DeleteFields,
    DeleteRecord, DeleteRecords, DeletesField, DeletesFieldGroup, DeletesFieldGroups,
    DeletesFields, DropColumn, DropColumns, DropTable, IdData, IdName, IdTypeAttributes,
    InsertField, InsertFieldGroup, InsertFieldGroups, InsertFields, InsertRecord, InsertRecords,
    InsertsField, InsertsFieldGroup, InsertsFieldGroups, InsertsFields, RenameColumn,
    RenameColumns, RenamePrimary, RenameTable, RetypeColumn, RetypeColumns, RetypePrimary,
};
use introspect_events::event::EventTrait;
use introspect_types::{FeltIterator, TableSchema, schema};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use thiserror::Error;
use torii_core::{Envelope, Event};
use torii_types_introspect::{
    CreateFieldGroupV1, DeclareTableV1, DeleteRecordsV1, DeletesFieldsV1, UpdateFieldsV1,
    UpdatesFieldsV1,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to parse event data")]
    ParseError,
    #[error("schema not found")]
    SchemaNotFound,
    #[error("column group not found")]
    ColumnGroupNotFound,
    #[error("method not implemented")]
    MethodNotImplemented,
}
pub type Result<T> = std::result::Result<T, Error>;
pub trait IntrospectParser {
    fn deserialize_event<T: EventTrait>(
        &self,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<T>;
    fn schema(&self, id: Felt, columns: &[Felt]) -> Result<SchemaRef>;
    fn full_schema(&self, id: Felt) -> Result<SchemaRef>;
    fn _schema(&self, id: Felt, columns: Option<&[Felt]>) -> Result<SchemaRef>;
    fn columns_from_groups(&self, groups: &[Felt]) -> Result<Vec<Felt>>;
    fn columns_from_group(&self, group_id: &Felt) -> Result<Vec<Felt>>;
    fn update_fields_msg(
        &self,
        raw: &EmittedEvent,
        table: Felt,
        columns: Option<&[Felt]>,
        record: Felt,
        data: Vec<Felt>,
    ) -> Result<Envelope>;
    fn updates_fields_msg(
        &self,
        raw: &EmittedEvent,
        table: Felt,
        columns: Option<&[Felt]>,
        records_data: Vec<IdData>,
    ) -> Result<Envelope>;
    fn delete_records_msg(
        &self,
        raw: &EmittedEvent,
        table: Felt,
        records: &[Felt],
    ) -> Result<Envelope>;
    fn delete_fields_msg(
        &self,
        raw: &EmittedEvent,
        table: Felt,
        records: &[Felt],
        columns: &[Felt],
    ) -> Result<Envelope>;
    async fn create_column_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn create_table(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn create_table_with_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn create_table_from_class_hash(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn rename_table(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn drop_table(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn rename_primary(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn retype_primary(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn add_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn add_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn rename_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn rename_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn retype_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn retype_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn drop_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    async fn drop_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn insert_record(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn insert_records(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn insert_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn insert_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn inserts_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn inserts_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn insert_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn insert_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn inserts_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn inserts_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn delete_record(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn delete_records(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn delete_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn delete_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn deletes_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn deletes_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn delete_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn delete_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn deletes_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
    fn deletes_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope>;
}

impl<S, F> IntrospectParser for IntrospectEventReader<S, F>
where
    S: StoreTrait + Send + Sync + 'static,
    F: Send + Sync + 'static,
{
    fn deserialize_event<T: EventTrait>(
        &self,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<T> {
        T::deserialize_event(keys, data).ok_or(Error::ParseError)
    }
    fn schema(&self, id: Felt, columns: &[Felt]) -> Result<SchemaRef> {
        self.manager
            .schema(id, columns)
            .ok_or(Error::SchemaNotFound)
    }
    fn full_schema(&self, id: Felt) -> Result<SchemaRef> {
        self.manager.full_schema(id).ok_or(Error::SchemaNotFound)
    }
    fn _schema(&self, id: Felt, columns: Option<&[Felt]>) -> Result<SchemaRef> {
        match columns {
            Some(cols) => self.schema(id, cols),
            None => self.full_schema(id),
        }
    }

    fn columns_from_groups(&self, groups: &[Felt]) -> Result<Vec<Felt>> {
        groups.iter().try_fold(Vec::new(), |mut columns, group_id| {
            columns.extend(self.columns_from_group(group_id)?);
            Ok(columns)
        })
    }
    fn columns_from_group(&self, group_id: &Felt) -> Result<Vec<Felt>> {
        self.manager
            .group(*group_id)
            .ok_or(Error::ColumnGroupNotFound)
    }
    fn update_fields_msg(
        &self,
        raw: &EmittedEvent,
        table: Felt,
        columns: Option<&[Felt]>,
        record: Felt,
        data: Vec<Felt>,
    ) -> Result<Envelope> {
        let schema = self._schema(table, columns)?;
        UpdateFieldsV1::from(schema.to_record(record, data).ok_or(Error::ParseError)?)
            .to_ok_envelope(raw)
    }

    fn delete_records_msg(
        &self,
        raw: &EmittedEvent,
        table_id: Felt,
        records: &[Felt],
    ) -> Result<Envelope> {
        let table = self.manager.table(table_id).ok_or(Error::SchemaNotFound)?;
        let records = records
            .iter()
            .map(|id| table.primary.to_primary_value(*id))
            .collect::<Option<Vec<_>>>()
            .ok_or(Error::ParseError)?;
        DeleteRecordsV1 {
            table_id,
            table_name: table.name.clone(),
            primary: table.primary.to_primary_info(),
            records,
        }
        .to_ok_envelope(raw)
    }

    fn delete_fields_msg(
        &self,
        raw: &EmittedEvent,
        table_id: Felt,
        records: &[Felt],
        columns: &[Felt],
    ) -> Result<Envelope> {
        let table = self.manager.table(table_id).ok_or(Error::SchemaNotFound)?;
        let records = records
            .iter()
            .map(|id| table.primary.to_primary_value(*id))
            .collect::<Option<Vec<_>>>()
            .ok_or(Error::ParseError)?;
        let columns = table.columns_info(columns).ok_or(Error::ParseError)?;
        DeletesFieldsV1 {
            table_id,
            table_name: table.name.clone(),
            primary: table.primary.to_primary_info(),
            columns,
            records,
        }
        .to_ok_envelope(raw)
    }

    fn updates_fields_msg(
        &self,
        raw: &EmittedEvent,
        table: Felt,
        columns: Option<&[Felt]>,
        records_data: Vec<IdData>,
    ) -> Result<Envelope> {
        let schema = self._schema(table, columns)?;
        let record_values = schema
            .to_records_values(records_data)
            .ok_or(Error::ParseError)?;
        let info = schema.to_info();
        UpdatesFieldsV1 {
            table_id: info.table_id,
            table_name: info.table_name,
            primary: info.primary,
            columns: info.columns,
            records: record_values,
        }
        .to_ok_envelope(raw)
    }

    async fn create_column_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: CreateColumnGroup = self.deserialize_event(keys, data)?;
        self.manager
            .add_column_group(event.id, event.columns.clone());
        CreateFieldGroupV1 {
            id: event.id,
            columns: event.columns,
        }
        .to_ok_envelope(raw)
    }
    async fn create_table(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: CreateTable = self.deserialize_event(keys, data)?;
        let schema = TableSchema::new(
            event.id,
            event.name,
            event.attributes,
            event.primary,
            vec![],
        );
        self.manager.create_table(schema.clone());
        DeclareTableV1::from(schema).to_ok_envelope(raw)
    }
    async fn create_table_with_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: CreateTableWithColumns = self.deserialize_event(keys, data)?;
        let schema = TableSchema::new(
            event.id,
            event.name,
            event.attributes,
            event.primary,
            event.columns,
        );
        self.manager.create_table(schema.clone());
        DeclareTableV1::from(schema).to_ok_envelope(raw)
    }
    async fn create_table_from_class_hash(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = CreateTableFromClassHash::deserialize_event(keys, data)?;
    }
    async fn rename_table(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = RenameTable::deserialize_event(keys, data)?;
    }
    async fn drop_table(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DropTable::deserialize_event(keys, data)?;
    }
    async fn rename_primary(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = RenamePrimary::deserialize_event(keys, data)?;
    }
    async fn retype_primary(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = RetypePrimary::deserialize_event(keys, data)?;
    }
    async fn add_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = AddColumn::deserialize_event(keys, data)?;
    }
    async fn add_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = AddColumns::deserialize_event(keys, data)?;
    }
    async fn rename_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = RenameColumn::deserialize_event(keys, data)?;
    }
    async fn rename_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = RenameColumns::deserialize_event(keys, data)?;
    }
    async fn retype_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = RetypeColumn::deserialize_event(keys, data)?;
    }
    async fn retype_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = RetypeColumns::deserialize_event(keys, data)?;
    }
    async fn drop_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DropColumn::deserialize_event(keys, data)?;
    }
    async fn drop_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DropColumns::deserialize_event(keys, data)?;
    }
    fn insert_record(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: InsertRecord = self.deserialize_event(keys, data)?;
        self.update_fields_msg(raw, event.table, None, event.record, event.data)
    }
    fn insert_records(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: InsertRecords = self.deserialize_event(keys, data)?;
        self.updates_fields_msg(raw, event.table, None, event.records_data)
    }
    fn insert_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: InsertField = self.deserialize_event(keys, data)?;
        self.update_fields_msg(
            raw,
            event.table,
            Some(&[event.column]),
            event.record,
            event.data,
        )
    }
    fn insert_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: InsertFields = self.deserialize_event(keys, data)?;
        self.update_fields_msg(
            raw,
            event.table,
            Some(&event.columns),
            event.record,
            event.data,
        )
    }
    fn inserts_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: InsertsField = self.deserialize_event(keys, data)?;
        self.updates_fields_msg(raw, event.table, Some(&[event.column]), event.records_data)
    }
    fn inserts_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: InsertsFields = self.deserialize_event(keys, data)?;
        self.updates_fields_msg(raw, event.table, Some(&event.columns), event.records_data)
    }
    fn insert_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: InsertFieldGroup = self.deserialize_event(keys, data)?;
        let columns = self.columns_from_group(&event.group)?;
        self.update_fields_msg(raw, event.table, Some(&columns), event.record, event.data)
    }
    fn insert_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: InsertFieldGroups = self.deserialize_event(keys, data)?;
        let columns = self.columns_from_groups(&event.groups)?;
        self.update_fields_msg(raw, event.table, Some(&columns), event.record, event.data)
    }
    fn inserts_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: InsertsFieldGroup = self.deserialize_event(keys, data)?;
        let columns = self.columns_from_group(&event.group)?;
        self.updates_fields_msg(raw, event.table, Some(&columns), event.records_data)
    }
    fn inserts_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: InsertsFieldGroups = self.deserialize_event(keys, data)?;
        let columns = self.columns_from_groups(&event.groups)?;
        self.updates_fields_msg(raw, event.table, Some(&columns), event.records_data)
    }
    fn delete_record(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DeleteRecord = self.deserialize_event(keys, data)?;
        self.delete_records_msg(raw, event.table, &[event.record])
    }
    fn delete_records(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DeleteRecords = self.deserialize_event(keys, data)?;
        self.delete_records_msg(raw, event.table, &event.records)
    }
    fn delete_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DeleteField = self.deserialize_event(keys, data)?;
        self.delete_fields_msg(raw, event.table, &[event.record], &[event.column])
    }
    fn delete_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DeleteFields = self.deserialize_event(keys, data)?;
        self.delete_fields_msg(raw, event.table, &[event.record], &event.columns)
    }
    fn deletes_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DeletesField = self.deserialize_event(keys, data)?;
        self.delete_fields_msg(raw, event.table, &event.records, &[event.column])
    }
    fn deletes_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DeletesFields = self.deserialize_event(keys, data)?;
        self.delete_fields_msg(raw, event.table, &event.records, &event.columns)
    }
    fn delete_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DeleteFieldGroup = self.deserialize_event(keys, data)?;
        let columns = self.columns_from_group(&event.group)?;
        self.delete_fields_msg(raw, event.table, &[event.record], &columns)
    }
    fn delete_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DeleteFieldGroups = self.deserialize_event(keys, data)?;
        let columns = self.columns_from_groups(&event.groups)?;
        self.delete_fields_msg(raw, event.table, &[event.record], &columns)
    }
    fn deletes_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DeletesFieldGroup = self.deserialize_event(keys, data)?;
        let columns = self.columns_from_group(&event.group)?;
        self.delete_fields_msg(raw, event.table, &event.records, &columns)
    }
    fn deletes_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DeletesFieldGroups = self.deserialize_event(keys, data)?;
        let columns = self.columns_from_groups(&event.groups)?;
        self.delete_fields_msg(raw, event.table, &event.records, &columns)
    }
}
