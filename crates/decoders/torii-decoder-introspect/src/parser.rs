use crate::IntrospectEventReader;
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
use introspect_types::FeltIterator;
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use thiserror::Error;
use torii_core::Envelope;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to parse event data")]
    ParseError,
}
pub type Result<T> = std::result::Result<T, Error>;
pub trait IntrospectParser {
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

impl<F> IntrospectParser for IntrospectEventReader<F> {
    async fn create_column_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = CreateColumnGroup::deserialize_event(keys, data)?;
    }
    async fn create_table(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = CreateTable::deserialize_event(keys, data)?;
    }
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
        let event = InsertRecord::deserialize_event(keys, data).ok_or(Error::ParseError)?;
        self.manager.
    }
    fn insert_records(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = InsertRecords::deserialize_event(keys, data)?;
    }
    fn insert_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = InsertField::deserialize_event(keys, data)?;
    }
    fn insert_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = InsertFields::deserialize_event(keys, data)?;
    }
    fn inserts_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = InsertsField::deserialize_event(keys, data)?;
    }
    fn inserts_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = InsertsFields::deserialize_event(keys, data)?;
    }
    fn insert_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = InsertFieldGroup::deserialize_event(keys, data)?;
    }
    fn insert_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = InsertFieldGroups::deserialize_event(keys, data)?;
    }
    fn inserts_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = InsertsFieldGroup::deserialize_event(keys, data)?;
    }
    fn inserts_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = InsertsFieldGroups::deserialize_event(keys, data)?;
    }
    fn delete_record(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DeleteRecord::deserialize_event(keys, data)?;
    }
    fn delete_records(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DeleteRecords::deserialize_event(keys, data)?;
    }
    fn delete_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DeleteField::deserialize_event(keys, data)?;
    }
    fn delete_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DeleteFields::deserialize_event(keys, data)?;
    }
    fn deletes_field(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DeletesField::deserialize_event(keys, data)?;
    }
    fn deletes_fields(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DeletesFields::deserialize_event(keys, data)?;
    }
    fn delete_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DeleteFieldGroup::deserialize_event(keys, data)?;
    }
    fn delete_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DeleteFieldGroups::deserialize_event(keys, data)?;
    }
    fn deletes_field_group(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DeletesFieldGroup::deserialize_event(keys, data)?;
    }
    fn deletes_field_groups(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event = DeletesFieldGroups::deserialize_event(keys, data)?;
    }
}
