use std::vec;

use crate::IntrospectEventReader;
use crate::fetcher::{SchemaFetcherTrait, TableClass};
use crate::manager::{SchemaRef, StoreTrait, TableNameAnd};
use introspect_events::database::{
    AddColumn, AddColumns, CreateColumnGroup, CreateTable, CreateTableFromClassHash,
    CreateTableWithColumns, DeleteField, DeleteFieldGroup, DeleteFieldGroups, DeleteFields,
    DeleteRecord, DeleteRecords, DeletesField, DeletesFieldGroup, DeletesFieldGroups,
    DeletesFields, DropColumn, DropColumns, DropTable, IdData, InsertField, InsertFieldGroup,
    InsertFieldGroups, InsertFields, InsertRecord, InsertRecords, InsertsField, InsertsFieldGroup,
    InsertsFieldGroups, InsertsFields, RenameColumn, RenameColumns, RenamePrimary, RenameTable,
    RetypeColumn, RetypeColumns, RetypePrimary,
};
use introspect_events::event::EventTrait;
use introspect_types::{ColumnDef, FeltIterator, TableSchema, TypeDef};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use thiserror::Error;
use torii_core::{Envelope, Event};
use torii_types_introspect::{
    AddColumnsV1, CreateFieldGroupV1, DeclareTableV1, DeleteRecordsV1, DeletesFieldsV1,
    DropColumnsV1, DropTableV1, RenameColumnsV1, RenamePrimaryV1, RenameTableV1, RetypeColumnsV1,
    RetypePrimaryV1, UpdateFieldsV1, UpdatesFieldsV1,
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
    fn add_columns_msg(
        &self,
        raw: &EmittedEvent,
        table: Felt,
        columns: Vec<ColumnDef>,
    ) -> Result<Envelope>;
    fn rename_columns_msg(
        &self,
        raw: &EmittedEvent,
        table: Felt,
        renames: Vec<(Felt, String)>,
    ) -> Result<Envelope>;
    fn retype_columns_msg(
        &self,
        raw: &EmittedEvent,
        table: Felt,
        retypes: Vec<(Felt, TypeDef)>,
    ) -> Result<Envelope>;
    fn drop_columns_msg(
        &self,
        raw: &EmittedEvent,
        table_id: Felt,
        columns: Vec<Felt>,
    ) -> Result<Envelope>;
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
    F: SchemaFetcherTrait + Send + Sync + 'static,
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
    fn add_columns_msg(
        &self,
        raw: &EmittedEvent,
        table_id: Felt,
        columns: Vec<ColumnDef>,
    ) -> Result<Envelope> {
        let TableNameAnd {
            table_name,
            value: _,
        } = self
            .manager
            .add_columns(table_id, columns.clone())
            .ok_or(Error::SchemaNotFound)?;
        AddColumnsV1 {
            table_id,
            table_name,
            columns,
        }
        .to_ok_envelope(raw)
    }
    fn drop_columns_msg(
        &self,
        raw: &EmittedEvent,
        table_id: Felt,
        columns: Vec<Felt>,
    ) -> Result<Envelope> {
        let TableNameAnd {
            table_name,
            value: columns,
        } = self
            .manager
            .drop_columns(table_id, columns.clone())
            .ok_or(Error::SchemaNotFound)?;
        DropColumnsV1 {
            table_id,
            table_name,
            columns,
        }
        .to_ok_envelope(raw)
    }
    fn rename_columns_msg(
        &self,
        raw: &EmittedEvent,
        table_id: Felt,
        renames: Vec<(Felt, String)>,
    ) -> Result<Envelope> {
        let TableNameAnd {
            table_name,
            value: columns,
        } = self
            .manager
            .rename_columns(table_id, renames.clone())
            .ok_or(Error::SchemaNotFound)?;
        RenameColumnsV1 {
            table_id,
            table_name,
            columns,
        }
        .to_ok_envelope(raw)
    }
    fn retype_columns_msg(
        &self,
        raw: &EmittedEvent,
        table: Felt,
        retypes: Vec<(Felt, TypeDef)>,
    ) -> Result<Envelope> {
        let TableNameAnd {
            table_name,
            value: columns,
        } = self
            .manager
            .retype_columns(table, retypes.clone())
            .ok_or(Error::SchemaNotFound)?;
        RetypeColumnsV1 {
            table_id: table,
            table_name,
            columns,
        }
        .to_ok_envelope(raw)
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
        let event: CreateTableFromClassHash = self.deserialize_event(keys, data)?;
        let TableClass {
            attributes,
            primary,
            columns,
        } = self
            .fetcher
            .columns_and_attributes(&event.class_hash)
            .await
            .ok_or(Error::SchemaNotFound)?;
        let schema = TableSchema {
            id: event.id,
            name: event.name,
            attributes,
            primary,
            columns,
        };
        self.manager.create_table(schema.clone());
        DeclareTableV1::from(schema).to_ok_envelope(raw)
    }
    async fn rename_table(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: RenameTable = self.deserialize_event(keys, data)?;
        let TableNameAnd {
            table_name: old_name,
            value: _,
        } = self
            .manager
            .rename_table(event.id, event.name.clone())
            .ok_or(Error::SchemaNotFound)?;
        RenameTableV1 {
            id: event.id,
            old_name,
            new_name: event.name,
        }
        .to_ok_envelope(raw)
    }
    async fn drop_table(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DropTable = self.deserialize_event(keys, data)?;
        let name = self
            .manager
            .drop_table(event.id)
            .ok_or(Error::SchemaNotFound)?;
        DropTableV1 { id: event.id, name }.to_ok_envelope(raw)
    }
    async fn rename_primary(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: RenamePrimary = self.deserialize_event(keys, data)?;
        let TableNameAnd { table_name, value } = self
            .manager
            .rename_primary(event.table, event.name.clone())
            .ok_or(Error::SchemaNotFound)?;
        RenamePrimaryV1 {
            table: event.table,
            table_name,
            old_name: value,
            new_name: event.name,
        }
        .to_ok_envelope(raw)
    }
    async fn retype_primary(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: RetypePrimary = self.deserialize_event(keys, data)?;
        let TableNameAnd { table_name, value } = self
            .manager
            .retype_primary(
                event.table,
                event.attributes.clone(),
                event.type_def.clone(),
            )
            .ok_or(Error::SchemaNotFound)?;
        RetypePrimaryV1 {
            table: event.table,
            table_name,
            name: value,
            attributes: event.attributes,
            type_def: event.type_def,
        }
        .to_ok_envelope(raw)
    }
    async fn add_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: AddColumn = self.deserialize_event(keys, data)?;
        let column = ColumnDef {
            id: event.id,
            name: event.name,
            attributes: event.attributes,
            type_def: event.type_def,
        };
        self.add_columns_msg(raw, event.table, vec![column])
    }
    async fn add_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: AddColumns = self.deserialize_event(keys, data)?;
        self.add_columns_msg(raw, event.table, event.columns)
    }
    async fn rename_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: RenameColumn = self.deserialize_event(keys, data)?;
        self.rename_columns_msg(raw, event.table, vec![(event.id, event.name)])
    }
    async fn rename_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: RenameColumns = self.deserialize_event(keys, data)?;
        let columns = event.columns.into_iter().map(|c| (c.id, c.name)).collect();
        self.rename_columns_msg(raw, event.table, columns)
    }
    async fn retype_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: RetypeColumn = self.deserialize_event(keys, data)?;
        self.retype_columns_msg(raw, event.table, vec![(event.id, event.type_def)])
    }
    async fn retype_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: RetypeColumns = self.deserialize_event(keys, data)?;
        let retypes = event
            .columns
            .into_iter()
            .map(|c| (c.id, c.type_def))
            .collect();
        self.retype_columns_msg(raw, event.table, retypes)
    }
    async fn drop_column(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DropColumn = self.deserialize_event(keys, data)?;
        self.drop_columns_msg(raw, event.table, vec![event.id])
    }
    async fn drop_columns(
        &self,
        raw: &EmittedEvent,
        keys: &mut FeltIterator,
        data: &mut FeltIterator,
    ) -> Result<Envelope> {
        let event: DropColumns = self.deserialize_event(keys, data)?;
        self.drop_columns_msg(raw, event.table, event.ids)
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
