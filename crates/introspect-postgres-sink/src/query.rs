use introspect_types::{ColumnDef, ColumnInfo, MemberDef, PrimaryDef, TypeDef};
use itertools::Itertools;
use sqlx::error::BoxDynError;
use sqlx::prelude::FromRow;
use sqlx::Error::Encode as EncodeError;
use sqlx::{postgres::PgArguments, types::Json};
use sqlx::{Arguments, Executor, Postgres};
use starknet_types_core::felt::Felt;
use torii_common::sql::{PgQuery, Queries};
use torii_introspect::postgres::types::{PgPrimary, Uint128};
use torii_introspect::postgres::PgFelt;
use torii_postgres::SqlxResult;

use crate::table::PgTable;
use crate::{
    processor::COMMIT_CMD, table::DeadField, PgSchema, PostgresField, PostgresType, PrimaryKey,
    SchemaName,
};
use std::collections::HashMap;
use std::{
    fmt::{Display, Formatter, Result as FmtResult, Write},
    rc::Rc,
};

const CREATE_METADATA_COLUMNS: &str =  "__created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), __updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), __created_block public.uint64 NOT NULL, __updated_block public.uint64 NOT NULL, __created_tx public.felt252 NOT NULL, __updated_tx public.felt252 NOT NULL);";
const INSERT_DEAD_MEMBER_QUERY: &str = r#"INSERT INTO introspect.db_dead_fields
    ("schema", "table", id, name, type_def, updated_at, created_block, updated_block, created_tx, updated_tx)
    SELECT $1, $2, unnest($3::bigint[]), unnest($4::text[]), unnest($5::jsonb[]), NOW(), $6::uint64, $6::uint64, $7, $7
    ON CONFLICT ("schema", "table", id) DO UPDATE SET
    name = EXCLUDED.name, type_def = EXCLUDED.type_def, updated_at = NOW(), updated_block = EXCLUDED.updated_block, updated_tx = EXCLUDED.updated_tx"#;
const INSERT_TABLE_QUERY: &str = r#"INSERT INTO introspect.db_tables
    ("schema", id, name, primary_def, updated_at, created_block, updated_block, created_tx, updated_tx)
    VALUES ($1, $2, $3, $4, NOW(), $5::uint64, $5::uint64, $6, $6)
    ON CONFLICT ("schema", id) DO UPDATE SET
    name = EXCLUDED.name, primary_def = EXCLUDED.primary_def, updated_at = NOW(), updated_block = EXCLUDED.updated_block, updated_tx = EXCLUDED.updated_tx"#;
const INSERT_COLUMN_QUERY: &str = r#"INSERT INTO introspect.db_columns
    ("schema", "table", id, name, type_def, updated_at, created_block, updated_block, created_tx, updated_tx)
    SELECT $1, $2, unnest($3::felt252[]), unnest($4::text[]), unnest($5::jsonb[]), NOW(), $6::uint64, $6::uint64, $7, $7
    ON CONFLICT ("schema", "table", id) DO UPDATE SET
    name = EXCLUDED.name, type_def = EXCLUDED.type_def, updated_at = NOW(), updated_block = EXCLUDED.updated_block, updated_tx = EXCLUDED.updated_tx"#;

const FETCH_TABLES_QUERY: &str =
    r#"SELECT id, name, primary_def FROM introspect.db_tables WHERE "schema" = $1"#;
const FETCH_COLUMNS_QUERY: &str =
    r#"SELECT "table", id, name, type_def FROM introspect.db_columns WHERE "schema" = $1"#;
const FETCH_DEAD_FIELDS_QUERY: &str =
    r#"SELECT "table", id, name, type_def FROM introspect.db_dead_fields WHERE "schema" = $1"#;

#[derive(FromRow)]
pub struct TableRow {
    pub id: PgFelt,
    pub name: String,
    pub primary_def: PgPrimary,
}

#[derive(FromRow)]
pub struct ColumnRow {
    pub table: PgFelt,
    pub id: PgFelt,
    pub name: String,
    pub type_def: Json<TypeDef>,
}

#[derive(FromRow)]
pub struct DeadFieldRow {
    pub table: PgFelt,
    pub id: Uint128,
    pub name: String,
    pub type_def: Json<TypeDef>,
}

pub struct PgTableHead {
    id: Felt,
    name: String,
    primary: PrimaryDef,
}

#[derive(Debug)]
pub struct CreatePgTable {
    pub name: SchemaName,
    pub primary: PrimaryKey,
    pub columns: Vec<PostgresField>,
    pub pg_types: Vec<CreatesType>,
}

#[derive(Debug)]
pub struct TableUpgrade {
    pub schema: Rc<PgSchema>,
    pub name: String,
    pub old_name: Option<String>,
    pub atomic: Vec<TypeMod>,
    pub alters: Vec<StructAlter>,
    pub columns: Vec<ColumnMod>,
    pub columns_upgraded: Vec<Felt>,
    pub dead: Vec<DeadFieldWithId>,
    pub col_alters: Vec<PostgresField>,
}

#[derive(Debug)]
pub struct ColumnUpgrade {
    pub atomic: Vec<TypeMod>,
    pub alters: Vec<StructAlter>,
    pub dead: Vec<DeadFieldWithId>,
    pub altered: bool,
    pub upgraded: bool,
}

#[derive(Debug)]
pub struct CreateStruct {
    pub name: SchemaName,
    pub fields: Vec<PostgresField>,
}

#[derive(Debug)]
pub struct CreateEnum {
    pub name: SchemaName,
    pub variants: Vec<String>,
}

#[derive(Debug)]
pub enum StructMod {
    Add(PostgresField),
    Rename(String, String),
}

#[derive(Debug)]
pub enum TypeMod {
    Struct(StructUpgrade),
    Enum(EnumUpgrade),
    Create(CreatesType),
}

#[derive(Debug)]
pub enum ColumnMod {
    Add(PostgresField),
    Rename(String, String),
    Alter(PostgresField),
}

#[derive(Debug)]
pub struct StructUpgrade {
    name: SchemaName,
    mods: Vec<StructMod>,
}

#[derive(Debug)]
pub struct StructAlter {
    name: SchemaName,
    field: String,
    pg_type: PostgresType,
}

#[derive(Debug)]
pub struct EnumUpgrade {
    name: SchemaName,
    rename: Vec<(String, String)>,
    add: Vec<String>,
}

#[derive(Debug)]
pub struct DeadFieldWithId {
    pub id: u128,
    pub name: String,
    pub type_def: TypeDef,
}

impl From<DeadFieldWithId> for (u128, DeadField) {
    fn from(value: DeadFieldWithId) -> Self {
        (
            value.id,
            DeadField {
                name: value.name,
                type_def: value.type_def,
            },
        )
    }
}

impl From<(u128, DeadField)> for DeadFieldWithId {
    fn from(value: (u128, DeadField)) -> Self {
        DeadFieldWithId {
            id: value.0,
            name: value.1.name,
            type_def: value.1.type_def,
        }
    }
}

#[derive(Debug)]
pub enum CreatesType {
    Struct(CreateStruct),
    Enum(CreateEnum),
}

impl Display for CreatePgTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            r#"CREATE TABLE IF NOT EXISTS {} ({}, "#,
            self.name, self.primary
        )?;
        for column in &self.columns {
            write!(f, "{column}, ")?;
        }
        CREATE_METADATA_COLUMNS.fmt(f)
    }
}

impl Display for CreateStruct {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "CREATE TYPE {} AS (", self.name)?;
        if let Some((last, batch)) = self.fields.split_last() {
            for field in batch {
                write!(f, "{field}, ")?;
            }
            last.fmt(f)?;
        }
        write!(f, ");")
    }
}

impl Display for CreateEnum {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "CREATE TYPE {} AS ENUM (", self.name)?;
        if let Some((last, batch)) = self.variants.split_last() {
            for field in batch {
                write!(f, "'{field}', ")?;
            }
            write!(f, "'{last}'")?;
        }
        write!(f, ");")
    }
}

impl Display for CreatesType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            CreatesType::Struct(s) => s.fmt(f),
            CreatesType::Enum(e) => e.fmt(f),
        }
    }
}

impl CreatesType {
    pub fn new_struct<S: Into<String>>(
        schema: &Rc<PgSchema>,
        name: S,
        fields: Vec<PostgresField>,
    ) -> Self {
        Self::Struct(CreateStruct {
            name: SchemaName::new(schema, name),
            fields,
        })
    }

    pub fn new_enum<S: Into<String>>(
        schema: &Rc<PgSchema>,
        name: S,
        variants: Vec<String>,
    ) -> Self {
        Self::Enum(CreateEnum {
            name: SchemaName::new(schema, name),
            variants,
        })
    }
}

impl TableUpgrade {
    pub fn new<S: Into<String>>(schema: &Rc<PgSchema>, name: S) -> Self {
        Self {
            schema: schema.clone(),
            name: name.into(),
            old_name: None,
            columns: Vec::new(),
            columns_upgraded: Vec::new(),
            alters: Vec::new(),
            atomic: Vec::new(),
            dead: Vec::new(),
            col_alters: Vec::new(),
        }
    }
    pub fn rename_column(&mut self, old: &mut String, new: &str) -> bool {
        let renamed = old != new;
        if renamed {
            let old = std::mem::replace(old, new.to_string());
            self.columns.push(ColumnMod::Rename(old, new.to_string()));
        }
        renamed
    }
    pub fn rename_table(&mut self, new: &str) {
        if self.name != new {
            self.old_name = Some(std::mem::replace(&mut self.name, new.into()));
        }
    }
    pub fn retype_primary(&mut self, name: &str, pg_type: Option<PostgresType>) {
        if let Some(pg_type) = pg_type {
            self.columns.push(ColumnMod::Alter(pg_type.to_field(name)));
        }
    }
    pub fn retype_column(
        &mut self,
        column: &ColumnDef,
        pg_type: Option<PostgresType>,
        upgrade: ColumnUpgrade,
        field: PostgresType,
    ) {
        if let Some(pg_type) = pg_type {
            self.columns
                .push(ColumnMod::Alter(pg_type.to_field(&column.name)));
        }
        if upgrade.altered {
            self.col_alters.push(field.to_field(&column.name));
        }
        if upgrade.upgraded {
            self.columns_upgraded.push(column.id);
        }
        self.atomic = upgrade.atomic;
        self.alters = upgrade.alters;
        self.dead = upgrade.dead;
    }

    pub fn add_column(&mut self, id: Felt, name: &str, pg_type: PostgresType) {
        self.columns.push(ColumnMod::Add(PostgresField::new(
            name.to_string(),
            pg_type,
        )));
        self.columns_upgraded.push(id);
    }

    pub fn column_upgrade(&mut self, upgraded: bool) -> ColumnUpgrade {
        ColumnUpgrade {
            atomic: std::mem::take(&mut self.atomic),
            alters: std::mem::take(&mut self.alters),
            dead: std::mem::take(&mut self.dead),
            altered: false,
            upgraded,
        }
    }

    pub fn to_queries(
        &self,
        table_id: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> SqlxResult<()> {
        let schema = &self.schema;
        let name = &self.name;
        queries.add(
            insert_dead_member_query(
                &self.schema,
                table_id,
                &self.dead,
                block_number,
                transaction_hash,
            )
            .map_err(EncodeError)?,
        );
        if let Some(old_name) = &self.old_name {
            queries.add(format!(
                r#"ALTER TABLE "{schema}"."{old_name}" RENAME TO "{name}";"#
            ));
        }
        self.atomic.iter().for_each(|m| m.to_queries(queries));
        if let Some((last, columns)) = self.columns.split_last() {
            let mut alterations = format!(r#"ALTER TABLE "{schema}"."{name}" "#);
            columns
                .iter()
                .for_each(|m| write!(alterations, "{m}, ").unwrap());
            write!(alterations, "{last};").unwrap();
            queries.add(alterations);
        }
        self.alter_queries(queries);
        Ok(())
    }

    fn alter_queries(&self, queries: &mut Vec<PgQuery>) {
        if let Some((last, others)) = self.col_alters.split_last() {
            let (schema, name) = (&self.schema, &self.name);
            let mut forward = format!(r#"ALTER TABLE "{schema}"."{name}" "#);
            let mut reverse = forward.clone();
            for PostgresField { name: col, pg_type } in others {
                write!(
                    forward,
                    r#"ALTER COLUMN "{col}" TYPE jsonb USING to_jsonb("{col}"),"#
                )
                .unwrap();
                write!(
                    reverse,
                    r#"ALTER COLUMN "{col}" TYPE {pg_type} USING jsonb_populate_record(null::{pg_type}, "{col}"),"#
                )
                .unwrap();
            }
            let PostgresField { name: col, pg_type } = last;
            write!(
                forward,
                r#"ALTER COLUMN "{col}" TYPE jsonb USING to_jsonb("{col}");"#
            )
            .unwrap();
            write!(
                reverse,
                r#"ALTER COLUMN "{col}" TYPE {pg_type} USING jsonb_populate_record(null::{pg_type}, "{col}");"#
            )
            .unwrap();
            queries.add(forward);
            self.alters.iter().for_each(|a| queries.add(a.to_string()));
            queries.add(reverse);
        }
    }
}

impl Display for ColumnMod {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ColumnMod::Add(field) => write!(f, r#"ADD COLUMN {field}"#),
            ColumnMod::Alter(PostgresField { name, pg_type }) => {
                write!(f, r#"ALTER COLUMN "{name}" TYPE {pg_type}"#,)
            }
            ColumnMod::Rename(old, new) => write!(f, r#"RENAME COLUMN "{old}" TO "{new}""#),
        }
    }
}

impl StructUpgrade {
    fn to_queries(&self, queries: &mut Vec<PgQuery>) {
        let name = &self.name;
        self.mods
            .iter()
            .for_each(|m| queries.add(format!("ALTER TYPE {name} {m};")));
    }
}

impl Display for StructMod {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            StructMod::Add(field) => write!(f, "ADD ATTRIBUTE {field}"),
            StructMod::Rename(old, new) => write!(f, r#"RENAME ATTRIBUTE "{old}" TO "{new}""#),
        }
    }
}

impl Display for StructAlter {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            r#"ALTER TYPE {} ALTER ATTRIBUTE "{}" TYPE {};"#,
            self.name, self.field, self.pg_type
        )
    }
}

impl EnumUpgrade {
    fn to_queries(&self, queries: &mut Vec<PgQuery>) {
        let name = &self.name;
        for (old, new) in &self.rename {
            queries.add(format!(
                r#"ALTER TYPE {name} RENAME VALUE '{old}' TO '{new}';"#
            ));
        }
        for variant in &self.add {
            queries.add(format!(r#"ALTER TYPE {name} ADD VALUE '{variant}';"#));
        }
        queries.add(COMMIT_CMD);
    }
}

impl TypeMod {
    fn to_queries(&self, queries: &mut Vec<PgQuery>) {
        match self {
            TypeMod::Struct(upgrade) => upgrade.to_queries(queries),
            TypeMod::Enum(upgrade) => upgrade.to_queries(queries),
            TypeMod::Create(create) => queries.add(create.to_string()),
        }
    }
}

impl StructMod {
    pub fn add<S: Into<String>>(name: S, pg_type: PostgresType) -> Self {
        StructMod::Add(PostgresField::new(name.into(), pg_type))
    }

    pub fn add_field(field: PostgresField) -> Self {
        StructMod::Add(field)
    }
}

pub trait TypeMods {
    fn add_mod(&mut self, type_mod: TypeMod);
}

impl TypeMods for Vec<TypeMod> {
    fn add_mod(&mut self, type_mod: TypeMod) {
        self.push(type_mod);
    }
}

impl ColumnUpgrade {
    pub fn maybe_alter(
        &mut self,
        schema: &Rc<PgSchema>,
        name: &str,
        field: &str,
        pg_type: Option<PostgresType>,
    ) {
        if let Some(pg_type) = pg_type {
            self.alters.push(StructAlter {
                name: SchemaName::new(schema, name),
                field: field.to_string(),
                pg_type,
            });
            self.upgraded = true;
        }
    }

    pub fn add_struct_mod<S: Into<String>>(
        &mut self,
        schema: &Rc<PgSchema>,
        name: S,
        mods: Vec<StructMod>,
    ) {
        if !mods.is_empty() {
            self.upgraded = true;
            self.atomic.push(TypeMod::Struct(StructUpgrade {
                name: SchemaName::new(schema, name),
                mods,
            }))
        }
    }
    pub fn add_enum_mod<S: Into<String>>(
        &mut self,
        schema: &Rc<PgSchema>,
        name: S,
        rename: Vec<(String, String)>,
        add: Vec<String>,
    ) {
        if !rename.is_empty() || !add.is_empty() {
            self.upgraded = true;

            self.atomic.push(TypeMod::Enum(EnumUpgrade {
                name: SchemaName::new(schema, name),
                rename,
                add,
            }))
        }
    }
    pub fn add_dead_member(&mut self, id: u128, member: &MemberDef) {
        self.upgraded = true;
        self.dead.push(DeadFieldWithId {
            id,
            name: member.name.clone(),
            type_def: member.type_def.clone(),
        });
    }
}

pub trait StructMods {
    fn add_mod(&mut self, struct_mod: StructMod);
    fn add<S: Into<String>>(&mut self, name: S, pg_type: PostgresType) {
        self.add_mod(StructMod::add(name, pg_type));
    }
    fn add_field(&mut self, field: PostgresField) {
        self.add_mod(StructMod::add_field(field));
    }
    fn rename<T: Into<String>, S: Into<String>>(&mut self, old: T, new: S) {
        self.add_mod(StructMod::Rename(old.into(), new.into()));
    }
}

impl StructMods for Vec<StructMod> {
    fn add_mod(&mut self, struct_mod: StructMod) {
        self.push(struct_mod);
    }
}

impl From<CreatesType> for TypeMod {
    fn from(value: CreatesType) -> Self {
        TypeMod::Create(value)
    }
}

impl From<ColumnRow> for (Felt, Felt, ColumnInfo) {
    fn from(value: ColumnRow) -> Self {
        (
            value.table.into(),
            value.id.into(),
            ColumnInfo {
                name: value.name,
                attributes: Vec::new(),
                type_def: value.type_def.0,
            },
        )
    }
}

impl From<DeadFieldRow> for (Felt, u128, DeadField) {
    fn from(value: DeadFieldRow) -> Self {
        (
            value.table.into(),
            value.id.into(),
            DeadField {
                name: value.name,
                type_def: value.type_def.0,
            },
        )
    }
}

impl From<TableRow> for PgTableHead {
    fn from(value: TableRow) -> Self {
        let row = value;
        PgTableHead {
            id: row.id.into(),
            name: row.name,
            primary: row.primary_def.into(),
        }
    }
}

impl PgTableHead {
    pub fn to_table(self, schema: &PgSchema) -> (Felt, PgTable) {
        (
            self.id,
            PgTable {
                schema: schema.clone(),
                name: self.name,
                primary: self.primary,
                columns: HashMap::new(),
                alive: true,
                dead: HashMap::new(),
            },
        )
    }
}

fn insert_dead_member_query(
    schema: &PgSchema,
    table: &Felt,
    fields: &[DeadFieldWithId],
    block_number: u64,
    transaction_hash: &Felt,
) -> Result<PgQuery, BoxDynError> {
    let mut args = PgArguments::default();
    args.add(schema.to_string())?;
    args.add(PgFelt::from(*table))?;
    args.add(fields.iter().map(|f| f.id.to_string()).collect::<Vec<_>>())?;
    args.add(fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>())?;
    args.add(fields.iter().map(|f| Json(&f.type_def)).collect::<Vec<_>>())?;
    args.add(block_number.to_string())?;
    args.add(PgFelt::from(*transaction_hash))?;

    Ok(PgQuery::new(INSERT_DEAD_MEMBER_QUERY, args))
}

pub fn insert_columns_query(
    schema: &PgSchema,
    table: &Felt,
    columns: Vec<(&Felt, &ColumnInfo)>,
    block_number: u64,
    transaction_hash: &Felt,
) -> Result<PgQuery, BoxDynError> {
    let mut args = PgArguments::default();
    args.add(schema.to_string())?;
    args.add(PgFelt::from(*table))?;
    args.add(
        columns
            .iter()
            .map(|(id, _)| PgFelt::from(*id))
            .collect_vec(),
    )?;
    args.add(columns.iter().map(|(_, c)| c.name.clone()).collect_vec())?;
    args.add(columns.iter().map(|(_, c)| Json(&c.type_def)).collect_vec())?;
    args.add(block_number.to_string())?;
    args.add(PgFelt::from(*transaction_hash))?;

    Ok(PgQuery::new(INSERT_COLUMN_QUERY, args))
}

pub fn insert_table_query(
    schema: &PgSchema,
    id: &Felt,
    name: &str,
    primary_def: &PrimaryDef,
    block_number: u64,
    transaction_hash: &Felt,
) -> Result<PgQuery, BoxDynError> {
    let mut args = PgArguments::default();
    args.add(schema.to_string())?;
    args.add(PgFelt::from(*id))?;
    args.add(name.to_owned())?;
    args.add(PgPrimary::from(primary_def))?;
    args.add(block_number.to_string())?;
    args.add(PgFelt::from(*transaction_hash))?;

    Ok(PgQuery::new(INSERT_TABLE_QUERY, args))
}

pub async fn fetch_tables<'e, 'c, E: 'e + Executor<'c, Database = Postgres>>(
    conn: E,
    schema: &PgSchema,
) -> SqlxResult<Vec<PgTableHead>> {
    sqlx::query_as::<_, TableRow>(FETCH_TABLES_QUERY)
        .bind(schema.to_string())
        .fetch_all(conn)
        .await
        .map(|rows| rows.into_iter().map_into().collect())
}

pub async fn fetch_columns<'e, 'c, E: 'e + Executor<'c, Database = Postgres>>(
    conn: E,
    schema: &PgSchema,
) -> SqlxResult<Vec<(Felt, Felt, ColumnInfo)>> {
    sqlx::query_as::<_, ColumnRow>(FETCH_COLUMNS_QUERY)
        .bind(schema.to_string())
        .fetch_all(conn)
        .await
        .map(|rows| rows.into_iter().map_into().collect())
}

pub async fn fetch_dead_fields<'e, 'c, E: 'e + Executor<'c, Database = Postgres>>(
    conn: E,
    schema: &PgSchema,
) -> SqlxResult<Vec<(Felt, u128, DeadField)>> {
    sqlx::query_as::<_, DeadFieldRow>(FETCH_DEAD_FIELDS_QUERY)
        .bind(schema.to_string())
        .fetch_all(conn)
        .await
        .map(|rows| rows.into_iter().map_into().collect())
}
