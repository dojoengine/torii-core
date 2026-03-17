use crate::{PgSchema, PostgresField, PostgresType, PrimaryKey, SchemaName};
use std::{
    fmt::{Display, Formatter, Result as FmtResult, Write},
    rc::Rc,
};

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
    pub col_alters: Vec<PostgresField>,
}

#[derive(Debug)]
pub struct ColumnUpgrade {
    pub atomic: Vec<TypeMod>,
    pub alters: Vec<StructAlter>,
    pub altered: bool,
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
pub enum CreatesType {
    Struct(CreateStruct),
    Enum(CreateEnum),
}

impl Display for CreatePgTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            r#"CREATE TABLE IF NOT EXISTS {} ({}"#,
            self.name, self.primary
        )?;
        for column in &self.columns {
            write!(f, ", {column}")?;
        }
        write!(f, ");")
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
            alters: Vec::new(),
            atomic: Vec::new(),
            col_alters: Vec::new(),
        }
    }
    pub fn rename_column(&mut self, old: &mut String, new: &str) {
        if old != new {
            let old = std::mem::replace(old, new.to_string());
            self.columns.push(ColumnMod::Rename(old, new.to_string()));
        }
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
        name: &str,
        pg_type: Option<PostgresType>,
        upgrade: ColumnUpgrade,
        field: PostgresType,
    ) {
        if let Some(pg_type) = pg_type {
            self.columns.push(ColumnMod::Alter(pg_type.to_field(name)));
        }
        if upgrade.altered {
            self.col_alters.push(field.to_field(name));
        }
        self.atomic = upgrade.atomic;
        self.alters = upgrade.alters;
    }

    pub fn add_column(&mut self, name: &str, pg_type: PostgresType) {
        self.columns.push(ColumnMod::Add(PostgresField::new(
            name.to_string(),
            pg_type,
        )));
    }

    pub fn column_upgrade(&mut self) -> ColumnUpgrade {
        ColumnUpgrade {
            atomic: std::mem::take(&mut self.atomic),
            alters: std::mem::take(&mut self.alters),
            altered: false,
        }
    }

    pub fn to_queries(&self, queries: &mut Vec<String>) {
        println!("{self:?}");
        let schema = &self.schema;
        let name = &self.name;
        if let Some(old_name) = &self.old_name {
            queries.push(format!(
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
            queries.push(alterations);
        }
        self.alter_queries(queries);
        for q in queries.iter() {
            println!("{q}");
        }
    }

    fn alter_queries(&self, queries: &mut Vec<String>) {
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
            queries.push(forward);
            self.alters.iter().for_each(|a| queries.push(a.to_string()));
            queries.push(reverse);
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
    fn to_queries(&self, queries: &mut Vec<String>) {
        let name = &self.name;
        self.mods
            .iter()
            .for_each(|m| queries.push(format!("ALTER TYPE {name} {m};")));
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
    fn to_queries(&self, queries: &mut Vec<String>) {
        let name = &self.name;
        for (old, new) in &self.rename {
            queries.push(format!(
                r#"ALTER TYPE {name} RENAME VALUE '{old}' TO '{new}';"#
            ));
        }
        for variant in &self.add {
            queries.push(format!(r#"ALTER TYPE {name} ADD VALUE '{variant}';"#));
        }
    }
}

impl TypeMod {
    fn to_queries(&self, queries: &mut Vec<String>) {
        match self {
            TypeMod::Struct(upgrade) => upgrade.to_queries(queries),
            TypeMod::Enum(upgrade) => upgrade.to_queries(queries),
            TypeMod::Create(create) => queries.push(create.to_string()),
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
        }
    }

    pub fn add_struct_mod<S: Into<String>>(
        &mut self,
        schema: &Rc<PgSchema>,
        name: S,
        mods: Vec<StructMod>,
    ) {
        if !mods.is_empty() {
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
            self.atomic.push(TypeMod::Enum(EnumUpgrade {
                name: SchemaName::new(schema, name),
                rename,
                add,
            }))
        }
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
