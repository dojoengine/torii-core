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
    pub type_mods: Vec<TypeMod>,
    pub columns: Vec<ColumnMod>,
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
    Alter(PostgresField),
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
            type_mods: Vec::new(),
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
    pub fn retype_column(&mut self, name: &str, pg_type: Option<PostgresType>) {
        if let Some(pg_type) = pg_type {
            self.columns
                .push(ColumnMod::Alter(pg_type.to_field(name.to_string())));
        }
    }
    pub fn add_column(&mut self, name: &str, pg_type: PostgresType) {
        self.columns.push(ColumnMod::Add(PostgresField::new(
            name.to_string(),
            pg_type,
        )));
    }

    pub fn to_queries(&self, queries: &mut Vec<String>) {
        let schema = &self.schema;
        let name = &self.name;
        if let Some(old_name) = &self.old_name {
            queries.push(format!(
                r#"ALTER TABLE "{schema}"."{old_name}" RENAME TO "{name}";"#
            ));
        }
        self.type_mods.iter().for_each(|m| m.to_queries(queries));
        if let Some((last, columns)) = self.columns.split_last() {
            let mut alterations = format!(r#"ALTER TABLE "{schema}"."{name}" "#);
            columns
                .iter()
                .for_each(|m| write!(alterations, "{m}, ").unwrap());
            write!(alterations, "{last};").unwrap();
            queries.push(alterations);
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
            StructMod::Alter(field) => write!(
                f,
                "ALTER ATTRIBUTE \"{}\" TYPE {}",
                field.name, field.pg_type
            ),
            StructMod::Rename(old, new) => write!(f, r#"RENAME ATTRIBUTE "{old}" TO "{new}""#),
        }
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

    pub fn alter<S: Into<String>>(name: S, pg_type: PostgresType) -> Self {
        StructMod::Alter(PostgresField::new(name.into(), pg_type))
    }

    pub fn alter_field(field: PostgresField) -> Self {
        StructMod::Alter(field)
    }
}

pub trait TypeMods {
    fn add_mod(&mut self, type_mod: TypeMod);
    fn add_struct_mod<S: Into<String>>(
        &mut self,
        schema: &Rc<PgSchema>,
        name: S,
        mods: Vec<StructMod>,
    ) {
        if !mods.is_empty() {
            self.add_mod(TypeMod::Struct(StructUpgrade {
                name: SchemaName::new(schema, name),
                mods,
            }))
        }
    }
    fn add_enum_mod<S: Into<String>>(
        &mut self,
        schema: &Rc<PgSchema>,
        name: S,
        rename: Vec<(String, String)>,
        add: Vec<String>,
    ) {
        if !rename.is_empty() || !add.is_empty() {
            self.add_mod(TypeMod::Enum(EnumUpgrade {
                name: SchemaName::new(schema, name),
                rename,
                add,
            }))
        }
    }
}

impl TypeMods for Vec<TypeMod> {
    fn add_mod(&mut self, type_mod: TypeMod) {
        self.push(type_mod);
    }
}

pub trait StructMods {
    fn add_mod(&mut self, struct_mod: StructMod);
    fn add<S: Into<String>>(&mut self, name: S, pg_type: PostgresType) {
        self.add_mod(StructMod::add(name, pg_type));
    }
    fn alter<S: Into<String>>(&mut self, name: S, pg_type: PostgresType) {
        self.add_mod(StructMod::alter(name, pg_type));
    }
    fn add_field(&mut self, field: PostgresField) {
        self.add_mod(StructMod::add_field(field));
    }
    fn alter_field(&mut self, field: PostgresField) {
        self.add_mod(StructMod::alter_field(field));
    }
    fn rename<T: Into<String>, S: Into<String>>(&mut self, old: T, new: S) {
        self.add_mod(StructMod::Rename(old.into(), new.into()));
    }
    fn maybe_alter<S: Into<String>>(&mut self, name: S, pg_type: Option<PostgresType>) {
        if let Some(pg_type) = pg_type {
            self.alter(name, pg_type);
        }
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
