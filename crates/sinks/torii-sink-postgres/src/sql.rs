use crate::{PostgresField, PostgresType};

pub fn add_column_query(name: &str, pg_type: &PostgresType) -> String {
    format!(r#"ADD COLUMN "{name}" {}"#, pg_type.to_string())
}

pub fn modify_column_query(name: &str, pg_type: &PostgresType) -> String {
    format!(r#"ALTER COLUMN "{name}" TYPE {}"#, pg_type.to_string())
}

pub fn add_member_query(type_name: &str, member_name: &str, pg_type: &PostgresType) -> String {
    format!(
        r#"ALTER TYPE "{type_name}" ADD ATTRIBUTE "{member_name}" {};"#,
        pg_type.to_string()
    )
}

pub fn modify_member_query(type_name: &str, member_name: &str, pg_type: &PostgresType) -> String {
    format!(
        r#"ALTER TYPE "{type_name}" ALTER ATTRIBUTE "{member_name}" TYPE {};"#,
        pg_type.to_string()
    )
}

pub fn add_enum_variant_query(type_name: &str, variant: &str) -> String {
    format!(r#"ALTER TYPE "{type_name}" ADD VALUE '{variant}';"#)
}

pub fn create_table_query(table_name: &str, columns: &[String]) -> String {
    let columns_sql = columns.join(", ");
    format!(r#"CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_sql});"#)
}

pub fn create_struct_type_query(type_name: &str, fields: &[PostgresField]) -> String {
    let field_defs = fields
        .iter()
        .map(|f| format!(r#""{}" {}"#, f.name, f.pg_type.to_string()))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r#"DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{type_name}') THEN
                CREATE TYPE "{type_name}" AS ({field_defs});
            END IF;
        END $$;"#
    )
}

pub fn create_enum_type_query(type_name: &str, variants: &[String]) -> String {
    let variant_defs = variants
        .iter()
        .map(|v| format!(r#"'{}'"#, v))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r#"DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{type_name}') THEN
                CREATE TYPE "{type_name}" AS ENUM ({variant_defs});
            END IF;
        END $$;"#
    )
}

pub fn create_tuple_type_query(type_name: &str, fields: &[PostgresType]) -> String {
    let field_defs = fields
        .iter()
        .enumerate()
        .map(|(i, f)| format!(r#""_{i}" {}"#, f.to_string()))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r#"DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{type_name}') THEN
                CREATE TYPE "{type_name}" AS ({field_defs});
            END IF;
        END $$;"#
    )
}

pub fn alter_table_query(table_name: &str, alterations: &[String]) -> String {
    let alterations_sql = alterations.join(", ");
    format!(r#"ALTER TABLE "{table_name}" {alterations_sql};"#)
}
