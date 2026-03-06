use std::io::Write;

use crate::{PostgresField, PostgresType};

pub fn add_column_query(name: &str, pg_type: &PostgresType) -> String {
    format!("ADD COLUMN \"{name}\" {pg_type}")
}

pub fn add_column_if_not_exists_query(name: &str, pg_type: &PostgresType) -> String {
    format!("ADD COLUMN IF NOT EXISTS \"{name}\" {pg_type}")
}

pub fn modify_column_query(name: &str, pg_type: &PostgresType) -> String {
    format!("ALTER COLUMN \"{name}\" TYPE {pg_type}")
}

pub fn add_member_query(type_name: &str, member_name: &str, pg_type: &PostgresType) -> String {
    format!(
        r#"
DO $$
BEGIN
    BEGIN
        ALTER TYPE "{type_name}" ADD ATTRIBUTE "{member_name}" {pg_type};
    EXCEPTION
        WHEN duplicate_object OR duplicate_column THEN
            RAISE NOTICE 'attribute already exists, skipping';
    END;
END $$;
"#,
        pg_type = pg_type
    )
}

pub fn modify_member_query(type_name: &str, member_name: &str, pg_type: &PostgresType) -> String {
    format!(r#"ALTER TYPE "{type_name}" ALTER ATTRIBUTE "{member_name}" TYPE {pg_type};"#)
}

pub fn add_enum_variant_query(type_name: &str, variant: &str) -> String {
    format!(
        r#"
DO $$ 
BEGIN
  BEGIN
    EXECUTE format('ALTER TYPE %I ADD VALUE %L', '{type_name}', '{variant}');
  EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'enum value already exists, skipping';
  END;
END $$;
"#,
    )
}

pub fn create_table_query(table_name: &str, columns: &[String]) -> String {
    let columns_sql = columns.join(", ");
    format!(r#"CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_sql});"#)
}

pub fn create_struct_type_query(type_name: &str, fields: &[PostgresField]) -> String {
    let field_defs = fields
        .iter()
        .map(|f| format!(r#""{}" {}"#, f.name, f.pg_type))
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

    let create = format!(
        r#"DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{type_name}') THEN
                CREATE TYPE "{type_name}" AS ENUM ({variant_defs});
            END IF;
        END $$;"#
    );
    let reconcile = variants
        .iter()
        .map(|variant| add_enum_variant_query(type_name, variant))
        .collect::<Vec<_>>()
        .join("\n");
    format!("{create}\n{reconcile}")
}

pub fn create_tuple_type_query(type_name: &str, fields: &[PostgresType]) -> String {
    let field_defs = fields
        .iter()
        .enumerate()
        .map(|(i, f)| format!("\"_{i}\" {f}"))
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

pub fn write_conflict_res<const DELIMINATOR: bool, W: Write>(
    writer: &mut W,
    table: &str,
    column: &str,
) -> std::io::Result<()> {
    let separator = if DELIMINATOR { ", " } else { "" };
    write!(
        writer,
        r#""{column}" = COALESCE(EXCLUDED."{column}", "{table}"."{column}"){separator}"#,
    )
}
