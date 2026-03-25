use introspect_types::{ColumnDef, PrimaryDef};

pub fn create_table_query(
    namespace: &str,
    name: &str,
    primary: &PrimaryDef,
    columns: &[ColumnDef],
) -> String {
    let mut columns_sql = Vec::with_capacity(columns.len() + 1);
    columns_sql.push(format!(r#""{}" TEXT PRIMARY KEY"#, primary.name));
    for column in columns {
        columns_sql.push(format!(r#""{}" TEXT"#, column.name));
    }
    format!(
        r#"CREATE TABLE IF NOT EXISTS "{}" ({});"#,
        table_name(&namespace, name),
        columns_sql.join(", ")
    )
}

pub fn table_name(namespace: &str, name: &str) -> String {
    format!("{namespace}__{name}")
}
