use sqlx::{postgres::PgArguments, query::Query, Postgres};
use std::fmt::Write;
use torii::etl::EventContext;

pub const INSERTS: &str = "__created_block, __updated_block, __created_tx, __updated_tx";
pub const CONFLICTS: &str = "__updated_at = NOW(), __updated_block = EXCLUDED.__updated_block, __updated_tx = EXCLUDED.__updated_tx";

pub trait PgMetadata {
    fn insert_string(
        &self,
        schema: &str,
        table: &str,
        primary_name: &str,
        primary_value: String,
    ) -> String;
    fn static_insert_query(schema: &str, table: &str, primary_name: &str) -> String {
        format!(
            r#"
            INSERT INTO "{schema}"."{table}" ("{primary_name}", {INSERTS})
            VALUES ($1, $2, $2, $3, $3) 
            ON CONFLICT ({primary_name}) DO UPDATE SET {CONFLICTS}"#
        )
    }
    fn insert_values(&self, string: &mut String);
    fn bind_query<'a>(
        &self,
        query: &'a str,
        primary_value: String,
    ) -> Query<'a, Postgres, PgArguments>;
}

impl PgMetadata for EventContext {
    fn insert_string(
        &self,
        schema: &str,
        table: &str,
        primary_name: &str,
        primary_value: String,
    ) -> String {
        let mut string = format!(
            r#"INSERT INTO "{schema}"."{table}" ("{primary_name}", {INSERTS}) VALUES ({primary_value}, "#
        );
        self.insert_values(&mut string);
        write!(
            string,
            ") ON CONFLICT ({primary_name}) DO UPDATE SET {CONFLICTS}"
        )
        .unwrap();
        string
    }
    fn bind_query<'a>(
        &self,
        query: &'a str,
        primary_value: String,
    ) -> Query<'a, Postgres, PgArguments> {
        let block_number = self.block.number.to_string();
        let tx_hash = self.transaction.hash.to_bytes_be();
        sqlx::query::<Postgres>(query)
            .bind(primary_value.clone())
            .bind(block_number)
            .bind(tx_hash)
    }
    fn insert_values(&self, string: &mut String) {
        let block_number = self.block.number;
        let tx_hash = hex::encode(self.transaction.hash.to_bytes_be());
        write!(
            string,
            "{block_number}, {block_number}, '\\x{tx_hash}', '\\x{tx_hash}'"
        )
        .unwrap();
    }
}
