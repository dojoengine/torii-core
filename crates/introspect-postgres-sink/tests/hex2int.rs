//! Integration test for the `hex2int` Postgres function shipped via migration
//! `004_hex2int_function.sql`. Requires a running Postgres reachable via
//! `DATABASE_URL`; skipped otherwise so local `cargo test` still passes.

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use torii_introspect_postgres_sink::INTROSPECT_PG_SINK_MIGRATIONS;

async fn get_pool() -> Option<sqlx::PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("failed to connect to DATABASE_URL");
    INTROSPECT_PG_SINK_MIGRATIONS
        .run(&pool)
        .await
        .expect("failed to run migrations");
    Some(pool)
}

async fn hex2int(pool: &sqlx::PgPool, input: Option<&str>) -> Option<i64> {
    let row = sqlx::query("SELECT hex2int($1) AS v")
        .bind(input)
        .fetch_one(pool)
        .await
        .unwrap();
    row.try_get::<Option<i64>, _>("v").unwrap()
}

#[tokio::test]
async fn test_hex2int_postgres() {
    let Some(pool) = get_pool().await else {
        eprintln!("DATABASE_URL not set; skipping hex2int Postgres test");
        return;
    };

    assert_eq!(hex2int(&pool, Some("0xff")).await, Some(255));
    assert_eq!(hex2int(&pool, Some("ff")).await, Some(255));
    assert_eq!(hex2int(&pool, Some("0x0")).await, Some(0));
    assert_eq!(hex2int(&pool, Some("0XAB")).await, Some(171));

    // u64::MAX → -1 as i64
    assert_eq!(hex2int(&pool, Some("0xffffffffffffffff")).await, Some(-1));

    // NULL passthrough
    assert_eq!(hex2int(&pool, None).await, None);

    // 256-bit value zero-padded: only the lower 64 bits are kept
    assert_eq!(
        hex2int(
            &pool,
            Some("0x00000000000000000000000000000000000000000000000000000000000000ff"),
        )
        .await,
        Some(255),
    );

    // 256-bit value with non-zero high bits: high bits ignored, lower 64 bits returned
    assert_eq!(
        hex2int(
            &pool,
            Some("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef1234567890abcdef"),
        )
        .await,
        Some(0x1234567890abcdef_i64),
    );
}
