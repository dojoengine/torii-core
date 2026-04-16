use std::ffi::CString;
use std::os::raw::c_int;
use std::ptr;

use libsqlite3_sys::{
    sqlite3, sqlite3_create_function_v2, sqlite3_result_error, sqlite3_result_int64,
    sqlite3_result_null, sqlite3_value, sqlite3_value_text, sqlite3_value_type, SQLITE_OK,
    SQLITE_TEXT, SQLITE_UTF8,
};
use sqlx::sqlite::SqliteConnection;

/// Register all custom UDFs on the given connection.
#[allow(unsafe_code)]
pub async fn register_udfs(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let mut handle = conn.lock_handle().await?;
    let raw = handle.as_raw_handle().as_ptr();

    // SAFETY: raw is a valid sqlite3* handle, and we hold the lock.
    unsafe {
        register_hex2int(raw);
    }

    Ok(())
}

/// Register the `hex2int` scalar function on a raw sqlite3 handle.
///
/// `hex2int(hex_string)` converts a hex-encoded integer (with or without `0x` prefix)
/// to an i64. Returns NULL for NULL input.
#[allow(unsafe_code)]
unsafe fn register_hex2int(db: *mut sqlite3) {
    let name = CString::new("hex2int").unwrap();
    let rc = sqlite3_create_function_v2(
        db,
        name.as_ptr(),
        1,                // nArg
        SQLITE_UTF8,      // eTextRep
        ptr::null_mut(),  // pApp
        Some(hex2int_fn), // xFunc
        None,             // xStep
        None,             // xFinal
        None,             // xDestroy
    );
    assert_eq!(rc, SQLITE_OK, "failed to register hex2int UDF");
}

/// The C callback implementing hex2int.
#[allow(unsafe_code)]
unsafe extern "C" fn hex2int_fn(
    ctx: *mut libsqlite3_sys::sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    debug_assert_eq!(argc, 1);
    let val = *argv;

    // NULL in → NULL out
    if sqlite3_value_type(val) == libsqlite3_sys::SQLITE_NULL {
        sqlite3_result_null(ctx);
        return;
    }

    // Must be text
    if sqlite3_value_type(val) != SQLITE_TEXT {
        let msg = CString::new("hex2int: expected text argument").unwrap();
        sqlite3_result_error(ctx, msg.as_ptr(), -1);
        return;
    }

    let text_ptr = sqlite3_value_text(val);
    if text_ptr.is_null() {
        sqlite3_result_null(ctx);
        return;
    }

    let text = std::ffi::CStr::from_ptr(text_ptr as *const _)
        .to_str()
        .unwrap_or("");
    let hex = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")).unwrap_or(text);

    match u64::from_str_radix(hex, 16) {
        Ok(n) => sqlite3_result_int64(ctx, n as i64),
        Err(_) => {
            let msg = CString::new(format!("hex2int: invalid hex string '{text}'")).unwrap();
            sqlite3_result_error(ctx, msg.as_ptr(), -1);
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::Row;

    use super::*;

    #[tokio::test]
    async fn test_hex2int() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        register_udfs(conn.as_mut()).await.unwrap();

        // Basic hex conversion
        let row = sqlx::query("SELECT hex2int('0xff') AS v")
            .fetch_one(conn.as_mut())
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("v"), 255);

        // Without prefix
        let row = sqlx::query("SELECT hex2int('ff') AS v")
            .fetch_one(conn.as_mut())
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("v"), 255);

        // Large value
        let row = sqlx::query("SELECT hex2int('0xffffffffffffffff') AS v")
            .fetch_one(conn.as_mut())
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("v"), -1); // u64::MAX as i64

        // NULL passthrough
        let row = sqlx::query("SELECT hex2int(NULL) AS v")
            .fetch_one(conn.as_mut())
            .await
            .unwrap();
        assert!(row.try_get::<i64, _>("v").is_err() || row.get::<Option<i64>, _>("v").is_none());

        // Zero
        let row = sqlx::query("SELECT hex2int('0x0') AS v")
            .fetch_one(conn.as_mut())
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("v"), 0);

        // Uppercase prefix
        let row = sqlx::query("SELECT hex2int('0XAB') AS v")
            .fetch_one(conn.as_mut())
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("v"), 171);
    }
}
