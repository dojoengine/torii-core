use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::Once;

use libsqlite3_sys::{
    sqlite3, sqlite3_api_routines, sqlite3_auto_extension, sqlite3_create_function_v2,
    sqlite3_result_error, sqlite3_result_int64, sqlite3_result_null, sqlite3_value,
    sqlite3_value_text, sqlite3_value_type, SQLITE_OK, SQLITE_TEXT, SQLITE_UTF8,
};

static INIT: Once = Once::new();

/// Register all custom UDFs as auto-extensions.
///
/// After calling this, every new SQLite connection in the process will
/// automatically have the UDFs available. Safe to call multiple times;
/// only the first call has any effect.
#[allow(unsafe_code)]
pub fn install_udfs() {
    INIT.call_once(|| {
        // SAFETY: sqlite3_auto_extension expects an extension entry point cast to
        // Option<unsafe extern "C" fn(db, errmsg, api) -> c_int>.
        // `udfs_init` matches this signature.
        unsafe {
            let rc = sqlite3_auto_extension(Some(udfs_init));
            assert_eq!(rc, SQLITE_OK, "failed to install SQLite UDF auto-extension");
        }
    });
}

/// Auto-extension entry point called by SQLite for each new connection.
#[allow(unsafe_code)]
unsafe extern "C" fn udfs_init(
    db: *mut sqlite3,
    _pz_err_msg: *mut *mut c_char,
    _p_thunk: *const sqlite3_api_routines,
) -> c_int {
    register_hex2int(db);
    SQLITE_OK
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
    let stripped = text
        .strip_prefix("0x")
        .or_else(|| text.strip_prefix("0X"))
        .unwrap_or(text);
    // Take the rightmost 16 hex chars (lower 64 bits). Hex strings may be up
    // to 256 bits; values that fit in u64 are preserved exactly, larger values
    // are truncated to their low 64 bits.
    let lower = stripped
        .get(stripped.len().saturating_sub(16)..)
        .unwrap_or(stripped);

    match u64::from_str_radix(lower, 16) {
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
        install_udfs();

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        // UDFs are automatically available — no per-connection registration needed
        let row = sqlx::query("SELECT hex2int('0xff') AS v")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("v"), 255);

        // Without prefix
        let row = sqlx::query("SELECT hex2int('ff') AS v")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("v"), 255);

        // Large value
        let row = sqlx::query("SELECT hex2int('0xffffffffffffffff') AS v")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("v"), -1); // u64::MAX as i64

        // NULL passthrough
        let row = sqlx::query("SELECT hex2int(NULL) AS v")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(row.try_get::<i64, _>("v").is_err() || row.get::<Option<i64>, _>("v").is_none());

        // Zero
        let row = sqlx::query("SELECT hex2int('0x0') AS v")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("v"), 0);

        // Uppercase prefix
        let row = sqlx::query("SELECT hex2int('0XAB') AS v")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("v"), 171);

        // 256-bit value: only the lower 64 bits are kept
        // 0x0000...0000_00000000000000ff → 255
        let row = sqlx::query(
            "SELECT hex2int('0x00000000000000000000000000000000000000000000000000000000000000ff') AS v",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.get::<i64, _>("v"), 255);

        // 256-bit value where high bits are non-zero but ignored
        // high 192 bits: 0xdead...; low 64 bits: 0x1234567890abcdef
        let row = sqlx::query(
            "SELECT hex2int('0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef1234567890abcdef') AS v",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.get::<i64, _>("v"), 0x1234567890abcdef_i64);
    }
}
