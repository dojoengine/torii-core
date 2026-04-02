use std::path::Path;

use rusqlite::{Connection, OpenFlags, Result as RusqliteResult};

pub fn connect<P: AsRef<Path>>(path: P) -> RusqliteResult<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
}
