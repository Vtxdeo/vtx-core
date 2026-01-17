use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use tracing::warn;

pub(crate) fn list_scan_roots(
    pool: &Pool<SqliteConnectionManager>,
) -> anyhow::Result<Vec<String>> {
    let conn = pool.get()?;
    let mut stmt = conn.prepare("SELECT path FROM sys_scan_roots ORDER BY path")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    Ok(rows.filter_map(Result::ok).collect())
}

pub(crate) fn add_scan_root(
    pool: &Pool<SqliteConnectionManager>,
    uri: &str,
) -> anyhow::Result<String> {
    let conn = pool.get()?;
    conn.execute(
        "INSERT OR IGNORE INTO sys_scan_roots (path) VALUES (?1)",
        params![uri],
    )?;

    Ok(uri.to_string())
}

pub(crate) fn remove_scan_root(
    pool: &Pool<SqliteConnectionManager>,
    uri: &str,
) -> anyhow::Result<String> {
    let conn = pool.get()?;
    let affected = conn.execute(
        "DELETE FROM sys_scan_roots WHERE path = ?1",
        params![uri],
    )?;
    if affected == 0 {
        warn!("[Admin] Scan root not found: {}", uri);
    }
    Ok(uri.to_string())
}
