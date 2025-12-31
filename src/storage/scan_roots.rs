use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use std::path::PathBuf;
use tracing::warn;

pub(crate) fn list_scan_roots(
    pool: &Pool<SqliteConnectionManager>,
) -> anyhow::Result<Vec<PathBuf>> {
    let conn = pool.get()?;
    let mut stmt = conn.prepare("SELECT path FROM sys_scan_roots ORDER BY path")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    Ok(rows
        .filter_map(Result::ok)
        .map(PathBuf::from)
        .collect())
}

pub(crate) fn add_scan_root(
    pool: &Pool<SqliteConnectionManager>,
    path: &PathBuf,
) -> anyhow::Result<PathBuf> {
    let resolved = std::fs::canonicalize(path)?;
    if !resolved.is_dir() {
        return Err(anyhow::anyhow!("Scan root must be a directory"));
    }

    let conn = pool.get()?;
    conn.execute(
        "INSERT OR IGNORE INTO sys_scan_roots (path) VALUES (?1)",
        params![resolved.to_string_lossy()],
    )?;

    Ok(resolved)
}

pub(crate) fn remove_scan_root(
    pool: &Pool<SqliteConnectionManager>,
    path: &PathBuf,
) -> anyhow::Result<PathBuf> {
    let resolved = std::fs::canonicalize(path)?;
    let conn = pool.get()?;
    let affected = conn.execute(
        "DELETE FROM sys_scan_roots WHERE path = ?1",
        params![resolved.to_string_lossy()],
    )?;
    if affected == 0 {
        warn!("[Admin] Scan root not found: {:?}", resolved);
    }
    Ok(resolved)
}
