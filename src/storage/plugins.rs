use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OptionalExtension};
use std::path::Path;
use tracing::{error, info, warn};

/// 获取插件版本
pub(crate) fn get_plugin_version(pool: &Pool<SqliteConnectionManager>, plugin_name: &str) -> usize {
    let Ok(conn) = pool.get() else { return 0 };
    conn.query_row(
        "SELECT version FROM sys_plugin_versions WHERE plugin_name = ?1",
        [plugin_name],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

/// 更新插件版本 (幂等)
pub(crate) fn set_plugin_version(
    pool: &Pool<SqliteConnectionManager>,
    plugin_name: &str,
    new_version: usize,
) {
    if let Ok(conn) = pool.get() {
        let _ = conn
            .execute(
                "INSERT INTO sys_plugin_versions (plugin_name, version)
             VALUES (?1, ?2)
             ON CONFLICT(plugin_name) DO UPDATE
             SET version = ?2, updated_at = CURRENT_TIMESTAMP",
                params![plugin_name, new_version],
            )
            .map_err(|e| error!("[Database] Failed to set plugin version: {}", e));
    }
}

/// 注册插件资源
pub(crate) fn register_resource(
    pool: &Pool<SqliteConnectionManager>,
    plugin_name: &str,
    res_type: &str,
    res_name: &str,
) {
    if let Ok(conn) = pool.get() {
        let _ = conn.execute(
            "INSERT OR IGNORE INTO sys_plugin_resources (plugin_name, resource_type, resource_name)
             VALUES (?1, ?2, ?3)",
            params![plugin_name, res_type, res_name],
        );
    }
}

/// 鑾峰彇鎻掍欢璧勬簮鍒楄〃
pub(crate) fn list_resources(
    pool: &Pool<SqliteConnectionManager>,
    plugin_name: &str,
    res_type: &str,
) -> anyhow::Result<Vec<String>> {
    let conn = pool.get()?;
    let mut stmt = conn.prepare(
        "SELECT resource_name FROM sys_plugin_resources WHERE plugin_name = ?1 AND resource_type = ?2",
    )?;
    let rows = stmt.query_map(params![plugin_name, res_type], |row| row.get(0))?;
    Ok(rows.filter_map(Result::ok).collect())
}

/// 验证并锁定安装路径
///
/// 职责：确保同一插件 ID 不会被安装到不同路径。
pub(crate) fn verify_installation(
    pool: &Pool<SqliteConnectionManager>,
    plugin_id: &str,
    current_path: &Path,
) -> anyhow::Result<bool> {
    let conn = pool.get()?;
    let abs_current = std::fs::canonicalize(current_path)?;
    let abs_current_str = abs_current.to_string_lossy().to_string();

    let mut stmt =
        conn.prepare("SELECT file_path FROM sys_plugin_installations WHERE plugin_id = ?1")?;
    let result: Option<String> = stmt.query_row([plugin_id], |row| row.get(0)).optional()?;

    match result {
        Some(registered_path) => {
            if registered_path == abs_current_str {
                Ok(true)
            } else {
                warn!(
                    "[Install] ID Conflict: '{}' registered at '{}', attempted '{}'",
                    plugin_id, registered_path, abs_current_str
                );
                Ok(false)
            }
        }
        None => {
            conn.execute(
                "INSERT INTO sys_plugin_installations (plugin_id, file_path) VALUES (?1, ?2)",
                params![plugin_id, abs_current_str],
            )?;
            info!(
                "[Install] Locked plugin '{}' to '{}'",
                plugin_id, abs_current_str
            );
            Ok(true)
        }
    }
}

/// 释放安装锁
pub(crate) fn release_installation(
    pool: &Pool<SqliteConnectionManager>,
    plugin_id: &str,
) -> anyhow::Result<()> {
    let conn = pool.get()?;
    conn.execute(
        "DELETE FROM sys_plugin_installations WHERE plugin_id = ?1",
        [plugin_id],
    )?;
    Ok(())
}

/// 彻底清理插件数据
pub(crate) fn nuke_plugin(
    pool: &Pool<SqliteConnectionManager>,
    plugin_name: &str,
) -> anyhow::Result<usize> {
    let conn = pool.get()?;

    let mut stmt = conn.prepare(
        "SELECT resource_name FROM sys_plugin_resources WHERE plugin_name = ?1 AND resource_type = 'TABLE'"
    )?;
    let tables: Vec<String> = stmt
        .query_map([plugin_name], |row| row.get(0))?
        .filter_map(Result::ok)
        .collect();
    drop(stmt);

    for table in tables {
        warn!("[Uninstall] Dropping table: {}", table);
        if table.chars().all(|c| c.is_alphanumeric() || c == '_') {
            conn.execute(&format!("DROP TABLE IF EXISTS {}", table), [])?;
        }
    }

    conn.execute(
        "DELETE FROM sys_plugin_resources WHERE plugin_name = ?1",
        [plugin_name],
    )?;
    conn.execute(
        "DELETE FROM sys_plugin_versions WHERE plugin_name = ?1",
        [plugin_name],
    )?;

    Ok(1)
}
