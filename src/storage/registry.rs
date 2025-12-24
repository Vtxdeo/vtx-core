use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rayon::prelude::*;
use rusqlite::{params, OptionalExtension};
use rusqlite_migration::{Migrations, M};
use serde::Serialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tracing::{error, info, warn};
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize)]
pub struct VideoMeta {
    pub id: String,
    pub filename: String,
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub full_path: PathBuf,
    pub created_at: String,
}

#[derive(Clone)]
pub struct VideoRegistry {
    pool: Pool<SqliteConnectionManager>,
}

impl VideoRegistry {
    /// 初始化视频注册中心并执行数据库迁移
    ///
    /// 副作用：
    /// - 如果数据库文件不存在，将创建文件。
    /// - 执行 SQL 迁移，修改数据库 Schema。
    /// - 设置 SQLite PRAGMA 参数（WAL 模式等）。
    pub fn new(db_path: &str) -> anyhow::Result<Self> {
        let manager = SqliteConnectionManager::file(db_path);
        let pool = Pool::new(manager)?;

        let mut conn = pool.get()?;

        let migrations = Migrations::new(vec![
            // 视频元数据表
            M::up(
                "CREATE TABLE IF NOT EXISTS videos (
                    id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    full_path TEXT NOT NULL UNIQUE
                );
                CREATE INDEX IF NOT EXISTS idx_full_path ON videos(full_path);",
            ),
            // 视频创建时间字段
            M::up("ALTER TABLE videos ADD COLUMN created_at TEXT DEFAULT '1970-01-01 00:00:00';"),
            // 插件版本记录表
            M::up(
                "CREATE TABLE IF NOT EXISTS sys_plugin_versions (
                    plugin_name TEXT PRIMARY KEY,
                    version INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );",
            ),
            // 插件资源账本
            M::up(
                "CREATE TABLE IF NOT EXISTS sys_plugin_resources (
                    plugin_name TEXT NOT NULL,
                    resource_type TEXT NOT NULL,
                    resource_name TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (plugin_name, resource_type, resource_name)
                );",
            ),
            // 插件安装锁定表（绑定插件ID与物理路径）
            M::up(
                "CREATE TABLE IF NOT EXISTS sys_plugin_installations (
                    plugin_id TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    installed_at TEXT DEFAULT CURRENT_TIMESTAMP
                );",
            ),
        ]);

        if let Err(e) = migrations.to_latest(&mut conn) {
            return Err(anyhow::anyhow!(
                "Failed to apply database migrations: {}",
                e
            ));
        }

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = 30000000000;
             PRAGMA foreign_keys = ON;",
        )?;

        // 健康检查
        match conn.execute("CREATE TEMPORARY TABLE health_check (id INTEGER)", []) {
            Ok(_) => {
                let _ = conn.execute("DROP TABLE health_check", []);
            }
            Err(e) => return Err(anyhow::anyhow!("Database health check failed: {}", e)),
        }

        info!("[Database] SQLite connection initialized at: {}", db_path);
        Ok(Self { pool })
    }

    /// 扫描指定目录中的视频文件，并将新发现的视频注册到数据库中。
    pub fn scan_directory(&self, dir_path: &str) -> Vec<VideoMeta> {
        let conn = self.pool.get().expect("Failed to acquire DB connection");

        // 校验并解析扫描根目录路径
        // 安全边界保护：禁止软链接跳出预期目录
        let root_path = match std::fs::canonicalize(dir_path) {
            Ok(p) => p,
            Err(e) => {
                error!("[scanner] failed to resolve scan root: {}", e);
                return vec![];
            }
        };

        info!("[scanner] start scanning directory: {:?}", root_path);

        // 预读取数据库中已存在的文件路径，用于内存判重
        let mut stmt = conn.prepare("SELECT full_path FROM videos").unwrap();
        let existing_paths: HashSet<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();

        // 使用并行遍历扫描目录，构造新的视频元数据集合
        let new_videos: Vec<VideoMeta> = WalkDir::new(dir_path)
            .into_iter()
            .par_bridge()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_file())
            .filter_map(|entry| {
                let path = entry.path();

                // 快速过滤：仅处理视频文件扩展名
                let ext = path.extension()?.to_string_lossy().to_lowercase();
                if !["mp4", "mkv", "mov", "avi", "webm"].contains(&ext.as_str()) {
                    return None;
                }

                // 获取绝对路径，用于软链接安全检查
                let real_path = std::fs::canonicalize(path).ok()?;
                let full_path_str = real_path.to_string_lossy().to_string();

                // 拒绝符号链接逃逸出根路径范围
                if !real_path.starts_with(&root_path) {
                    warn!("[scanner] symlink outside root skipped: {:?}", real_path);
                    return None;
                }

                // 判重：若已存在于数据库中则跳过
                if existing_paths.contains(&full_path_str) {
                    return None;
                }

                Some(VideoMeta {
                    id: Uuid::new_v4().to_string(),
                    filename: path.file_name()?.to_string_lossy().to_string(),
                    full_path: real_path,
                    created_at: "Just Now".to_string(),
                })
            })
            .collect();

        // 将新视频批量写入数据库（事务提交以优化性能）
        if !new_videos.is_empty() {
            let mut conn = self.pool.get().unwrap();
            let tx = conn.transaction().unwrap();

            {
                let mut stmt = tx
                    .prepare(
                        "INSERT OR IGNORE INTO videos (id, filename, full_path, created_at)
                     VALUES (?1, ?2, ?3, datetime('now', 'localtime'))",
                    )
                    .unwrap();

                for video in &new_videos {
                    let path_str = video.full_path.to_string_lossy().to_string();
                    if let Err(e) = stmt.execute(params![&video.id, &video.filename, &path_str]) {
                        error!(
                            "[scanner] failed to insert video: {} ({})",
                            video.filename, e
                        );
                    }
                }
            }

            if let Err(e) = tx.commit() {
                error!("[scanner] database transaction commit failed: {}", e);
            } else {
                info!(
                    "[scanner] scan completed: {} new videos registered",
                    new_videos.len()
                );
            }
        }

        new_videos
    }

    /// 列出数据库中所有视频资源
    pub fn list_all(&self) -> Vec<VideoMeta> {
        let conn = self.pool.get().expect("Failed to acquire DB connection");
        let mut stmt = conn
            .prepare(
                "SELECT id, filename, full_path, created_at FROM videos ORDER BY created_at DESC",
            )
            .unwrap();

        let video_iter = stmt
            .query_map([], |row| {
                Ok(VideoMeta {
                    id: row.get(0)?,
                    filename: row.get(1)?,
                    full_path: PathBuf::from(row.get::<_, String>(2)?),
                    created_at: row.get::<_, String>(3)?,
                })
            })
            .unwrap();

        video_iter.filter_map(Result::ok).collect()
    }

    /// 通过 ID 查询视频的实际文件路径
    pub fn get_path(&self, id: &str) -> Option<PathBuf> {
        let conn = self.pool.get().ok()?;
        let mut stmt = conn
            .prepare_cached("SELECT full_path FROM videos WHERE id = ?1")
            .ok()?;
        let path_str: String = stmt.query_row(params![id], |row| row.get(0)).ok()?;
        let path = PathBuf::from(path_str);

        if path.exists() {
            Some(path)
        } else {
            warn!("[Database] File not found on disk: {:?}", path);
            None
        }
    }

    #[allow(dead_code)]
    pub fn get_conn(&self) -> anyhow::Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        Ok(self.pool.get()?)
    }

    /// 查询插件当前版本号（默认为 0）
    pub fn get_plugin_version(&self, plugin_name: &str) -> usize {
        let conn = self.pool.get().unwrap();
        conn.query_row(
            "SELECT version FROM sys_plugin_versions WHERE plugin_name = ?1",
            [plugin_name],
            |row| row.get(0),
        )
        .unwrap_or(0)
    }

    /// 设置插件当前版本号
    ///
    /// 幂等性：该操作具备幂等性（Upsert）。
    pub fn set_plugin_version(&self, plugin_name: &str, new_version: usize) {
        let conn = self.pool.get().unwrap();
        conn.execute(
            "INSERT INTO sys_plugin_versions (plugin_name, version)
             VALUES (?1, ?2)
             ON CONFLICT(plugin_name) DO UPDATE
             SET version = ?2, updated_at = CURRENT_TIMESTAMP",
            params![plugin_name, new_version],
        )
        .unwrap();
    }

    /// 插件资源注册
    pub fn register_resource(&self, plugin_name: &str, res_type: &str, res_name: &str) {
        let conn = self.pool.get().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO sys_plugin_resources (plugin_name, resource_type, resource_name)
             VALUES (?1, ?2, ?3)",
            params![plugin_name, res_type, res_name],
        )
        .ok();
    }

    /// 安装验证与路径绑定机制
    ///
    /// 确保同一个 Plugin ID 不会被绑定到不同的物理路径上，防止数据错乱。
    pub fn verify_installation(
        &self,
        plugin_id: &str,
        current_path: &Path,
    ) -> anyhow::Result<bool> {
        let conn = self.pool.get()?;

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
                        "[Install] Installation ID conflict. ID '{}' is registered at '{}', but attempted path is '{}'",
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
                    "[Install] Plugin '{}' locked to '{}'",
                    plugin_id, abs_current_str
                );
                Ok(true)
            }
        }
    }

    /// 卸载插件时释放锁定记录
    pub fn release_installation(&self, plugin_id: &str) -> anyhow::Result<()> {
        let conn = self.pool.get()?;
        conn.execute(
            "DELETE FROM sys_plugin_installations WHERE plugin_id = ?1",
            [plugin_id],
        )?;
        Ok(())
    }

    /// 卸载插件并清除所有相关资源表与记录
    ///
    /// 副作用：执行 DROP TABLE 操作，数据不可恢复。
    pub fn nuke_plugin(&self, plugin_name: &str) -> anyhow::Result<usize> {
        let conn = self.pool.get()?;

        let mut stmt = conn.prepare(
            "SELECT resource_name FROM sys_plugin_resources WHERE plugin_name = ?1 AND resource_type = 'TABLE'"
        )?;
        let tables: Vec<String> = stmt
            .query_map([plugin_name], |row| row.get(0))?
            .filter_map(Result::ok)
            .collect();

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
}
