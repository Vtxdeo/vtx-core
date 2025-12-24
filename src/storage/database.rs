use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite_migration::{Migrations, M};
use tracing::info;

/// 初始化数据库连接池并执行迁移
pub(crate) fn initialize_pool(
    db_path: &str,
    max_connections: u32,
) -> anyhow::Result<Pool<SqliteConnectionManager>> {
    let manager = SqliteConnectionManager::file(db_path);

    // 显式配置连接池，防止高并发下资源耗尽
    let pool = Pool::builder()
        .max_size(max_connections)
        .connection_timeout(std::time::Duration::from_secs(5))
        .build(manager)
        .map_err(|e| anyhow::anyhow!("Failed to initialize DB pool: {}", e))?;

    let mut conn = pool
        .get()
        .map_err(|e| anyhow::anyhow!("Failed to acquire init connection: {}", e))?;

    // 定义迁移脚本
    let migrations = Migrations::new(vec![
        // M1: 视频元数据表
        M::up(
            "CREATE TABLE IF NOT EXISTS videos (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                full_path TEXT NOT NULL UNIQUE
            );
            CREATE INDEX IF NOT EXISTS idx_full_path ON videos(full_path);",
        ),
        // M2: 视频创建时间字段
        M::up("ALTER TABLE videos ADD COLUMN created_at TEXT DEFAULT '1970-01-01 00:00:00';"),
        // M3: 插件版本记录表
        M::up(
            "CREATE TABLE IF NOT EXISTS sys_plugin_versions (
                plugin_name TEXT PRIMARY KEY,
                version INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );",
        ),
        // M4: 插件资源账本
        M::up(
            "CREATE TABLE IF NOT EXISTS sys_plugin_resources (
                plugin_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                resource_name TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (plugin_name, resource_type, resource_name)
            );",
        ),
        // M5: 插件安装锁定表
        M::up(
            "CREATE TABLE IF NOT EXISTS sys_plugin_installations (
                plugin_id TEXT PRIMARY KEY,
                file_path TEXT NOT NULL,
                installed_at TEXT DEFAULT CURRENT_TIMESTAMP
            );",
        ),
    ]);

    if let Err(e) = migrations.to_latest(&mut conn) {
        return Err(anyhow::anyhow!("Failed to apply database migrations: {}", e));
    }

    // 优化 SQLite 性能参数
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;
         PRAGMA mmap_size = 30000000000;
         PRAGMA foreign_keys = ON;",
    )?;

    // 执行健康检查
    match conn.execute("CREATE TEMPORARY TABLE health_check (id INTEGER)", []) {
        Ok(_) => {
            let _ = conn.execute("DROP TABLE health_check", []);
        }
        Err(e) => return Err(anyhow::anyhow!("Database health check failed: {}", e)),
    }

    info!(
        "[Database] SQLite connection initialized at: {} (Pool size: {})",
        db_path, max_connections
    );
    Ok(pool)
}
