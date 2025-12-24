pub mod database;
pub mod plugins;
pub mod videos;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;
use std::path::{Path, PathBuf};

/// 视频元数据结构
#[derive(Debug, Clone, Serialize)]
pub struct VideoMeta {
    /// 视频唯一标识符
    pub id: String,
    /// 文件名（不含路径）
    pub filename: String,
    /// 原始完整路径（用于内部处理，不序列化）
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub full_path: PathBuf,
    /// 创建时间（格式化字符串）
    pub created_at: String,
}

/// 注册中心：统一管理数据库访问，供插件与视频模块使用
#[derive(Clone)]
pub struct VideoRegistry {
    /// SQLite 连接池（共享给子模块）
    pub(crate) pool: Pool<SqliteConnectionManager>,
}

impl VideoRegistry {
    /// 创建注册中心并初始化数据库连接池
    ///
    /// # Parameters
    /// - `db_path`: 数据库文件路径
    /// - `max_connections`: 最大连接数
    pub fn new(db_path: &str, max_connections: u32) -> anyhow::Result<Self> {
        let pool = database::initialize_pool(db_path, max_connections)?;
        Ok(Self { pool })
    }

    // ===============================
    // 视频资源管理相关（代理调用）
    // ===============================

    /// 扫描指定目录中的视频文件并写入数据库
    pub fn scan_directory(&self, dir_path: &str) -> anyhow::Result<Vec<VideoMeta>> {
        videos::scan_directory(&self.pool, dir_path)
    }

    /// 查询数据库中所有视频元数据
    pub fn list_all(&self) -> anyhow::Result<Vec<VideoMeta>> {
        videos::list_all(&self.pool)
    }

    /// 获取指定视频的实际文件路径
    pub fn get_path(&self, id: &str) -> Option<PathBuf> {
        videos::get_path(&self.pool, id)
    }

    // ===============================
    // 插件元数据管理相关（代理调用）
    // ===============================

    /// 查询插件当前版本号
    pub fn get_plugin_version(&self, plugin_name: &str) -> usize {
        plugins::get_plugin_version(&self.pool, plugin_name)
    }

    /// 更新插件版本号
    pub fn set_plugin_version(&self, plugin_name: &str, new_version: usize) {
        plugins::set_plugin_version(&self.pool, plugin_name, new_version)
    }

    /// 向数据库注册插件所需的资源项（如表名、模型等）
    pub fn register_resource(&self, plugin_name: &str, res_type: &str, res_name: &str) {
        plugins::register_resource(&self.pool, plugin_name, res_type, res_name)
    }

    /// 验证插件是否已安装，并尝试锁定安装路径
    pub fn verify_installation(
        &self,
        plugin_id: &str,
        current_path: &Path,
    ) -> anyhow::Result<bool> {
        plugins::verify_installation(&self.pool, plugin_id, current_path)
    }

    /// 释放插件的安装锁定
    pub fn release_installation(&self, plugin_id: &str) -> anyhow::Result<()> {
        plugins::release_installation(&self.pool, plugin_id)
    }

    /// 从系统中彻底移除插件记录
    pub fn nuke_plugin(&self, plugin_name: &str) -> anyhow::Result<usize> {
        plugins::nuke_plugin(&self.pool, plugin_name)
    }

    // ===============================
    // 高级控制（不推荐常规使用）
    // ===============================

    /// 获取底层数据库连接（用于自定义事务或原始访问）
    #[allow(dead_code)]
    pub fn get_conn(&self) -> anyhow::Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        Ok(self.pool.get()?)
    }
}
