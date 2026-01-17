pub mod database;
pub mod jobs;
pub mod plugins;
pub mod scan_roots;
pub mod videos;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;

/// 视频元数据结�?
#[derive(Debug, Clone, Serialize)]
pub struct VideoMeta {
    /// 视频唯一标识�?
    pub id: String,
    /// 文件名（不含路径�?
    pub filename: String,
    /// 原始完整路径（用于内部处理，不序列化�?
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub source_uri: String,
    /// 创建时间（格式化字符串）
    pub created_at: String,
}

/// 注册中心：统一管理数据库访问，供插件与视频模块使用
#[derive(Clone)]
pub struct VideoRegistry {
    /// SQLite 连接池（共享给子模块�?
    pub(crate) pool: Pool<SqliteConnectionManager>,
}

impl VideoRegistry {
    /// 创建注册中心并初始化数据库连接池
    ///
    /// # Parameters
    /// - `db_path`: 数据库文件路�?
    /// - `max_connections`: 最大连接数
    pub fn new(db_path: &str, max_connections: u32) -> anyhow::Result<Self> {
        let pool = database::initialize_pool(db_path, max_connections)?;
        Ok(Self { pool })
    }

    // ===============================
    // 视频资源管理相关（代理调用）
    // ===============================

    /// 扫描指定目录中的视频文件并写入数据库
    pub async fn scan_directory(
        &self,
        vfs: &crate::vfs::VfsManager,
        root_uri: &str,
    ) -> anyhow::Result<Vec<VideoMeta>> {
        videos::scan_directory(&self.pool, vfs, root_uri).await
    }

    pub(crate) async fn scan_directory_with_abort<F>(
        &self,
        vfs: &crate::vfs::VfsManager,
        root_uri: &str,
        should_continue: F,
    ) -> anyhow::Result<videos::ScanOutcome>
    where
        F: Fn() -> Result<(), videos::ScanAbort> + Send + Sync,
    {
        videos::scan_directory_with_abort(&self.pool, vfs, root_uri, should_continue).await
    }

    /// 查询数据库中所有视频元数据
    pub fn list_all(&self) -> anyhow::Result<Vec<VideoMeta>> {
        videos::list_all(&self.pool)
    }

    /// 获取指定视频的实际文件路�?
    pub fn get_uri(&self, id: &str) -> Option<String> {
        videos::get_uri(&self.pool, id)
    }

    // ===============================
    // 插件元数据管理相关（代理调用�?
    // ===============================

    /// 查询插件当前版本�?
    pub fn get_plugin_version(&self, plugin_name: &str) -> usize {
        plugins::get_plugin_version(&self.pool, plugin_name)
    }

    /// 更新插件版本�?
    pub fn set_plugin_version(&self, plugin_name: &str, new_version: usize) {
        plugins::set_plugin_version(&self.pool, plugin_name, new_version)
    }

    /// 向数据库注册插件所需的资源项（如表名、模型等�?
    pub fn register_resource(&self, plugin_name: &str, res_type: &str, res_name: &str) {
        plugins::register_resource(&self.pool, plugin_name, res_type, res_name)
    }

    /// 获取插件已注册的资源列表
    pub fn list_plugin_resources(
        &self,
        plugin_name: &str,
        res_type: &str,
    ) -> anyhow::Result<Vec<String>> {
        plugins::list_resources(&self.pool, plugin_name, res_type)
    }

    /// 验证插件是否已安装，并尝试锁定安装路�?
    pub fn verify_installation(
        &self,
        plugin_id: &str,
        current_uri: &str,
    ) -> anyhow::Result<bool> {
        plugins::verify_installation(&self.pool, plugin_id, current_uri)
    }

    /// 释放插件的安装锁�?
    pub fn release_installation(&self, plugin_id: &str) -> anyhow::Result<()> {
        plugins::release_installation(&self.pool, plugin_id)
    }

    pub fn set_plugin_metadata(
        &self,
        plugin_id: &str,
        meta: &crate::runtime::manager::VtxPackageMetadata,
    ) -> anyhow::Result<()> {
        plugins::set_plugin_metadata(&self.pool, plugin_id, meta)
    }

    /// 从系统中彻底移除插件记录
    pub fn nuke_plugin(&self, plugin_name: &str) -> anyhow::Result<usize> {
        plugins::nuke_plugin(&self.pool, plugin_name)
    }

    pub fn list_scan_roots(&self) -> anyhow::Result<Vec<String>> {
        scan_roots::list_scan_roots(&self.pool)
    }

    pub fn add_scan_root(&self, uri: &str) -> anyhow::Result<String> {
        scan_roots::add_scan_root(&self.pool, uri)
    }

    pub fn remove_scan_root(&self, uri: &str) -> anyhow::Result<String> {
        scan_roots::remove_scan_root(&self.pool, uri)
    }

    // ===============================
    // Job queue (persistent)
    // ===============================

    pub fn enqueue_job(
        &self,
        job_type: &str,
        payload: &str,
        payload_version: i64,
        max_retries: i64,
    ) -> anyhow::Result<String> {
        jobs::enqueue_job(&self.pool, job_type, payload, payload_version, max_retries)
    }

    pub fn get_job(&self, job_id: &str) -> anyhow::Result<Option<jobs::JobRecord>> {
        jobs::get_job(&self.pool, job_id)
    }

    pub fn get_job_status(&self, job_id: &str) -> anyhow::Result<Option<String>> {
        jobs::get_job_status(&self.pool, job_id)
    }

    pub fn set_job_error(&self, job_id: &str, error: &str) -> anyhow::Result<()> {
        jobs::set_job_error(&self.pool, job_id, error)
    }

    pub fn set_job_result(&self, job_id: &str, result: &str) -> anyhow::Result<()> {
        jobs::set_job_result(&self.pool, job_id, result)
    }

    pub fn set_job_status_terminal(&self, job_id: &str, status: &str) -> anyhow::Result<()> {
        jobs::set_job_status_terminal(&self.pool, job_id, status)
    }

    pub fn list_recent_jobs(&self, limit: i64) -> anyhow::Result<Vec<jobs::JobRecord>> {
        jobs::list_recent_jobs(&self.pool, limit)
    }

    pub fn claim_next_job(
        &self,
        worker_id: &str,
        lease_secs: u64,
    ) -> anyhow::Result<Option<jobs::JobRecord>> {
        jobs::claim_next_job(&self.pool, worker_id, lease_secs)
    }

    pub fn update_job_progress(&self, job_id: &str, progress: i64) -> anyhow::Result<()> {
        jobs::update_progress(&self.pool, job_id, progress)
    }

    pub fn complete_job(&self, job_id: &str, result: &str) -> anyhow::Result<()> {
        jobs::complete_job(&self.pool, job_id, result)
    }

    pub fn fail_job(&self, job_id: &str, error: &str) -> anyhow::Result<()> {
        jobs::fail_job(&self.pool, job_id, error)
    }

    pub fn retry_job(&self, job_id: &str, error: &str) -> anyhow::Result<()> {
        jobs::retry_job(&self.pool, job_id, error)
    }

    pub fn increment_job_retries(&self, job_id: &str) -> anyhow::Result<()> {
        jobs::increment_retries(&self.pool, job_id)
    }

    pub fn cancel_job(&self, job_id: &str) -> anyhow::Result<usize> {
        jobs::cancel_job(&self.pool, job_id)
    }

    pub fn fail_timed_out_jobs(&self, timeout_secs: u64) -> anyhow::Result<usize> {
        jobs::fail_timed_out_jobs(&self.pool, timeout_secs)
    }

    pub fn renew_job_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        lease_secs: u64,
    ) -> anyhow::Result<()> {
        jobs::renew_lease(&self.pool, job_id, worker_id, lease_secs)
    }

    pub fn requeue_expired_job_leases(&self) -> anyhow::Result<usize> {
        jobs::requeue_expired_leases(&self.pool)
    }

    pub fn count_jobs_by_type_and_status(
        &self,
        job_type: &str,
        status: &str,
    ) -> anyhow::Result<usize> {
        jobs::count_jobs_by_type_and_status(&self.pool, job_type, status)
    }
    // ===============================
    // 高级控制（不推荐常规使用�?
    // ===============================

    /// 获取底层数据库连接（用于自定义事务或原始访问�?
    #[allow(dead_code)]
    pub fn get_conn(&self) -> anyhow::Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        Ok(self.pool.get()?)
    }
}
