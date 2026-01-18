pub mod database;
pub mod jobs;
pub mod plugins;
pub mod scan_roots;
pub mod videos;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct VideoMeta {
    pub id: String,

    pub filename: String,

    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub source_uri: String,

    pub created_at: String,
}

#[derive(Clone)]
pub struct VideoRegistry {
    pub(crate) pool: Pool<SqliteConnectionManager>,
}

impl VideoRegistry {
    pub fn new(db_path: &str, max_connections: u32) -> anyhow::Result<Self> {
        let pool = database::initialize_pool(db_path, max_connections)?;
        Ok(Self { pool })
    }

    pub async fn scan_directory(
        &self,
        vfs: &crate::vtx_vfs::VfsManager,
        root_uri: &str,
    ) -> anyhow::Result<Vec<VideoMeta>> {
        videos::scan_directory(&self.pool, vfs, root_uri).await
    }

    pub(crate) async fn scan_directory_with_abort<F>(
        &self,
        vfs: &crate::vtx_vfs::VfsManager,
        root_uri: &str,
        should_continue: F,
    ) -> anyhow::Result<videos::ScanOutcome>
    where
        F: Fn() -> Result<(), videos::ScanAbort> + Send + Sync,
    {
        videos::scan_directory_with_abort(&self.pool, vfs, root_uri, should_continue).await
    }

    pub fn list_all(&self) -> anyhow::Result<Vec<VideoMeta>> {
        videos::list_all(&self.pool)
    }

    pub fn get_uri(&self, id: &str) -> Option<String> {
        videos::get_uri(&self.pool, id)
    }

    pub fn get_plugin_version(&self, plugin_name: &str) -> usize {
        plugins::get_plugin_version(&self.pool, plugin_name)
    }

    pub fn set_plugin_version(&self, plugin_name: &str, new_version: usize) {
        plugins::set_plugin_version(&self.pool, plugin_name, new_version)
    }

    pub fn register_resource(&self, plugin_name: &str, res_type: &str, res_name: &str) {
        plugins::register_resource(&self.pool, plugin_name, res_type, res_name)
    }

    pub fn list_plugin_resources(
        &self,
        plugin_name: &str,
        res_type: &str,
    ) -> anyhow::Result<Vec<String>> {
        plugins::list_resources(&self.pool, plugin_name, res_type)
    }

    pub fn verify_installation(&self, plugin_id: &str, current_uri: &str) -> anyhow::Result<bool> {
        plugins::verify_installation(&self.pool, plugin_id, current_uri)
    }

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

    #[allow(dead_code)]
    pub fn get_conn(&self) -> anyhow::Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        Ok(self.pool.get()?)
    }
}
