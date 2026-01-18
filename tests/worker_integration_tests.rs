use tempfile::tempdir;
use vtx_core::config::{AdaptiveScanSettings, JobQueueSettings};
use vtx_core::runtime::jobs::run_worker_once_for_tests;
use vtx_core::storage::VideoRegistry;
use vtx_core::vtx_vfs::VfsManager;

fn test_settings() -> JobQueueSettings {
    JobQueueSettings {
        poll_interval_ms: 10,
        max_concurrent: 1,
        timeout_secs: 30,
        sweep_interval_ms: 60_000,
        lease_secs: 5,
        reclaim_interval_ms: 60_000,
        adaptive_scan: AdaptiveScanSettings::default(),
    }
}

#[tokio::test]
async fn worker_processes_noop_job() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");
    let vfs = VfsManager::new().expect("vfs");

    let job_id = registry.enqueue_job("noop", "{}", 1, 0).expect("enqueue");

    let did_work = run_worker_once_for_tests(
        "worker-1",
        &registry,
        std::sync::Arc::new(vfs),
        &test_settings(),
    )
    .await;
    assert!(did_work);

    let job = registry.get_job(&job_id).expect("get job").expect("job");
    assert_eq!(job.status, "succeeded");
}

#[tokio::test]
async fn worker_fails_unsupported_job_type() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");
    let vfs = VfsManager::new().expect("vfs");

    let job_id = registry.enqueue_job("bogus", "{}", 1, 0).expect("enqueue");

    let did_work = run_worker_once_for_tests(
        "worker-1",
        &registry,
        std::sync::Arc::new(vfs),
        &test_settings(),
    )
    .await;
    assert!(did_work);

    let job = registry.get_job(&job_id).expect("get job").expect("job");
    assert_eq!(job.status, "failed");
    assert!(job.error.unwrap_or_default().contains("unsupported"));
}
