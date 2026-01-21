use crate::config::JobQueueSettings;
use crate::storage::VtxVideoRegistry;
use crate::vtx_vfs::VtxVfsManager;
use std::sync::Arc;
use tracing::{error, warn};

mod adaptive;
mod handlers;
mod worker;

use adaptive::{spawn_adaptive_controller, AdaptiveScanLimiter};
use worker::{run_once, spawn_worker, WorkerState, WorkerTick};

pub fn spawn_workers(
    registry: VtxVideoRegistry,
    vfs: Arc<VtxVfsManager>,
    settings: JobQueueSettings,
) {
    let workers = std::cmp::max(1, settings.max_concurrent) as usize;
    let adaptive_settings = settings.adaptive_scan.clone();
    let scan_limiter = if adaptive_settings.enabled {
        let max = std::cmp::max(
            adaptive_settings.min_concurrent,
            adaptive_settings.max_concurrent,
        ) as usize;
        Some(Arc::new(AdaptiveScanLimiter::new(max)))
    } else {
        None
    };

    if let Some(limiter) = scan_limiter.clone() {
        spawn_adaptive_controller(registry.clone(), adaptive_settings, limiter);
    }

    for idx in 0..workers {
        let worker_id = format!("worker-{}", idx + 1);
        spawn_worker(
            worker_id,
            registry.clone(),
            vfs.clone(),
            scan_limiter.clone(),
            settings.clone(),
        );
    }
}

pub async fn recover_startup(registry: VtxVideoRegistry, settings: JobQueueSettings) {
    let timeout_secs = settings.timeout_secs;
    let lease_reclaim = tokio::task::spawn_blocking({
        let registry = registry.clone();
        move || registry.requeue_expired_job_leases()
    })
    .await;
    match lease_reclaim {
        Ok(Ok(count)) => {
            if count > 0 {
                warn!("[Jobs] Startup requeued {} expired leases", count);
            }
        }
        Ok(Err(e)) => error!("[Jobs] Startup lease reclaim failed: {}", e),
        Err(join_err) => error!("[Jobs] Startup lease reclaim join error: {}", join_err),
    }

    let timeout_sweep =
        tokio::task::spawn_blocking(move || registry.fail_timed_out_jobs(timeout_secs)).await;
    match timeout_sweep {
        Ok(Ok(count)) => {
            if count > 0 {
                warn!("[Jobs] Startup marked {} timed-out jobs as failed", count);
            }
        }
        Ok(Err(e)) => error!("[Jobs] Startup timeout sweep failed: {}", e),
        Err(join_err) => error!("[Jobs] Startup timeout sweep join error: {}", join_err),
    }
}

#[doc(hidden)]
#[allow(dead_code)]
pub async fn run_worker_once_for_tests(
    worker_id: &str,
    registry: &VtxVideoRegistry,
    vfs: Arc<VtxVfsManager>,
    settings: &JobQueueSettings,
) -> bool {
    let mut state = WorkerState::new();
    run_once(&mut state, worker_id, registry, vfs, None, settings).await == WorkerTick::DidWork
}
