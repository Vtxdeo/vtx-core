use crate::config::JobQueueSettings;
use crate::storage::jobs::JobRecord;
use crate::storage::VideoRegistry;
use crate::vtx_vfs::VtxVfsManager;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::time::{sleep, Instant};
use tracing::{error, warn};

use super::adaptive::AdaptiveScanLimiter;
use super::handlers::handle_job;

pub(crate) struct WorkerState {
    last_sweep: Instant,
    last_reclaim: Instant,
}

impl WorkerState {
    pub(crate) fn new() -> Self {
        Self {
            last_sweep: Instant::now(),
            last_reclaim: Instant::now(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WorkerTick {
    DidWork,
    Idle,
}

pub(crate) fn spawn_worker(
    worker_id: String,
    registry: VideoRegistry,
    vfs: Arc<VtxVfsManager>,
    scan_limiter: Option<Arc<AdaptiveScanLimiter>>,
    settings: JobQueueSettings,
) {
    tokio::spawn(async move {
        let mut state = WorkerState::new();
        loop {
            let tick = run_once(
                &mut state,
                &worker_id,
                &registry,
                vfs.clone(),
                scan_limiter.clone(),
                &settings,
            )
            .await;

            if tick == WorkerTick::Idle {
                sleep(Duration::from_millis(settings.poll_interval_ms)).await;
            }
        }
    });
}

pub(crate) async fn run_once(
    state: &mut WorkerState,
    worker_id: &str,
    registry: &VideoRegistry,
    vfs: Arc<VtxVfsManager>,
    scan_limiter: Option<Arc<AdaptiveScanLimiter>>,
    settings: &JobQueueSettings,
) -> WorkerTick {
    let lease_secs = settings.lease_secs;
    maybe_sweep(
        state,
        registry,
        settings.timeout_secs,
        settings.sweep_interval_ms,
    )
    .await;
    maybe_reclaim(state, registry, settings.reclaim_interval_ms).await;

    let claim_result = tokio::task::spawn_blocking({
        let registry = registry.clone();
        let worker_id = worker_id.to_string();
        move || registry.claim_next_job(&worker_id, lease_secs)
    })
    .await;

    let job = match claim_result {
        Ok(Ok(job)) => job,
        Ok(Err(e)) => {
            error!("[Jobs] Claim failed: {}", e);
            return WorkerTick::Idle;
        }
        Err(join_err) => {
            error!("[Jobs] Claim join error: {}", join_err);
            return WorkerTick::Idle;
        }
    };

    let Some(job) = job else {
        return WorkerTick::Idle;
    };

    process_job(
        job,
        worker_id.to_string(),
        registry.clone(),
        vfs,
        scan_limiter,
        settings.lease_secs,
        settings.timeout_secs,
    )
    .await;

    WorkerTick::DidWork
}

async fn maybe_sweep(
    state: &mut WorkerState,
    registry: &VideoRegistry,
    timeout_secs: u64,
    sweep_interval_ms: u64,
) {
    let sweep_interval = Duration::from_millis(sweep_interval_ms);
    if state.last_sweep.elapsed() < sweep_interval {
        return;
    }
    let registry = registry.clone();
    let sweep_result =
        tokio::task::spawn_blocking(move || registry.fail_timed_out_jobs(timeout_secs)).await;
    match sweep_result {
        Ok(Ok(count)) => {
            if count > 0 {
                warn!("[Jobs] Marked {} timed-out jobs as failed", count);
            }
        }
        Ok(Err(e)) => error!("[Jobs] Timeout sweep failed: {}", e),
        Err(join_err) => error!("[Jobs] Timeout sweep join error: {}", join_err),
    }
    state.last_sweep = Instant::now();
}

async fn maybe_reclaim(
    state: &mut WorkerState,
    registry: &VideoRegistry,
    reclaim_interval_ms: u64,
) {
    let reclaim_interval = Duration::from_millis(reclaim_interval_ms);
    if state.last_reclaim.elapsed() < reclaim_interval {
        return;
    }
    let registry = registry.clone();
    let reclaim_result =
        tokio::task::spawn_blocking(move || registry.requeue_expired_job_leases()).await;
    match reclaim_result {
        Ok(Ok(count)) => {
            if count > 0 {
                warn!("[Jobs] Requeued {} expired leases", count);
            }
        }
        Ok(Err(e)) => error!("[Jobs] Lease reclaim failed: {}", e),
        Err(join_err) => error!("[Jobs] Lease reclaim join error: {}", join_err),
    }
    state.last_reclaim = Instant::now();
}

async fn process_job(
    job: JobRecord,
    worker_id: String,
    registry: VideoRegistry,
    vfs: Arc<VtxVfsManager>,
    scan_limiter: Option<Arc<AdaptiveScanLimiter>>,
    lease_secs: u64,
    timeout_secs: u64,
) {
    let job_id = job.id.clone();
    let job_type = job.job_type.clone();
    let payload = job.payload.clone();
    let payload_version = job.payload_version;
    let max_retries = job.max_retries;
    let retries = job.retries;
    let running = Arc::new(AtomicBool::new(true));
    let heartbeat_running = running.clone();
    let heartbeat_registry = registry.clone();
    let heartbeat_worker = worker_id.clone();
    let heartbeat_interval = Duration::from_secs(std::cmp::max(1, lease_secs / 2));
    let heartbeat_job_id = job_id.clone();

    tokio::spawn(async move {
        while heartbeat_running.load(Ordering::Relaxed) {
            sleep(heartbeat_interval).await;
            if !heartbeat_running.load(Ordering::Relaxed) {
                break;
            }
            let registry = heartbeat_registry.clone();
            let job_id = heartbeat_job_id.clone();
            let worker_id = heartbeat_worker.clone();
            let result = tokio::task::spawn_blocking(move || {
                registry.renew_job_lease(&job_id, &worker_id, lease_secs)
            })
            .await;
            if let Err(join_err) = result {
                error!("[Jobs] Lease renew join error: {}", join_err);
            } else if let Ok(Err(err)) = result {
                error!("[Jobs] Lease renew failed: {}", err);
            }
        }
    });

    let registry_for_job = registry.clone();
    let scan_permit = if job_type == "scan-directory" {
        if let Some(limiter) = scan_limiter {
            Some(limiter.acquire().await)
        } else {
            None
        }
    } else {
        None
    };
    let job_id_for_handle = job_id.clone();
    let handle_result = tokio::task::spawn_blocking(move || {
        let _permit = scan_permit;
        handle_job(
            &registry_for_job,
            vfs,
            &job_id_for_handle,
            &job_type,
            &payload,
            payload_version,
            timeout_secs,
        )
    })
    .await;

    match handle_result {
        Ok(Ok(())) => {
            running.store(false, Ordering::Relaxed);
        }
        Ok(Err(e)) => {
            running.store(false, Ordering::Relaxed);
            let should_retry = retries < max_retries;
            let registry_for_update = registry.clone();
            let job_id = job_id.clone();
            let err_message = e.clone();
            let update_result = tokio::task::spawn_blocking(move || {
                if should_retry {
                    registry_for_update.increment_job_retries(&job_id)?;
                    registry_for_update.retry_job(&job_id, &err_message)
                } else {
                    registry_for_update.fail_job(&job_id, &err_message)
                }
            })
            .await;

            if let Err(join_err) = update_result {
                error!("[Jobs] Retry/Fail join error: {}", join_err);
            } else if let Ok(Err(db_err)) = update_result {
                error!("[Jobs] Retry/Fail update error: {}", db_err);
            }
        }
        Err(join_err) => {
            running.store(false, Ordering::Relaxed);
            error!("[Jobs] Job handler join error: {}", join_err);
        }
    }
}
