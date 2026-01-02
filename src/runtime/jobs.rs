use crate::config::JobQueueSettings;
use crate::runtime::job_registry;
use crate::storage::VideoRegistry;
use serde::Deserialize;
use std::time::Duration;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use tokio::time::{sleep, Instant};
use tracing::{error, info, warn};

#[derive(Deserialize)]
struct ScanDirectoryPayload {
    path: String,
}

pub fn spawn_workers(registry: VideoRegistry, settings: JobQueueSettings) {
    let workers = std::cmp::max(1, settings.max_concurrent) as usize;
    for idx in 0..workers {
        let worker_id = format!("worker-{}", idx + 1);
        let registry = registry.clone();
        let poll_interval = Duration::from_millis(settings.poll_interval_ms);
        let timeout_secs = settings.timeout_secs;
        let sweep_interval = Duration::from_millis(settings.sweep_interval_ms);
        let lease_secs = settings.lease_secs;
        let reclaim_interval = Duration::from_millis(settings.reclaim_interval_ms);
        tokio::spawn(async move {
            let mut last_sweep = Instant::now();
            let mut last_reclaim = Instant::now();
            loop {
                if last_sweep.elapsed() >= sweep_interval {
                    let registry = registry.clone();
                    let sweep_result = tokio::task::spawn_blocking(move || {
                        registry.fail_timed_out_jobs(timeout_secs)
                    })
                    .await;
                    match sweep_result {
                        Ok(Ok(count)) => {
                            if count > 0 {
                                warn!("[Jobs] Marked {} timed-out jobs as failed", count);
                            }
                        }
                        Ok(Err(e)) => error!("[Jobs] Timeout sweep failed: {}", e),
                        Err(join_err) => error!("[Jobs] Timeout sweep join error: {}", join_err),
                    }
                    last_sweep = Instant::now();
                }
                if last_reclaim.elapsed() >= reclaim_interval {
                    let registry = registry.clone();
                    let reclaim_result = tokio::task::spawn_blocking(move || {
                        registry.requeue_expired_job_leases()
                    })
                    .await;
                    match reclaim_result {
                        Ok(Ok(count)) => {
                            if count > 0 {
                                warn!("[Jobs] Requeued {} expired leases", count);
                            }
                        }
                        Ok(Err(e)) => error!("[Jobs] Lease reclaim failed: {}", e),
                        Err(join_err) => error!("[Jobs] Lease reclaim join error: {}", join_err),
                    }
                    last_reclaim = Instant::now();
                }

                let claim_result = tokio::task::spawn_blocking({
                    let registry = registry.clone();
                    let worker_id = worker_id.clone();
                    move || registry.claim_next_job(&worker_id, lease_secs)
                })
                .await;

                match claim_result {
                    Ok(Ok(Some(job))) => {
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
                        let handle_result = tokio::task::spawn_blocking(move || {
                            handle_job(
                                &registry_for_job,
                                &job_id,
                                &job_type,
                                &payload,
                                payload_version,
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
                    Ok(Ok(None)) => sleep(poll_interval).await,
                    Ok(Err(e)) => {
                        error!("[Jobs] Claim failed: {}", e);
                        sleep(poll_interval).await;
                    }
                    Err(join_err) => {
                        error!("[Jobs] Claim join error: {}", join_err);
                        sleep(poll_interval).await;
                    }
                };
            }
        });
    }
}

pub async fn recover_startup(registry: VideoRegistry, settings: JobQueueSettings) {
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

    let timeout_sweep = tokio::task::spawn_blocking(move || {
        registry.fail_timed_out_jobs(timeout_secs)
    })
    .await;
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

fn handle_job(
    registry: &VideoRegistry,
    job_id: &str,
    job_type: &str,
    payload: &str,
    payload_version: i64,
) -> Result<(), String> {
    let payload_value: serde_json::Value =
        serde_json::from_str(payload).map_err(|e| format!("Invalid payload: {}", e))?;
    let (normalized_payload, _) =
        job_registry::normalize_payload(job_type, &payload_value, payload_version)?;
    match job_type {
        "noop" => {
            registry
                .complete_job(job_id, r#"{"status":"ok"}"#)
                .map_err(|e| e.to_string())
        }
        "scan-directory" => handle_scan_directory(registry, job_id, &normalized_payload),
        _ => Err("unsupported job_type".into()),
    }
}

fn handle_scan_directory(
    registry: &VideoRegistry,
    job_id: &str,
    payload: &serde_json::Value,
) -> Result<(), String> {
    let payload: ScanDirectoryPayload = serde_json::from_value(payload.clone())
        .map_err(|e| format!("Invalid payload: {}", e))?;
    let allowed_roots = registry
        .list_scan_roots()
        .map_err(|e| format!("Load scan roots failed: {}", e))?;
    let scan_root = validate_scan_path(&payload.path, &allowed_roots)?;

    match registry.scan_directory(&scan_root.to_string_lossy()) {
        Ok(new_videos) => {
            let result = serde_json::json!({
                "scanned_count": new_videos.len(),
            });
            registry
                .complete_job(job_id, &result.to_string())
                .map_err(|e| e.to_string())?;
            info!("[Jobs] scan-directory completed: {} new videos", new_videos.len());
            Ok(())
        }
        Err(e) => Err(format!("Scan failed: {}", e)),
    }
}

fn validate_scan_path(requested: &str, allowed_roots: &[std::path::PathBuf]) -> Result<std::path::PathBuf, String> {
    let resolved = std::fs::canonicalize(requested)
        .map_err(|_| "Invalid scan path".to_string())?;

    if !resolved.is_dir() {
        return Err("Scan path must be a directory".into());
    }

    let mut has_root = false;
    for root in allowed_roots {
        let Ok(root_path) = std::fs::canonicalize(root) else {
            continue;
        };
        has_root = true;
        if resolved.starts_with(&root_path) {
            return Ok(resolved);
        }
    }

    if !has_root {
        return Err("Scan roots not configured".into());
    }

    Err("Scan path not allowed".into())
}
