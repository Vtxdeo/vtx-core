use crate::config::JobQueueSettings;
use crate::storage::VideoRegistry;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::sleep;
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
        tokio::spawn(async move {
            loop {
                match registry.claim_next_job(&worker_id) {
                    Ok(Some(job)) => {
                        if let Err(e) = handle_job(&registry, &job.id, &job.job_type, &job.payload) {
                            let should_retry = job.retries < job.max_retries;
                            if should_retry {
                                if let Err(err) = registry.increment_job_retries(&job.id) {
                                    error!("[Jobs] Failed to increment retries: {}", err);
                                }
                                if let Err(err) = registry.retry_job(&job.id, &e) {
                                    error!("[Jobs] Failed to reschedule job {}: {}", job.id, err);
                                }
                            } else if let Err(err) = registry.fail_job(&job.id, &e) {
                                error!("[Jobs] Failed to mark job {} as failed: {}", job.id, err);
                            }
                        }
                    }
                    Ok(None) => sleep(poll_interval).await,
                    Err(e) => {
                        error!("[Jobs] Claim failed: {}", e);
                        sleep(poll_interval).await;
                    }
                }
            }
        });
    }
}

fn handle_job(
    registry: &VideoRegistry,
    job_id: &str,
    job_type: &str,
    payload: &str,
) -> Result<(), String> {
    match job_type {
        "noop" => {
            registry
                .complete_job(job_id, r#"{"status":"ok"}"#)
                .map_err(|e| e.to_string())
        }
        "scan-directory" => handle_scan_directory(registry, job_id, payload),
        _ => {
            let message = format!("Unknown job type: {}", job_type);
            warn!("[Jobs] {}", message);
            Err(message)
        }
    }
}

fn handle_scan_directory(
    registry: &VideoRegistry,
    job_id: &str,
    payload: &str,
) -> Result<(), String> {
    let payload: ScanDirectoryPayload =
        serde_json::from_str(payload).map_err(|e| format!("Invalid payload: {}", e))?;
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
