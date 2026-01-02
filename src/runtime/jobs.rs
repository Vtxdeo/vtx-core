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
                let claim_result = tokio::task::spawn_blocking({
                    let registry = registry.clone();
                    let worker_id = worker_id.clone();
                    move || registry.claim_next_job(&worker_id)
                })
                .await;

                match claim_result {
                    Ok(Ok(Some(job))) => {
                        let job_id = job.id.clone();
                        let job_type = job.job_type.clone();
                        let payload = job.payload.clone();
                        let max_retries = job.max_retries;
                        let retries = job.retries;
                        let registry_for_job = registry.clone();
                        let handle_result = tokio::task::spawn_blocking(move || {
                            handle_job(&registry_for_job, &job_id, &job_type, &payload)
                        })
                        .await;

                        match handle_result {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
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
