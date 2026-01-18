use crate::common::json_guard::check_json_limits;
use crate::runtime::job_registry;
use crate::storage::{
    videos::{ScanAbort, ScanOutcome},
    VideoRegistry,
};
use crate::vtx_vfs::VtxVfsManager;
use serde::Deserialize;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};

#[derive(Deserialize)]
struct ScanDirectoryPayload {
    path: String,
}

pub(crate) fn handle_job(
    registry: &VideoRegistry,
    vfs: Arc<VtxVfsManager>,
    job_id: &str,
    job_type: &str,
    payload: &str,
    payload_version: i64,
    timeout_secs: u64,
) -> Result<(), String> {
    const MAX_JOB_PAYLOAD_BYTES: usize = 256 * 1024;
    const MAX_JOB_JSON_DEPTH: usize = 20;

    check_json_limits(payload, MAX_JOB_PAYLOAD_BYTES, MAX_JOB_JSON_DEPTH)
        .map_err(|e| format!("Invalid payload: {}", e))?;

    let payload_value: serde_json::Value =
        serde_json::from_str(payload).map_err(|e| format!("Invalid payload: {}", e))?;
    let (normalized_payload, _) =
        job_registry::normalize_payload(job_type, &payload_value, payload_version)?;
    match job_type {
        "noop" => registry
            .complete_job(job_id, r#"{"status":"ok"}"#)
            .map_err(|e| e.to_string()),
        "scan-directory" => {
            handle_scan_directory(registry, vfs, job_id, &normalized_payload, timeout_secs)
        }
        _ => Err("unsupported job_type".into()),
    }
}

fn handle_scan_directory(
    registry: &VideoRegistry,
    vfs: Arc<VtxVfsManager>,
    job_id: &str,
    payload: &serde_json::Value,
    timeout_secs: u64,
) -> Result<(), String> {
    let payload: ScanDirectoryPayload =
        serde_json::from_value(payload.clone()).map_err(|e| format!("Invalid payload: {}", e))?;
    let allowed_roots = registry
        .list_scan_roots()
        .map_err(|e| format!("Load scan roots failed: {}", e))?;
    let scan_root = validate_scan_path(&payload.path, &allowed_roots, &vfs)?;

    let running = Arc::new(AtomicBool::new(true));
    let abort_reason = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let monitor = {
        let registry = registry.clone();
        let job_id = job_id.to_string();
        let running = running.clone();
        let abort_reason = abort_reason.clone();
        let done = done.clone();
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        std::thread::spawn(move || {
            let poll = Duration::from_secs(1);
            while !done.load(Ordering::Relaxed) {
                if Instant::now() >= deadline {
                    abort_reason.store(2, Ordering::Relaxed);
                    running.store(false, Ordering::Relaxed);
                    break;
                }
                match registry.get_job_status(&job_id) {
                    Ok(Some(status)) => {
                        if status != "running" {
                            abort_reason.store(1, Ordering::Relaxed);
                            running.store(false, Ordering::Relaxed);
                            break;
                        }
                    }
                    Ok(None) => {
                        abort_reason.store(1, Ordering::Relaxed);
                        running.store(false, Ordering::Relaxed);
                        break;
                    }
                    Err(err) => {
                        warn!("[Jobs] scan-directory status check failed: {}", err);
                    }
                }
                std::thread::sleep(poll);
            }
        })
    };

    let handle = tokio::runtime::Handle::current();
    let outcome = handle
        .block_on(registry.scan_directory_with_abort(&vfs, &scan_root, || {
            if running.load(Ordering::Relaxed) {
                Ok(())
            } else {
                match abort_reason.load(Ordering::Relaxed) {
                    2 => Err(ScanAbort::TimedOut),
                    _ => Err(ScanAbort::Canceled),
                }
            }
        }))
        .map_err(|e| format!("Scan failed: {}", e))?;

    done.store(true, Ordering::Relaxed);
    let _ = monitor.join();

    match outcome {
        ScanOutcome::Completed(new_videos) => {
            let result = serde_json::json!({
                "scanned_count": new_videos.len(),
            });
            registry
                .complete_job(job_id, &result.to_string())
                .map_err(|e| e.to_string())?;
            info!(
                "[Jobs] scan-directory completed: {} new videos",
                new_videos.len()
            );
            Ok(())
        }
        ScanOutcome::Aborted(ScanAbort::Canceled) => {
            let _ = registry.set_job_error(job_id, "canceled");
            let _ = registry.set_job_result(job_id, r#"{"status":"canceled"}"#);
            let _ = registry.update_job_progress(job_id, 0);
            let _ = registry.set_job_status_terminal(job_id, "canceled");
            info!("[Jobs] scan-directory canceled");
            Ok(())
        }
        ScanOutcome::Aborted(ScanAbort::TimedOut) => {
            let _ = registry.set_job_error(job_id, "timeout");
            let _ = registry.set_job_result(job_id, r#"{"status":"timeout"}"#);
            let _ = registry.update_job_progress(job_id, 0);
            let _ = registry.set_job_status_terminal(job_id, "failed");
            Err("timeout".into())
        }
    }
}

fn validate_scan_path(
    requested: &str,
    allowed_roots: &[String],
    vfs: &VtxVfsManager,
) -> Result<String, String> {
    vfs.match_allowed_prefix(requested, allowed_roots)
}
