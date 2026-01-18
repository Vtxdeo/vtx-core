use crate::runtime::host_impl::api::auth_types::UserContext;
use crate::runtime::job_registry;
use crate::web::state::AppState;
use crate::web::utils::errors;
use axum::{
    extract::{Extension, Json, Path, Query, State},
    Json as AxumJson,
};
use serde::Deserialize;
use std::path::Path as StdPath;
use std::sync::Arc;
use url::Url;

#[derive(Deserialize)]
pub struct ScanRequest {
    pub path: String,
}

#[derive(Deserialize)]
pub struct ScanRootRequest {
    pub path: String,
}

#[derive(Deserialize)]
pub struct UninstallParams {
    pub plugin_id: String,
    pub keep_data: bool,
}

#[derive(Deserialize)]
pub struct JobSubmitRequest {
    pub job_type: String,
    pub payload: serde_json::Value,
    pub max_retries: Option<i64>,
    pub payload_version: Option<i64>,
}

#[derive(Deserialize)]
pub struct JobListParams {
    pub limit: Option<i64>,
}

/// 扫描目录接口
///
pub async fn scan_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ScanRequest>,
) -> AxumJson<serde_json::Value> {
    let allowed_roots = match state.registry.list_scan_roots() {
        Ok(roots) => roots,
        Err(e) => {
            tracing::error!("[Admin] Scan roots load failed: {}", e);
            return AxumJson(errors::admin_internal_error_json(&e.to_string()));
        }
    };

    let scan_root = match validate_scan_path(&payload.path, &allowed_roots, &state.vfs) {
        Ok(path) => path,
        Err(message) => return AxumJson(errors::admin_bad_request_json(&message)),
    };

    let registry = state.registry.clone();
    let vfs = state.vfs.clone();
    let scan_root = scan_root.clone();
    let result = tokio::task::spawn_blocking(move || {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(registry.scan_directory(&vfs, &scan_root))
    })
    .await;

    match result {
        Ok(Ok(new_videos)) => AxumJson(success_with_count(new_videos, "scanned_count")),
        Ok(Err(e)) => {
            tracing::error!("[Admin] Scan failed: {}", e);
            AxumJson(errors::admin_internal_error_json(&e.to_string()))
        }
        Err(e) => {
            tracing::error!("[Admin] Scan join failed: {}", e);
            AxumJson(errors::admin_internal_error_json(&e.to_string()))
        }
    }
}

fn validate_scan_path(
    requested: &str,
    allowed_roots: &[String],
    vfs: &crate::vtx_vfs::VtxVfsManager,
) -> Result<String, String> {
    let requested_uri = normalize_request_uri(vfs, requested, false)?;
    vfs.match_allowed_prefix(&requested_uri, allowed_roots)
}

/// 列表查询接口
pub async fn list_handler(State(state): State<Arc<AppState>>) -> AxumJson<serde_json::Value> {
    match state.registry.list_all() {
        Ok(videos) => AxumJson(success_with_count(videos, "count")),
        Err(e) => {
            tracing::error!("[Admin] List videos failed: {}", e);
            AxumJson(errors::admin_internal_error_json(&e.to_string()))
        }
    }
}

/// 列出所有已加载插件
pub async fn list_plugins_handler(
    State(state): State<Arc<AppState>>,
) -> AxumJson<serde_json::Value> {
    let plugins = state.plugin_manager.list_plugins();
    AxumJson(success_with_count(plugins, "count"))
}

/// 卸载插件接口
pub async fn uninstall_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<UninstallParams>,
) -> AxumJson<serde_json::Value> {
    match state
        .plugin_manager
        .uninstall(&params.plugin_id, params.keep_data)
    {
        Ok(_) => AxumJson(success_json(serde_json::json!({
            "message": format!("Plugin '{}' uninstalled", params.plugin_id)
        }))),
        Err(e) => AxumJson(errors::admin_internal_error_json(&e.to_string())),
    }
}

pub async fn list_scan_roots_handler(
    State(state): State<Arc<AppState>>,
) -> AxumJson<serde_json::Value> {
    match state.registry.list_scan_roots() {
        Ok(roots) => AxumJson(success_with_count(roots, "count")),
        Err(e) => AxumJson(errors::admin_internal_error_json(&e.to_string())),
    }
}

pub async fn add_scan_root_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ScanRootRequest>,
) -> AxumJson<serde_json::Value> {
    let uri = match normalize_request_uri(&state.vfs, &payload.path, true) {
        Ok(value) => value,
        Err(message) => return AxumJson(errors::admin_bad_request_json(&message)),
    };
    match state.registry.add_scan_root(&uri) {
        Ok(resolved) => AxumJson(success_json(serde_json::json!({ "path": resolved }))),
        Err(e) => AxumJson(errors::admin_internal_error_json(&e.to_string())),
    }
}

pub async fn remove_scan_root_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ScanRootRequest>,
) -> AxumJson<serde_json::Value> {
    let uri = match normalize_request_uri(&state.vfs, &payload.path, true) {
        Ok(value) => value,
        Err(message) => return AxumJson(errors::admin_bad_request_json(&message)),
    };
    match state.registry.remove_scan_root(&uri) {
        Ok(resolved) => AxumJson(success_json(serde_json::json!({ "path": resolved }))),
        Err(e) => AxumJson(errors::admin_internal_error_json(&e.to_string())),
    }
}

fn normalize_request_uri(
    vfs: &crate::vtx_vfs::VtxVfsManager,
    raw: &str,
    ensure_prefix: bool,
) -> Result<String, String> {
    let uri = if looks_like_path(raw) {
        let path = StdPath::new(raw);
        let abs = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| e.to_string())?
                .join(path)
        };
        Url::from_file_path(&abs)
            .map_err(|_| format!("Invalid file path: {}", abs.display()))?
            .to_string()
    } else {
        raw.to_string()
    };

    let normalized = if ensure_prefix {
        vfs.ensure_prefix_uri(&uri)
    } else {
        vfs.normalize_uri(&uri)
    };
    normalized.map_err(|e| e.to_string())
}

fn looks_like_path(value: &str) -> bool {
    if value.starts_with("\\\\") {
        return true;
    }
    let bytes = value.as_bytes();
    if bytes.len() >= 2 {
        let letter = bytes[0];
        if letter.is_ascii_alphabetic() && bytes[1] == b':' {
            return true;
        }
    }
    !looks_like_uri(value)
}

fn looks_like_uri(value: &str) -> bool {
    value.contains("://") || value.starts_with("file:") || value.starts_with("s3:")
}

pub async fn submit_job_handler(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(payload): Json<JobSubmitRequest>,
) -> AxumJson<serde_json::Value> {
    if let Err(message) = validate_job_submission(&user, &payload) {
        return AxumJson(errors::admin_bad_request_json(&message));
    }
    let max_retries = payload.max_retries.unwrap_or(0);
    let payload_version = payload.payload_version.unwrap_or(1);
    let (normalized_payload, normalized_version) =
        match job_registry::normalize_payload(&payload.job_type, &payload.payload, payload_version)
        {
            Ok(result) => result,
            Err(message) => return AxumJson(errors::admin_bad_request_json(&message)),
        };
    let payload_json = normalized_payload.to_string();
    match state.registry.enqueue_job(
        &payload.job_type,
        &payload_json,
        normalized_version,
        max_retries,
    ) {
        Ok(job_id) => AxumJson(success_json(serde_json::json!({ "job_id": job_id }))),
        Err(e) => AxumJson(errors::admin_internal_error_json(&e.to_string())),
    }
}

pub async fn get_job_handler(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> AxumJson<serde_json::Value> {
    match state.registry.get_job(&job_id) {
        Ok(Some(job)) => AxumJson(success_json(job)),
        Ok(None) => AxumJson(errors::admin_not_found_json("Job not found")),
        Err(e) => AxumJson(errors::admin_internal_error_json(&e.to_string())),
    }
}

fn validate_job_submission(user: &UserContext, payload: &JobSubmitRequest) -> Result<(), String> {
    let payload_version = payload.payload_version.unwrap_or(1);
    job_registry::validate_job_submission(
        &payload.job_type,
        &payload.payload,
        Some(&user.groups),
        payload_version,
    )
}

pub async fn list_jobs_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<JobListParams>,
) -> AxumJson<serde_json::Value> {
    let limit = params.limit.unwrap_or(50).max(1);
    match state.registry.list_recent_jobs(limit) {
        Ok(jobs) => AxumJson(success_with_count(jobs, "count")),
        Err(e) => AxumJson(errors::admin_internal_error_json(&e.to_string())),
    }
}

pub async fn cancel_job_handler(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> AxumJson<serde_json::Value> {
    match state.registry.cancel_job(&job_id) {
        Ok(0) => AxumJson(errors::admin_not_found_json(
            "Job not found or not cancelable",
        )),
        Ok(_) => AxumJson(success_json(serde_json::json!({ "job_id": job_id }))),
        Err(e) => AxumJson(errors::admin_internal_error_json(&e.to_string())),
    }
}

fn success_json<T: serde::Serialize>(data: T) -> serde_json::Value {
    serde_json::json!({
        "status": "success",
        "data": data
    })
}

fn success_with_count<T: serde::Serialize>(data: T, key: &str) -> serde_json::Value {
    let mut value = success_json(data);
    if let serde_json::Value::Object(ref mut map) = value {
        if let Some(data_value) = map.get("data") {
            if let Some(array) = data_value.as_array() {
                map.insert(key.to_string(), serde_json::json!(array.len()));
            }
        }
    }
    value
}
