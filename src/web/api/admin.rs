use crate::web::state::AppState;
use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json as AxumJson,
};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;

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

/// 扫描目录接口
///
/// 触发文件系统扫描并更新数据库。
pub async fn scan_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ScanRequest>,
) -> AxumJson<serde_json::Value> {
    let allowed_roots = match state.registry.list_scan_roots() {
        Ok(roots) => roots,
        Err(e) => {
            tracing::error!("[Admin] Scan roots load failed: {}", e);
            return AxumJson(serde_json::json!({
                "status": "error",
                "message": "Failed to load scan roots"
            }));
        }
    };

    let scan_root = match validate_scan_path(&payload.path, &allowed_roots) {
        Ok(path) => path,
        Err(message) => {
            return AxumJson(serde_json::json!({
                "status": "error",
                "message": message
            }))
        }
    };

    match state
        .registry
        .scan_directory(&scan_root.to_string_lossy())
    {
        Ok(new_videos) => AxumJson(serde_json::json!({
            "status": "success",
            "scanned_count": new_videos.len(),
            "data": new_videos
        })),
        Err(e) => {
            tracing::error!("[Admin] Scan failed: {}", e);
            AxumJson(serde_json::json!({
                "status": "error",
                "message": e.to_string()
            }))
        }
    }
}

fn validate_scan_path(
    requested: &str,
    allowed_roots: &[PathBuf],
) -> Result<PathBuf, String> {
    let resolved = std::fs::canonicalize(requested)
        .map_err(|_| "Invalid scan path".to_string())?;

    if !resolved.is_dir() {
        return Err("Scan path must be a directory".into());
    }

    let mut has_root = false;
    for root in allowed_roots {
        let Ok(root_path) = std::fs::canonicalize(root) else {
            tracing::warn!("[Admin] Invalid scan root: {:?}", root);
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

/// 列表查询接口
pub async fn list_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.registry.list_all() {
        Ok(videos) => AxumJson(videos).into_response(),
        Err(e) => {
            tracing::error!("[Admin] List videos failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
                .into_response()
        }
    }
}

/// 列出所有已加载插件
pub async fn list_plugins_handler(
    State(state): State<Arc<AppState>>,
) -> AxumJson<serde_json::Value> {
    let plugins = state.plugin_manager.list_plugins();
    AxumJson(serde_json::json!({
        "status": "success",
        "count": plugins.len(),
        "data": plugins
    }))
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
        Ok(_) => AxumJson(serde_json::json!({
            "status": "success",
            "message": format!("Plugin '{}' uninstalled", params.plugin_id)
        })),
        Err(e) => AxumJson(serde_json::json!({
            "status": "error",
            "message": e.to_string()
        })),
    }
}

pub async fn list_scan_roots_handler(
    State(state): State<Arc<AppState>>,
) -> AxumJson<serde_json::Value> {
    match state.registry.list_scan_roots() {
        Ok(roots) => AxumJson(serde_json::json!({
            "status": "success",
            "count": roots.len(),
            "data": roots
        })),
        Err(e) => AxumJson(serde_json::json!({
            "status": "error",
            "message": e.to_string()
        })),
    }
}

pub async fn add_scan_root_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ScanRootRequest>,
) -> AxumJson<serde_json::Value> {
    let path = PathBuf::from(payload.path);
    match state.registry.add_scan_root(&path) {
        Ok(resolved) => AxumJson(serde_json::json!({
            "status": "success",
            "path": resolved
        })),
        Err(e) => AxumJson(serde_json::json!({
            "status": "error",
            "message": e.to_string()
        })),
    }
}

pub async fn remove_scan_root_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ScanRootRequest>,
) -> AxumJson<serde_json::Value> {
    let path = PathBuf::from(payload.path);
    match state.registry.remove_scan_root(&path) {
        Ok(resolved) => AxumJson(serde_json::json!({
            "status": "success",
            "path": resolved
        })),
        Err(e) => AxumJson(serde_json::json!({
            "status": "error",
            "message": e.to_string()
        })),
    }
}
