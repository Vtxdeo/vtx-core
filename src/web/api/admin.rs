use crate::web::state::AppState;
use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json as AxumJson,
};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct ScanRequest {
    pub path: String,
}

#[derive(Deserialize)]
pub struct UninstallParams {
    pub keep_data: bool,
}

/// 扫描目录接口
///
/// 触发文件系统扫描并更新数据库。
pub async fn scan_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ScanRequest>,
) -> AxumJson<serde_json::Value> {
    match state.registry.scan_directory(&payload.path) {
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

/// 卸载插件接口
pub async fn uninstall_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<UninstallParams>,
) -> AxumJson<serde_json::Value> {
    match state.plugin_manager.uninstall(params.keep_data) {
        Ok(_) => AxumJson(serde_json::json!({
            "status": "success",
            "message": "uninstalled"
        })),
        Err(e) => AxumJson(serde_json::json!({
            "status": "error",
            "message": e.to_string()
        })),
    }
}
