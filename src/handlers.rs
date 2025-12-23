use axum::{
    extract::{Path, State, Json, Query},
    response::IntoResponse,
    body::Body,
    http::{HeaderMap, StatusCode, header},
};
use std::sync::Arc;
use std::io::{Seek, SeekFrom};
use tokio::fs::File as TokioFile;
use tokio_util::io::ReaderStream;
use tokio::io::AsyncReadExt;
use serde::Deserialize;
use wasmtime::Store;

use crate::state::{AppState, StreamContext};
use crate::registry::VideoMeta;
use crate::host::{Plugin, api};
use crate::resources::BufferType;

// =====================
// Admin 请求参数定义
// =====================

#[derive(Deserialize)]
pub struct ScanRequest {
    pub path: String,
}

#[derive(Deserialize)]
pub struct UninstallParams {
    pub keep_data: bool,
}

// =====================
// Admin 接口
// =====================

/// 扫描指定目录并注册新的视频资源
pub async fn admin_scan_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ScanRequest>,
) -> Json<serde_json::Value> {
    let new_videos = state.registry.scan_directory(&payload.path);
    Json(serde_json::json!({
        "status": "success",
        "scanned_count": new_videos.len(),
        "data": new_videos
    }))
}

/// 列出当前系统中已注册的所有视频
pub async fn admin_list_handler(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<VideoMeta>> {
    Json(state.registry.list_all())
}

/// 卸载当前插件
pub async fn admin_uninstall_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<UninstallParams>,
) -> Json<serde_json::Value> {
    match state.plugin_manager.uninstall(params.keep_data) {
        Ok(_) => Json(serde_json::json!({
            "status": "success",
            "message": "plugin uninstalled",
            "data_kept": params.keep_data
        })),
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "message": e.to_string()
        })),
    }
}

// =====================
// Plugin 执行入口
// =====================

/// 插件 HTTP 请求处理入口
///
/// - 在阻塞线程中执行 Wasm 插件
/// - 根据插件返回的 Buffer 类型构造响应
pub async fn plugin_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(path_param): Path<String>,
) -> impl IntoResponse {
    let engine = state.engine.clone();
    let registry = state.registry.clone();
    let component = state.plugin_manager.get_component();
    let linker = state.linker.clone();
    let request_path = path_param.clone();

    let result = tokio::task::spawn_blocking(move || {
        // 配置插件执行资源限制
        let limits = wasmtime::StoreLimitsBuilder::new()
            .memory_size(100 * 1024 * 1024)
            .instances(5)
            .tables(1000)
            .build();

        let ctx = StreamContext::new_secure(registry, limits);
        let mut store = Store::new(&engine, ctx);
        store.limiter(|s| &mut s.limiter);

        let (plugin, _) = Plugin::instantiate(&mut store, &component, &linker)
            .map_err(|e| format!("plugin instantiation failed: {}", e))?;

        let req = api::types::HttpRequest {
            method: "GET".to_string(),
            path: request_path,
            query: String::new(),
        };

        let response = plugin.call_handle(&mut store, &req)
            .map_err(|e| format!("plugin execution failed: {}", e))?;

        if let Some(resource_handle) = response.body {
            let buffer = store.data_mut()
                .table
                .delete(resource_handle)
                .map_err(|_| "invalid buffer handle".to_string())?;
            Ok((buffer, response.status))
        } else {
            Err("NO_CONTENT".to_string())
        }
    }).await;

    match result {
        Ok(Ok((real_buffer, status_code))) => {
            match real_buffer.inner {
                // =====================
                // 文件流响应（视频）
                // =====================
                BufferType::File(mut std_file) => {
                    let file_size = std_file.metadata().unwrap().len();
                    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());

                    let (start, end) = match range_header {
                        Some(range) if range.starts_with("bytes=") => {
                            let parts: Vec<&str> = range["bytes=".len()..].split('-').collect();
                            let start = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(0);
                            let end = parts.get(1)
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(file_size - 1);
                            (start, end)
                        }
                        _ => (0, file_size - 1),
                    };

                    std_file.seek(SeekFrom::Start(start)).unwrap();
                    let tokio_file = TokioFile::from_std(std_file);
                    let content_length = end - start + 1;

                    let stream = ReaderStream::with_capacity(
                        tokio_file.take(content_length),
                        64 * 1024,
                    );

                    let status = if range_header.is_some() {
                        StatusCode::PARTIAL_CONTENT
                    } else {
                        StatusCode::from_u16(status_code).unwrap()
                    };

                    axum::response::Response::builder()
                        .status(status)
                        .header("Content-Type", "video/mp4")
                        .header("Accept-Ranges", "bytes")
                        .header("Content-Length", content_length.to_string())
                        .header(
                            "Content-Range",
                            format!("bytes {}-{}/{}", start, end, file_size),
                        )
                        .body(Body::from_stream(stream))
                        .unwrap()
                }

                // =====================
                // 内存响应（JSON / 文本）
                // =====================
                BufferType::Memory(cursor) => {
                    let data = cursor.into_inner();
                    axum::response::Response::builder()
                        .status(StatusCode::from_u16(status_code).unwrap())
                        .header("Content-Type", "application/json")
                        .header("X-Powered-By", "vtxdeo-api")
                        .body(Body::from(data))
                        .unwrap()
                }
            }
        }

        Ok(Err(err)) => {
            let status = if err == "NO_CONTENT" {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            axum::response::Response::builder()
                .status(status)
                .body(Body::from(err))
                .unwrap()
        }

        Err(join_err) => {
            axum::response::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("worker task failed: {}", join_err)))
                .unwrap()
        }
    }
}
