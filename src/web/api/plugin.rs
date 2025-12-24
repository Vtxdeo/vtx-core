use crate::common::buffer::BufferType;
use crate::runtime::executor::PluginExecutor;
use crate::web::{state::AppState, utils::streaming::VideoStreamer};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// 插件业务执行 Handler
///
/// 职责：
/// 1. 接收请求参数。
/// 2. 调度 Runtime 执行 Wasm 逻辑。
/// 3. 根据返回类型（文件流/JSON）构造对应响应。
pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(path_param): Path<String>,
) -> Response {
    // 调用执行器
    let result = PluginExecutor::execute(&state, path_param).await;

    match result {
        Ok((buffer, status_code)) => {
            match buffer.inner {
                BufferType::File(file) => {
                    // 委托流处理工具处理视频流 (返回具体的 Response)
                    VideoStreamer::stream(file, &headers).await
                }
                BufferType::Memory(cursor) => {
                    // 返回内存 JSON 数据
                    Response::builder()
                        .status(StatusCode::from_u16(status_code).unwrap_or(StatusCode::OK))
                        .header("Content-Type", "application/json")
                        .header("X-Powered-By", "vtxdeo-api")
                        .body(Body::from(cursor.into_inner()))
                        .unwrap()
                        .into_response()
                }
            }
        }
        Err(msg) => {
            // 处理业务逻辑错误或系统错误
            if msg == "NO_CONTENT" {
                (StatusCode::NOT_FOUND, "Resource not found").into_response()
            } else {
                tracing::error!("[Handler] System error: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, "Worker execution failed").into_response()
            }
        }
    }
}
