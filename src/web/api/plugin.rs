use crate::runtime::executor::PluginExecutor;
use crate::web::{state::AppState, utils::streaming::StreamProtocolLayer};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// HTTP 请求处理器：处理插件资源请求并返回流式响应
pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(path_param): Path<String>,
) -> Response {
    // 调用插件执行器，运行对应的插件逻辑，返回缓冲区结果
    let result = PluginExecutor::execute(&state, path_param).await;

    match result {
        Ok((buffer, _status_code)) => {
            // 使用流协议层统一处理响应细节（MIME、ETag、Range 支持等）
            StreamProtocolLayer::process(buffer, &headers).await
        }
        Err(msg) => {
            // 处理业务逻辑错误或系统错误
            if msg == "NO_CONTENT" {
                (StatusCode::NOT_FOUND, "Resource not found").into_response()
            } else {
                tracing::error!("[handler] 插件执行失败: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, "Worker execution failed").into_response()
            }
        }
    }
}
