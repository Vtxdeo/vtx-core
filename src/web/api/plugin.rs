use crate::runtime::executor::PluginExecutor;
use crate::web::{state::AppState, utils::streaming::StreamProtocolLayer};
use axum::{
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// 通用网关处理器 (Gateway Handler)
///
/// 职责：
/// 1. 拦截所有非系统路由的请求
/// 2. 在 PluginManager 中匹配最长前缀路由
/// 3. 将请求转发给对应的插件执行
pub async fn gateway_handler(
    State(state): State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let path = uri.path();
    let query = uri.query().unwrap_or("").to_string();

    // 1. 路由匹配
    let (plugin_runtime, sub_path) = match state.plugin_manager.match_route(path) {
        Some(res) => res,
        None => {
            return (
                StatusCode::NOT_FOUND,
                format!("No plugin configured to handle route: {}", path),
            )
                .into_response();
        }
    };

    // 2. 执行插件
    let result = PluginExecutor::execute_runtime(
        &state,
        plugin_runtime,
        sub_path,
        method.to_string(), // 转换为 String 传给 WASM
        query,
    )
    .await;

    // 3. 处理响应
    match result {
        Ok((buffer, _status_code)) => StreamProtocolLayer::process(buffer, &headers).await,
        Err(msg) => {
            if msg == "NO_CONTENT" {
                (StatusCode::NOT_FOUND, "Resource not found").into_response()
            } else {
                tracing::error!("[Gateway] Execution failed: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, "Plugin execution failed").into_response()
            }
        }
    }
}
