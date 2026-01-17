use crate::runtime::executor::PluginExecutor;
use crate::web::{state::AppState, utils::errors, utils::streaming::StreamProtocolLayer};
use axum::{
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
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
                Json(errors::plugin_not_found_json(
                    "No plugin configured for route",
                )),
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
        None,
    )
    .await;

    // 3. 处理响应
    match result {
        Ok((Some(buffer), status_code)) => {
            StreamProtocolLayer::process(buffer, &headers, status_code, state.vfs.clone()).await
        }
        Ok((None, status_code)) => StatusCode::from_u16(status_code)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            .into_response(),
        Err(msg) => {
            tracing::error!("[Gateway] Execution failed: {}", msg);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(errors::plugin_internal_error_json(&msg)),
            )
                .into_response()
        }
    }
}
