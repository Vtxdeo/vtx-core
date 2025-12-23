use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
    http::StatusCode,
};
use std::sync::Arc;
use crate::state::AppState;

/// 插件鉴权中间件
///
/// - 调用插件的 `verify_identity` 实现自定义身份认证
/// - 插件需返回 HTTP 状态码或包含用户信息的结构体
/// - 成功后将 `UserContext` 注入 request 扩展中
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let headers = req.headers().clone();
    let manager = state.plugin_manager.clone();

    // 在阻塞线程中执行插件身份验证逻辑
    let auth_result = tokio::task::spawn_blocking(move || {
        manager.verify_identity(&headers)
    }).await;

    match auth_result {
        Ok(plugin_result) => match plugin_result {
            Ok(user_context) => {
                tracing::debug!(
                    "[Auth] User authenticated: {} (id = {})",
                    user_context.username,
                    user_context.user_id
                );

                // 注入用户上下文到请求扩展中
                let mut req = req;
                req.extensions_mut().insert(user_context);

                Ok(next.run(req).await)
            }
            Err(code) => {
                tracing::warn!("[Auth] Unauthorized access. Code = {}", code);
                Err(StatusCode::from_u16(code).unwrap_or(StatusCode::UNAUTHORIZED))
            }
        },
        Err(e) => {
            tracing::error!("[Auth] Authentication thread failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
