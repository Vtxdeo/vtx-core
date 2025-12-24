use crate::web::state::AppState;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use std::sync::Arc;

/// 插件鉴权中间件
///
/// 职责：调用插件 `verify_identity` 方法进行身份验证。
/// 前置条件：必须在注册了 `AppState` 的路由组中使用。
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let headers = req.headers().clone();
    let manager = state.plugin_manager.clone();

    // 在阻塞线程执行插件验证
    let auth_result = tokio::task::spawn_blocking(move || manager.verify_identity(&headers)).await;

    match auth_result {
        Ok(plugin_result) => match plugin_result {
            Ok(user_context) => {
                tracing::debug!(
                    "[Auth] Authenticated: {} (id={})",
                    user_context.username,
                    user_context.user_id
                );
                let mut req = req;
                req.extensions_mut().insert(user_context);
                Ok(next.run(req).await)
            }
            Err(code) => {
                tracing::warn!("[Auth] Denied. Code={}", code);
                Err(StatusCode::from_u16(code).unwrap_or(StatusCode::UNAUTHORIZED))
            }
        },
        Err(e) => {
            tracing::error!("[Auth] Thread failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
