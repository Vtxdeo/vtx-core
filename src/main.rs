mod common;
mod config;
mod runtime;
mod storage;
mod web;

use axum::{
    routing::{delete, get, post},
    Router,
};
use std::sync::Arc;
use tower_http::{catch_panic::CatchPanicLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::info;
use wasmtime::component::Linker;

use crate::config::Settings;
use crate::runtime::{host_impl::api, manager::PluginManager};
use crate::storage::registry::VideoRegistry;
use crate::web::{
    api::{admin, plugin},
    middleware::auth::auth_middleware,
    state::AppState,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("vtx_core=info".parse().unwrap())
                .add_directive("tower_http=debug".parse().unwrap()),
        )
        .init();

    info!("[Startup] vtxdeo Core 2.0 (Refactored) initializing...");

    let settings = Settings::new().expect("Failed to load config");
    info!(
        "[Config] Binding at {}:{}",
        settings.server.host, settings.server.port
    );

    // 基础设施初始化
    let mut wasm_config = wasmtime::Config::new();
    wasm_config.wasm_component_model(true);
    wasm_config.async_support(false);
    let engine = wasmtime::Engine::new(&wasm_config)?;

    let mut linker = Linker::<crate::runtime::context::StreamContext>::new(&engine);
    wasmtime_wasi::add_to_linker_sync(&mut linker)?;
    api::stream_io::add_to_linker(&mut linker, |ctx| ctx)?;
    api::sql::add_to_linker(&mut linker, |ctx| ctx)?;

    let registry = VideoRegistry::new(&settings.database.url)?;
    let plugin_manager = PluginManager::new(
        engine.clone(),
        settings.plugins.location.clone(),
        registry.clone(),
    )?;

    // 构造全局状态
    let state = Arc::new(AppState {
        engine,
        plugin_manager,
        registry,
        linker,
    });

    // 路由定义
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .nest(
            "/admin",
            Router::new()
                .route("/scan", post(admin::scan_handler))
                .route("/videos", get(admin::list_handler))
                .route("/plugin", delete(admin::uninstall_handler))
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    auth_middleware,
                )),
        )
        .route("/api/video/*path", get(plugin::handler))
        .with_state(state)
        .layer(CorsLayer::permissive())
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http());

    let addr = format!("{}:{}", settings.server.host, settings.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("[Startup] Service ready at http://{}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}
