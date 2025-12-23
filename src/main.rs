mod resources;
mod registry;
mod state;
mod host;
mod handlers;
mod plugin_manager;
mod config;
mod middleware;

use std::sync::Arc;
use axum::{routing::{get, post}, Router};
use axum::middleware as axum_middleware;
use wasmtime::{Engine, Config};
use wasmtime::component::Linker;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::registry::VideoRegistry;
use crate::state::{AppState, StreamContext};
use crate::plugin_manager::PluginManager;
use crate::handlers::{
    admin_scan_handler,
    admin_list_handler,
    plugin_handler,
    admin_uninstall_handler,
};
use crate::middleware::auth::auth_middleware;
use crate::host::api;
use crate::config::Settings;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化日志系统（支持通过 RUST_LOG 控制日志级别）
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("vtx_core=info".parse().unwrap())
                .add_directive("tower_http=debug".parse().unwrap())
        )
        .init();

    info!("[Startup] vtxdeo Core 1.1 initializing...");

    // 加载配置（支持默认值、config.{toml/yaml/json}、环境变量）
    let settings = Settings::new().expect("Failed to load configuration");
    info!("[Config] Loaded. Binding at {}:{}", settings.server.host, settings.server.port);

    // 初始化 Wasmtime 引擎（启用 Component Model，关闭 async 支持）
    let mut wasm_config = Config::new();
    wasm_config.wasm_component_model(true);
    wasm_config.async_support(false);
    let engine = Engine::new(&wasm_config)?;

    // 初始化插件链接器（注册宿主接口）
    let mut linker = Linker::<StreamContext>::new(&engine);
    wasmtime_wasi::add_to_linker_sync(&mut linker)?;
    api::stream_io::add_to_linker(&mut linker, |ctx| ctx)?;
    api::sql::add_to_linker(&mut linker, |ctx| ctx)?;

    // 初始化视频注册表（SQLite，用于插件访问数据）
    let registry = VideoRegistry::new(&settings.database.url)?;

    // 加载插件管理器（自动执行迁移并锁定插件 ID）
    let plugin_manager = PluginManager::new(
        engine.clone(),
        settings.plugins.location.clone(),
        registry.clone(),
    )?;

    // 构造全局状态对象（AppState）
    let state = Arc::new(AppState {
        engine,
        plugin_manager,
        registry,
        linker,
    });

    // 构建 HTTP 路由
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))

        // 管理接口（受保护路由）
        .nest("/admin", Router::new()
            .route("/scan", post(admin_scan_handler))
            .route("/videos", get(admin_list_handler))
            .route("/plugin", axum::routing::delete(admin_uninstall_handler))
            .layer(axum_middleware::from_fn_with_state(state.clone(), auth_middleware)) // 注册鉴权中间件
        )

        // 插件执行入口
        .route("/api/video/*path", get(plugin_handler))

        // 注入全局状态与中间件
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    // 启动服务监听
    let addr = format!("{}:{}", settings.server.host, settings.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("[Startup] Service is ready at http://{}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}
