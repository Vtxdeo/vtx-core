mod common;
mod config;
mod runtime;
mod storage;
mod web;

use axum::{
    routing::{any, delete, get, post},
    Router,
};
use std::sync::Arc;
use tower_http::{catch_panic::CatchPanicLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::info;
use wasmtime::component::Linker;

use crate::config::Settings;
use crate::runtime::{ffmpeg::VtxFfmpegManager, host_impl::api, manager::PluginManager};
use crate::storage::VideoRegistry;
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
                .add_directive("vtx_core=info".parse()?)
                .add_directive("tower_http=debug".parse()?),
        )
        .init();

    info!("[Startup] vtxdeo Core V0.1.3 initializing...");

    let settings = Settings::new().expect("Failed to load config");
    info!(
        "[Config] Binding at {}:{}",
        settings.server.host, settings.server.port
    );
    info!(
        "[Config] Max plugin memory: {} MB",
        settings.plugins.max_memory_mb
    );

    // 基础设施初始化
    let mut wasm_config = wasmtime::Config::new();
    wasm_config.wasm_component_model(true);
    wasm_config.async_support(false);

    let mut pooling_strategy = wasmtime::PoolingAllocationConfig::default();

    // 设置最大保留的热实例槽位
    pooling_strategy.max_unused_warm_slots(16);

    // 设置单个线性内存的最大页数
    // Wasm 页大小为 64KB。
    // 动态计算: (max_mb * 1024 * 1024) / 65536
    let max_memory_bytes = settings.plugins.max_memory_mb * 1024 * 1024;
    let max_wasm_pages = max_memory_bytes / 65536;

    // 必须确保 Pooling Allocator 的预留空间 >= 实例请求的空间
    pooling_strategy.memory_pages(max_wasm_pages);

    // 设置并发组件实例上限
    pooling_strategy.total_component_instances(100);

    wasm_config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(
        pooling_strategy,
    ));

    let engine = wasmtime::Engine::new(&wasm_config)?;
    let mut linker = Linker::<runtime::context::StreamContext>::new(&engine);
    wasmtime_wasi::add_to_linker_sync(&mut linker)?;
    api::stream_io::add_to_linker(&mut linker, |ctx| ctx)?;
    api::sql::add_to_linker(&mut linker, |ctx| ctx)?;
    api::ffmpeg::add_to_linker(&mut linker, |ctx| ctx)?;

    let registry = VideoRegistry::new(&settings.database.url, 120)?;

    // 初始化 vtx-ffmpeg 中间层管理器
    let vtx_ffmpeg_manager = Arc::new(VtxFfmpegManager::new(
        settings.vtx_ffmpeg.binary_root.clone(),
        settings.vtx_ffmpeg.execution_timeout_secs,
    ));

    // 初始化插件管理器 (传入 vtx_ffmpeg_manager)
    let plugin_manager = PluginManager::new(
        engine.clone(),
        settings.plugins.location.clone(),
        registry.clone(),
        linker,
        settings.plugins.auth_provider.clone(),
        vtx_ffmpeg_manager.clone(),
    )?;

    // 构造全局状态
    let state = Arc::new(AppState {
        engine,
        plugin_manager,
        registry,
        config: settings.clone(),
        vtx_ffmpeg: vtx_ffmpeg_manager,
    });

    // 路由定义
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        // 管理后台路由 (优先级最高)
        .nest(
            "/admin",
            Router::new()
                .route("/scan", post(admin::scan_handler))
                .route("/videos", get(admin::list_handler))
                .route("/plugins", get(admin::list_plugins_handler))
                .route("/plugin", delete(admin::uninstall_handler))
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    auth_middleware,
                )),
        )
        // 插件网关路由 (Catch-All)
        // 任何未被匹配的请求都会进入 gateway_handler，由它分发给具体插件
        .route("/*path", any(plugin::gateway_handler))
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
