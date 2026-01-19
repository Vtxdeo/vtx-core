mod common;
mod config;
mod runtime;
mod storage;
mod vtx_vfs;
mod web;

use axum::{
    routing::{any, delete, get, post},
    Router,
};
use std::io;
use std::sync::Arc;
use tower_http::{catch_panic::CatchPanicLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::info;
use wasmtime::component::{HasSelf, Linker};
use wasmtime_wasi::p2::add_to_linker_async;

use crate::config::Settings;
use crate::runtime::{
    bus::EventBus,
    ffmpeg::VtxFfmpegManager,
    jobs,
    manager::{PluginManager, PluginManagerConfig},
    vtx_host_impl::api,
    vtx_host_impl::vtx_ipc_transport::VtxIpcTransport,
};
use crate::storage::VideoRegistry;
use crate::vtx_vfs::VtxVfsManager;
use crate::web::{
    api::{admin, plugin, ws},
    middleware::auth::auth_middleware,
    state::AppState,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("vtx_core=info".parse()?)
                .add_directive("tower_http=debug".parse()?),
        )
        .with_writer(io::stderr)
        .init();

    info!("[Startup] vtxdeo Core V0.1.6 initializing...");

    let settings = Settings::new().expect("Failed to load config");
    info!(
        "[Config] Binding at {}:{}",
        settings.server.host, settings.server.port
    );
    info!(
        "[Config] Max plugin memory: {} MB",
        settings.plugins.max_memory_mb
    );

    let mut wasm_config = wasmtime::Config::new();
    wasm_config.wasm_component_model(true);
    wasm_config.async_support(true);

    let mut pooling_strategy = wasmtime::PoolingAllocationConfig::default();

    let max_memory_bytes = settings.plugins.max_memory_mb * 1024 * 1024;
    pooling_strategy.max_unused_warm_slots(16);

    pooling_strategy.max_memory_size(max_memory_bytes as usize);

    pooling_strategy.total_component_instances(100);

    wasm_config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(
        pooling_strategy,
    ));

    let engine = wasmtime::Engine::new(&wasm_config)?;
    let mut linker = Linker::<runtime::context::StreamContext>::new(&engine);
    add_to_linker_async(&mut linker)?;
    api::vtx_vfs::add_to_linker::<_, HasSelf<_>>(&mut linker, |ctx| ctx)?;
    api::vtx_sql::add_to_linker::<_, HasSelf<_>>(&mut linker, |ctx| ctx)?;
    api::vtx_ffmpeg::add_to_linker::<_, HasSelf<_>>(&mut linker, |ctx| ctx)?;
    api::vtx_context::add_to_linker::<_, HasSelf<_>>(&mut linker, |ctx| ctx)?;
    api::vtx_event_bus::add_to_linker::<_, HasSelf<_>>(&mut linker, |ctx| ctx)?;
    api::vtx_http_client::add_to_linker::<_, HasSelf<_>>(&mut linker, |ctx| ctx)?;

    let registry = VideoRegistry::new(&settings.database.url, 120)?;
    let vfs = Arc::new(VtxVfsManager::new()?);

    let vtx_ffmpeg_manager = Arc::new(VtxFfmpegManager::new(
        settings.vtx_ffmpeg.execution_timeout_secs,
    )?);

    let event_bus = Arc::new(EventBus::new(256));
    let (ipc_outbound_tx, ipc_outbound_rx) = tokio::sync::mpsc::channel(100);
    VtxIpcTransport::spawn(ipc_outbound_rx);

    let plugin_manager = PluginManager::new(PluginManagerConfig {
        engine: engine.clone(),
        plugin_root: settings.plugins.location.clone(),
        registry: registry.clone(),
        vfs: vfs.clone(),
        linker,
        auth_provider: settings.plugins.auth_provider.clone(),
        vtx_ffmpeg: vtx_ffmpeg_manager.clone(),
        max_buffer_read_bytes: settings.plugins.max_buffer_read_mb * 1024 * 1024,
        max_memory_bytes: max_memory_bytes as usize,
        event_bus: event_bus.clone(),
    })
    .await?;

    let state = Arc::new(AppState {
        engine,
        plugin_manager,
        registry,
        config: settings.clone(),
        vtx_ffmpeg: vtx_ffmpeg_manager,
        vfs: vfs.clone(),
        event_bus,
        ipc_outbound: ipc_outbound_tx,
    });

    jobs::recover_startup(state.registry.clone(), settings.job_queue.clone()).await;
    jobs::spawn_workers(state.registry.clone(), vfs, settings.job_queue.clone());

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .nest(
            "/admin",
            Router::new()
                .route("/scan", post(admin::scan_handler))
                .route("/scan-roots", get(admin::list_scan_roots_handler))
                .route("/scan-roots", post(admin::add_scan_root_handler))
                .route("/scan-roots", delete(admin::remove_scan_root_handler))
                .route("/videos", get(admin::list_handler))
                .route("/plugins", get(admin::list_plugins_handler))
                .route("/plugin", delete(admin::uninstall_handler))
                .route("/jobs", post(admin::submit_job_handler))
                .route("/jobs", get(admin::list_jobs_handler))
                .route("/jobs/{id}", get(admin::get_job_handler))
                .route("/jobs/{id}/cancel", post(admin::cancel_job_handler))
                .route("/ws/events", get(ws::ws_handler))
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    auth_middleware,
                )),
        )
        .route("/{*path}", any(plugin::gateway_handler))
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
