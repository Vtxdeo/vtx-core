use crate::config::Settings;
use crate::runtime::ffmpeg::VtxFfmpegManager;
use crate::runtime::bus::EventBus;
use crate::runtime::manager::PluginManager;
use crate::storage::VideoRegistry;
use std::sync::Arc;
use wasmtime::Engine;

/// Web 应用全局状态
///
/// 职责：包含所有跨请求共享的重资源对象，通过 Arc 注入到 Axum 的 Handler 中。
#[derive(Clone)]
pub struct AppState {
    pub engine: Engine,
    pub plugin_manager: PluginManager,
    pub registry: VideoRegistry,
    pub config: Settings,
    pub vtx_ffmpeg: Arc<VtxFfmpegManager>,
    pub event_bus: Arc<EventBus>,
}
