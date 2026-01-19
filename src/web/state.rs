use crate::common::events::SystemRequest;
use crate::config::Settings;
use crate::runtime::bus::EventBus;
use crate::runtime::ffmpeg::VtxFfmpegManager;
use crate::runtime::manager::PluginManager;
use crate::storage::VtxVideoRegistry;
use crate::vtx_vfs::VtxVfsManager;
use std::sync::Arc;
use tokio::sync::mpsc;
use wasmtime::Engine;

pub struct AppState {
    pub engine: Engine,
    pub plugin_manager: PluginManager,
    pub registry: VtxVideoRegistry,
    pub config: Settings,
    pub vtx_ffmpeg: Arc<VtxFfmpegManager>,
    pub vfs: Arc<VtxVfsManager>,
    pub event_bus: Arc<EventBus>,
    #[allow(dead_code)]
    pub ipc_outbound: mpsc::Sender<SystemRequest>,
}
