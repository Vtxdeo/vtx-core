use crate::runtime::context::StreamContext;
use crate::runtime::manager::PluginManager;
use crate::storage::registry::VideoRegistry;
use wasmtime::component::Linker;
use wasmtime::Engine;

/// Web 应用全局状态
///
/// 包含所有跨请求共享的重资源对象，通过 Arc 注入到 Axum 的 Handler 中。
#[derive(Clone)]
pub struct AppState {
    pub engine: Engine,
    pub plugin_manager: PluginManager,
    pub registry: VideoRegistry,
    pub linker: Linker<StreamContext>,
}
