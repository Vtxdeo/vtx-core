use wasmtime::Engine;
use wasmtime::component::Linker;
use crate::storage::registry::VideoRegistry;
use crate::runtime::manager::PluginManager;
use crate::runtime::context::StreamContext;

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
