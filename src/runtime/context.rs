use std::sync::Arc;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};

use crate::runtime::bus::EventBus;
use crate::runtime::ffmpeg::VtxFfmpegManager;
use crate::storage::VideoRegistry;

/// 安全策略等级
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityPolicy {
    /// 根权限：允许所有操作（文件IO、数据库读写、进程执行）
    /// 适用于：正常请求处理 (PluginExecutor)
    Root,
    /// Plugin policy: allow plugin to access its own resources (restricted SQL)
    /// For HTTP gateway plugin requests
    Plugin,
    /// 受限权限：仅允许只读 SQL，禁止文件 IO 和进程执行
    /// 适用于：身份验证 (verify_identity)
    Restricted,
}

/// 插件沙箱运行时上下文
pub struct StreamContext {
    pub table: ResourceTable,
    pub wasi: WasiCtx,
    pub registry: VideoRegistry,
    pub limiter: wasmtime::StoreLimits,
    pub policy: SecurityPolicy,
    pub plugin_id: Option<String>,
    pub max_buffer_read_bytes: u64,
    pub current_user: Option<CurrentUser>,
    pub event_bus: Arc<EventBus>,
    pub permissions: std::collections::HashSet<String>,
    /// VtxFfmpeg 管理器引用
    /// 允许 Host Function 访问工具链配置
    pub vtx_ffmpeg: Arc<VtxFfmpegManager>,
}

pub struct StreamContextConfig {
    pub registry: VideoRegistry,
    pub vtx_ffmpeg: Arc<VtxFfmpegManager>,
    pub limiter: wasmtime::StoreLimits,
    pub policy: SecurityPolicy,
    pub plugin_id: Option<String>,
    pub max_buffer_read_bytes: u64,
    pub current_user: Option<CurrentUser>,
    pub event_bus: Arc<EventBus>,
    pub permissions: std::collections::HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct CurrentUser {
    pub user_id: String,
    pub username: String,
    pub groups: Vec<String>,
}

impl StreamContext {
    /// 创建一个零信任的插件沙箱上下文
    pub fn new_secure(config: StreamContextConfig) -> Self {
        let StreamContextConfig {
            registry,
            vtx_ffmpeg,
            limiter,
            policy,
            plugin_id,
            max_buffer_read_bytes,
            current_user,
            event_bus,
            permissions,
        } = config;
        let wasi = WasiCtxBuilder::new()
            .inherit_stdio()
            .env("VTX_ENV", "production")
            .args(&["plugin_worker"])
            .build();

        Self {
            table: ResourceTable::new(),
            wasi,
            registry,
            limiter,
            policy,
            plugin_id,
            max_buffer_read_bytes,
            current_user,
            event_bus,
            permissions,
            vtx_ffmpeg,
        }
    }
}

impl StreamContext {
    pub fn has_permission(&self, perm: &str) -> bool {
        self.permissions.iter().any(|p| p == perm)
    }
}

impl WasiView for StreamContext {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

/// 实现资源限制接口
impl wasmtime::ResourceLimiter for StreamContext {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.limiter.memory_growing(current, desired, maximum)
    }

    fn table_growing(
        &mut self,
        current: u32,
        desired: u32,
        maximum: Option<u32>,
    ) -> wasmtime::Result<bool> {
        self.limiter.table_growing(current, desired, maximum)
    }
}
