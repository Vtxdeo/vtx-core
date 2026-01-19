use std::sync::Arc;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::runtime::bus::EventBus;
use crate::runtime::ffmpeg::VtxFfmpegManager;
use crate::runtime::vtx_host_impl::api::vtx_types::HttpAllowRule;
use crate::storage::VideoRegistry;
use crate::vtx_vfs::VtxVfsManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityPolicy {
    Root,

    Plugin,

    Restricted,
}

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
    pub http_allowlist: Vec<HttpAllowRule>,

    pub vtx_ffmpeg: Arc<VtxFfmpegManager>,
    pub vfs: Arc<VtxVfsManager>,
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
    pub http_allowlist: Vec<HttpAllowRule>,
    pub vfs: Arc<VtxVfsManager>,
}

#[derive(Debug, Clone)]
pub struct CurrentUser {
    pub user_id: String,
    pub username: String,
    pub groups: Vec<String>,
}

impl StreamContext {
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
            http_allowlist,
            vfs,
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
            http_allowlist,
            vtx_ffmpeg,
            vfs,
        }
    }
}

impl StreamContext {
    pub fn has_permission(&self, perm: &str) -> bool {
        self.permissions.iter().any(|p| p == perm)
    }
}

impl WasiView for StreamContext {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

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
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.limiter.table_growing(current, desired, maximum)
    }
}
