use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};

use crate::storage::VideoRegistry;

/// 安全策略等级
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityPolicy {
    /// 根权限：允许所有操作（文件IO、数据库读写）
    /// 适用于：正常请求处理 (PluginExecutor)
    Root,
    /// 受限权限：仅允许只读 SQL，禁止文件 IO
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
}

impl StreamContext {
    /// 创建一个零信任的插件沙箱上下文
    pub fn new_secure(
        registry: VideoRegistry,
        limiter: wasmtime::StoreLimits,
        policy: SecurityPolicy,
    ) -> Self {
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
        }
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
///
/// 职责：限制内存、表空间的增长，防止插件滥用资源。
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
