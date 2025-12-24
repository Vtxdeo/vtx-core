use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};

use crate::storage::registry::VideoRegistry;

/// 插件沙箱运行时上下文
///
/// 包含受限的 WASI 环境、资源表、插件注册表和执行限制器。
pub struct StreamContext {
    pub table: ResourceTable,
    pub wasi: WasiCtx,
    pub registry: VideoRegistry,
    pub limiter: wasmtime::StoreLimits,
}

impl StreamContext {
    /// 创建一个零信任的插件沙箱上下文
    ///
    /// 安全策略包括：
    /// 1. 继承标准输入输出，仅用于日志目的。
    /// 2. 不注入宿主环境变量，避免敏感信息泄露（如云服务密钥等）。
    /// 3. 禁止访问宿主传参，仅注入固定参数。
    /// 4. 不开放宿主文件系统，插件无法访问宿主磁盘。
    ///    所有文件访问必须通过 Host 的 stream_io 接口实现。
    pub fn new_secure(registry: VideoRegistry, limiter: wasmtime::StoreLimits) -> Self {
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
