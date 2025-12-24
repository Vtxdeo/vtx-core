use crate::common::buffer::RealBuffer;
use crate::runtime::context::StreamContext;
use crate::runtime::host_impl::{api, Plugin};
use crate::web::state::AppState;
use std::sync::Arc;
use wasmtime::Store;

/// 插件请求执行器
///
/// 职责：封装单次请求的 Wasm 环境构建、执行与资源回收过程。
pub struct PluginExecutor;

impl PluginExecutor {
    /// 执行插件的 `handle` 方法
    ///
    /// - 参数 `state`: 全局应用状态，包含引擎与组件。
    /// - 参数 `path_param`: API 路径参数。
    /// - 返回值: 成功时返回缓冲区与 HTTP 状态码，失败返回错误描述。
    pub async fn execute(
        state: &Arc<AppState>,
        path_param: String,
    ) -> Result<(RealBuffer, u16), String> {
        let engine = state.engine.clone();
        let registry = state.registry.clone();
        let instance_pre = state.plugin_manager.get_instance_pre();

        // 异步任务中执行阻塞的 Wasm 调用
        tokio::task::spawn_blocking(move || {
            // 设置单次执行资源上限
            // 注意：当启用 Pooling Allocator 时，memory_size 必须小于等于 pool 配置的 max_memory_size
            let limits = wasmtime::StoreLimitsBuilder::new()
                .memory_size(100 * 1024 * 1024)
                .instances(5)
                .tables(1000)
                .build();

            let ctx = StreamContext::new_secure(registry, limits);
            let mut store = Store::new(&engine, ctx);
            store.limiter(|s| &mut s.limiter);

            let instance = instance_pre
                .instantiate(&mut store)
                .map_err(|e| format!("Fast instantiation failed: {}", e))?;

            // 使用生成的 Instance 构建强类型 Plugin 包装器
            let plugin = Plugin::new(&mut store, &instance)
                .map_err(|e| format!("Plugin binding failed: {}", e))?;

            let req = api::types::HttpRequest {
                method: "GET".to_string(),
                path: path_param,
                query: String::new(),
            };

            let response = plugin
                .call_handle(&mut store, &req)
                .map_err(|e| format!("Execution failed: {}", e))?;

            if let Some(resource_handle) = response.body {
                let buffer = store
                    .data_mut()
                    .table
                    .delete(resource_handle)
                    .map_err(|_| "Invalid buffer handle".to_string())?;
                Ok((buffer, response.status))
            } else {
                Err("NO_CONTENT".to_string())
            }
        })
        .await
        .map_err(|e| format!("Task join error: {}", e))?
    }
}
