use crate::common::buffer::RealBuffer;
use crate::runtime::bus::EventBus;
use crate::runtime::context::{CurrentUser, SecurityPolicy, StreamContext};
use crate::runtime::ffmpeg::VtxFfmpegManager;
use crate::runtime::host_impl::{api, Plugin};
use crate::runtime::manager::PluginRuntime;
use crate::storage::VideoRegistry;
use crate::web::state::AppState;
use std::sync::Arc;
use wasmtime::Store;

/// 插件请求执行器
///
/// 职责：封装单次请求的 Wasm 环境构建、执行与资源回收过程。
pub struct PluginExecutor;

impl PluginExecutor {
    /// 执行指定的插件实例
    ///
    /// - `state`: 全局应用状态
    /// - `runtime`: 目标插件的运行时上下文（包含预编译实例）
    /// - `sub_path`: 剔除挂载点后的子路径（传给插件的 path 参数）
    /// - `method`: HTTP 方法 (e.g., "GET", "POST")
    /// - `query`: URL 查询字符串 (e.g., "page=1&limit=10")
    pub async fn execute_runtime(
        state: &Arc<AppState>,
        runtime: Arc<PluginRuntime>,
        sub_path: String,
        method: String,
        query: String,
        current_user: Option<CurrentUser>,
    ) -> Result<(RealBuffer, u16), String> {
        let engine = state.engine.clone();
        let registry = state.registry.clone();
        // 获取全局的 ffmpeg 管理器引用
        let vtx_ffmpeg = state.vtx_ffmpeg.clone();

        // 使用传入的 instance_pre，而非去 manager 全局查找
        let instance_pre = runtime.instance_pre.clone();
        let plugin_id = runtime.id.clone();

        let memory_limit_bytes = state.config.plugins.max_memory_mb as usize * 1024 * 1024;
        let max_buffer_read_bytes = state.config.plugins.max_buffer_read_mb * 1024 * 1024;

        let limits = wasmtime::StoreLimitsBuilder::new()
            .memory_size(memory_limit_bytes)
            .instances(5)
            .tables(1000)
            .build();

        // 注入 vtx_ffmpeg 到上下文
        let permissions = runtime
            .policy
            .permissions
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();

        let ctx = StreamContext::new_secure(
            registry,
            vtx_ffmpeg,
            limits,
            SecurityPolicy::Plugin,
            Some(plugin_id),
            max_buffer_read_bytes,
            current_user,
            state.event_bus.clone(),
            permissions,
        );

        let mut store = Store::new(&engine, ctx);
        store.limiter(|s| &mut s.limiter);

        let instance = instance_pre
            .instantiate_async(&mut store)
            .await
            .map_err(|e| format!("Fast instantiation failed: {}", e))?;

        let plugin = Plugin::new(&mut store, &instance)
            .map_err(|e| format!("Plugin binding failed: {}", e))?;

        let req = api::types::HttpRequest {
            method,
            path: sub_path,
            query,
        };

        let response = plugin
            .call_handle(&mut store, &req)
            .await
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
    }

    #[allow(dead_code)]
    pub async fn dispatch_event(
        state: &Arc<AppState>,
        runtime: Arc<PluginRuntime>,
        event: crate::common::events::VtxEvent,
    ) -> Result<(), String> {
        Self::dispatch_event_with(
            state.engine.clone(),
            state.registry.clone(),
            state.vtx_ffmpeg.clone(),
            state.event_bus.clone(),
            state.config.plugins.max_memory_mb as usize * 1024 * 1024,
            state.config.plugins.max_buffer_read_mb * 1024 * 1024,
            runtime,
            event,
        )
        .await
    }

    pub async fn dispatch_event_with(
        engine: wasmtime::Engine,
        registry: VideoRegistry,
        vtx_ffmpeg: Arc<VtxFfmpegManager>,
        event_bus: Arc<EventBus>,
        max_memory_bytes: usize,
        max_buffer_read_bytes: u64,
        runtime: Arc<PluginRuntime>,
        event: crate::common::events::VtxEvent,
    ) -> Result<(), String> {
        let plugin_id = runtime.id.clone();
        let permissions = runtime
            .policy
            .permissions
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let limits = wasmtime::StoreLimitsBuilder::new()
            .memory_size(max_memory_bytes)
            .instances(5)
            .tables(1000)
            .build();

        let current_user = event.context.user_id.as_ref().map(|user_id| CurrentUser {
            user_id: user_id.clone(),
            username: event.context.username.clone().unwrap_or_default(),
            groups: Vec::new(),
        });

        let ctx = StreamContext::new_secure(
            registry,
            vtx_ffmpeg,
            limits,
            SecurityPolicy::Plugin,
            Some(plugin_id),
            max_buffer_read_bytes,
            current_user,
            event_bus,
            permissions,
        );

        let mut store = Store::new(&engine, ctx);
        store.limiter(|s| &mut s.limiter);

        let instance = runtime
            .instance_pre
            .instantiate_async(&mut store)
            .await
            .map_err(|e| format!("Event instantiation failed: {}", e))?;

        let plugin = Plugin::new(&mut store, &instance)
            .map_err(|e| format!("Plugin binding failed: {}", e))?;

        let event_payload = serde_json::to_string(&event.payload)
            .map_err(|_| "Event payload serialize failed".to_string())?;

        let wit_event = api::events::VtxEvent {
            id: event.id,
            topic: event.topic,
            source: event.source,
            payload: event_payload,
            context: api::events::EventContext {
                user_id: event.context.user_id,
                username: event.context.username,
                request_id: event.context.request_id,
            },
            occurred_at: event.occurred_at,
        };

        plugin
            .call_handle_event(&mut store, &wit_event)
            .await
            .map_err(|e| format!("Event dispatch failed: {}", e))?
            .map_err(|e| format!("Event handler rejected: {}", e))
    }
}
