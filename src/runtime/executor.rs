use crate::common::buffer::RealBuffer;
use crate::runtime::bus::EventBus;
use crate::runtime::context::{CurrentUser, SecurityPolicy, StreamContext, StreamContextConfig};
use crate::runtime::ffmpeg::VtxFfmpegManager;
use crate::runtime::host_impl::{api, Plugin};
use crate::runtime::manager::PluginRuntime;
use crate::storage::VideoRegistry;
use crate::vtx_vfs::VtxVfsManager;
use crate::web::state::AppState;
use std::sync::Arc;
use wasmtime::Store;

pub struct PluginExecutor;

pub struct EventDispatchContext {
    pub engine: wasmtime::Engine,
    pub registry: VideoRegistry,
    pub vtx_ffmpeg: Arc<VtxFfmpegManager>,
    pub vfs: Arc<VtxVfsManager>,
    pub event_bus: Arc<EventBus>,
    pub max_memory_bytes: usize,
    pub max_buffer_read_bytes: u64,
}

impl PluginExecutor {
    pub async fn execute_runtime(
        state: &Arc<AppState>,
        runtime: Arc<PluginRuntime>,
        sub_path: String,
        method: String,
        query: String,
        current_user: Option<CurrentUser>,
    ) -> Result<(Option<RealBuffer>, u16), String> {
        let engine = state.engine.clone();
        let registry = state.registry.clone();

        let vtx_ffmpeg = state.vtx_ffmpeg.clone();

        let instance_pre = runtime.instance_pre.clone();
        let plugin_id = runtime.id.clone();

        let memory_limit_bytes = state.config.plugins.max_memory_mb as usize * 1024 * 1024;
        let max_buffer_read_bytes = state.config.plugins.max_buffer_read_mb * 1024 * 1024;

        let limits = wasmtime::StoreLimitsBuilder::new()
            .memory_size(memory_limit_bytes)
            .instances(5)
            .tables(1000)
            .build();

        let permissions = runtime
            .policy
            .permissions
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();

        let ctx = StreamContext::new_secure(StreamContextConfig {
            registry,
            vtx_ffmpeg,
            vfs: state.vfs.clone(),
            limiter: limits,
            policy: SecurityPolicy::Plugin,
            plugin_id: Some(plugin_id),
            max_buffer_read_bytes,
            current_user,
            event_bus: state.event_bus.clone(),
            permissions,
            http_allowlist: runtime.policy.http.clone(),
        });

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
            Ok((Some(buffer), response.status))
        } else {
            Ok((None, response.status))
        }
    }

    #[allow(dead_code)]
    pub async fn dispatch_event(
        state: &Arc<AppState>,
        runtime: Arc<PluginRuntime>,
        event: crate::common::events::VtxEvent,
    ) -> Result<(), String> {
        Self::dispatch_event_with(
            EventDispatchContext {
                engine: state.engine.clone(),
                registry: state.registry.clone(),
                vtx_ffmpeg: state.vtx_ffmpeg.clone(),
                vfs: state.vfs.clone(),
                event_bus: state.event_bus.clone(),
                max_memory_bytes: state.config.plugins.max_memory_mb as usize * 1024 * 1024,
                max_buffer_read_bytes: state.config.plugins.max_buffer_read_mb * 1024 * 1024,
            },
            runtime,
            event,
        )
        .await
    }

    pub async fn dispatch_event_with(
        context: EventDispatchContext,
        runtime: Arc<PluginRuntime>,
        event: crate::common::events::VtxEvent,
    ) -> Result<(), String> {
        let EventDispatchContext {
            engine,
            registry,
            vtx_ffmpeg,
            vfs,
            event_bus,
            max_memory_bytes,
            max_buffer_read_bytes,
        } = context;
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

        let ctx = StreamContext::new_secure(StreamContextConfig {
            registry,
            vtx_ffmpeg,
            vfs,
            limiter: limits,
            policy: SecurityPolicy::Plugin,
            plugin_id: Some(plugin_id),
            max_buffer_read_bytes,
            current_user,
            event_bus,
            permissions,
            http_allowlist: runtime.policy.http.clone(),
        });

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
