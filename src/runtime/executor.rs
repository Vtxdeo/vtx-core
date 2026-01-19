use crate::common::buffer::RealBuffer;
use crate::runtime::bus::EventBus;
use crate::runtime::context::{CurrentUser, SecurityPolicy, StreamContext, StreamContextConfig};
use crate::runtime::ffmpeg::VtxFfmpegManager;
use crate::runtime::manager::PluginRuntime;
use crate::runtime::vtx_host_impl::{api, VtxPlugin};
use crate::storage::VtxVideoRegistry;
use crate::vtx_vfs::VtxVfsManager;
use crate::web::state::AppState;
use std::sync::Arc;
use wasmtime::Store;

pub struct VtxPluginExecutor;

pub struct EventDispatchContext {
    pub engine: wasmtime::Engine,
    pub registry: VtxVideoRegistry,
    pub vtx_ffmpeg: Arc<VtxFfmpegManager>,
    pub vfs: Arc<VtxVfsManager>,
    pub event_bus: Arc<EventBus>,
    pub max_memory_bytes: usize,
    pub max_buffer_read_bytes: u64,
}

impl VtxPluginExecutor {
    fn build_limits(max_memory_bytes: usize) -> wasmtime::StoreLimits {
        wasmtime::StoreLimitsBuilder::new()
            .memory_size(max_memory_bytes)
            .instances(5)
            .tables(1000)
            .build()
    }

    fn build_context(
        state: &AppState,
        runtime: &PluginRuntime,
        current_user: Option<CurrentUser>,
    ) -> StreamContext {
        let permissions = runtime
            .policy
            .permissions
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let memory_limit_bytes = state.config.plugins.max_memory_mb as usize * 1024 * 1024;
        let max_buffer_read_bytes = state.config.plugins.max_buffer_read_mb * 1024 * 1024;
        let limits = Self::build_limits(memory_limit_bytes);

        StreamContext::new_secure(StreamContextConfig {
            registry: state.registry.clone(),
            vtx_ffmpeg: state.vtx_ffmpeg.clone(),
            vfs: state.vfs.clone(),
            limiter: limits,
            policy: SecurityPolicy::Plugin,
            plugin_id: Some(runtime.id.clone()),
            max_buffer_read_bytes,
            current_user,
            event_bus: state.event_bus.clone(),
            permissions,
            http_allowlist: runtime.policy.http.clone(),
        })
    }

    fn build_store(engine: &wasmtime::Engine, ctx: StreamContext) -> Store<StreamContext> {
        let mut store = Store::new(engine, ctx);
        store.limiter(|s| &mut s.limiter);
        store
    }

    async fn instantiate_plugin(
        store: &mut Store<StreamContext>,
        instance_pre: &wasmtime::component::InstancePre<StreamContext>,
    ) -> Result<VtxPlugin, String> {
        let instance = instance_pre
            .instantiate_async(&mut *store)
            .await
            .map_err(|e| format!("Fast instantiation failed: {}", e))?;
        VtxPlugin::new(store, &instance).map_err(|e| format!("Plugin binding failed: {}", e))
    }

    fn build_request(
        method: String,
        sub_path: String,
        query: String,
    ) -> api::vtx_types::HttpRequest {
        api::vtx_types::HttpRequest {
            method,
            path: sub_path,
            query,
        }
    }

    fn resolve_response(
        store: &mut Store<StreamContext>,
        response: api::vtx_types::HttpResponse,
    ) -> Result<(Option<RealBuffer>, u16), String> {
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

    pub async fn execute_runtime(
        state: &Arc<AppState>,
        runtime: Arc<PluginRuntime>,
        sub_path: String,
        method: String,
        query: String,
        current_user: Option<CurrentUser>,
    ) -> Result<(Option<RealBuffer>, u16), String> {
        let ctx = Self::build_context(state.as_ref(), runtime.as_ref(), current_user);
        let mut store = Self::build_store(&state.engine, ctx);
        let plugin = Self::instantiate_plugin(&mut store, &runtime.instance_pre).await?;
        let req = Self::build_request(method, sub_path, query);

        let response = plugin
            .call_handle(&mut store, &req)
            .await
            .map_err(|e| format!("Execution failed: {}", e))?;

        Self::resolve_response(&mut store, response)
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

        let plugin = VtxPlugin::new(&mut store, &instance)
            .map_err(|e| format!("Plugin binding failed: {}", e))?;

        let event_payload = serde_json::to_string(&event.payload)
            .map_err(|_| "Event payload serialize failed".to_string())?;

        let wit_event = api::vtx_events::VtxEvent {
            id: event.id,
            topic: event.topic,
            source: event.source,
            payload: event_payload,
            context: api::vtx_events::EventContext {
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
