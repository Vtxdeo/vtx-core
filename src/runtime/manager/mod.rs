pub mod loader;
pub mod migration_policy;
pub mod watcher;

use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tracing::{error, info, warn};
use wasmtime::component::{Component, InstancePre, Linker};
use wasmtime::Engine;

use crate::runtime::bus::EventBus;
use crate::runtime::context::{SecurityPolicy, StreamContext, StreamContextConfig};
use crate::runtime::executor::{EventDispatchContext, PluginExecutor};
use crate::runtime::ffmpeg::VtxFfmpegManager;
use crate::runtime::host_impl::api::auth_types::UserContext;
use crate::runtime::host_impl::api::types::{HttpAllowRule, Manifest};
use crate::runtime::host_impl::Plugin;
use crate::storage::VideoRegistry;

#[derive(Debug, Clone, Serialize)]
pub struct VtxPackageMetadata {
    pub author: Option<String>,
    pub sdk_version: Option<String>,
    pub package: Option<String>,
    pub language: Option<String>,
    pub tool_name: Option<String>,
    pub tool_version: Option<String>,
}

pub struct PluginRuntime {
    pub id: String,
    pub manifest: Manifest,
    pub policy: PluginPolicy,
    pub vtx_meta: Option<VtxPackageMetadata>,
    pub instance_pre: InstancePre<StreamContext>,
    #[allow(dead_code)]
    pub component: Component,
    pub source_path: PathBuf,
}

/// 插件状态视图
#[derive(Serialize)]
pub struct PluginStatus {
    pub id: String,
    pub name: String,
    pub version: String,
    pub entrypoint: String,
    pub source_path: String,
    pub vtx_meta: Option<VtxPackageMetadata>,
}

#[derive(Debug, Clone, Default)]
pub struct PluginPolicy {
    pub subscriptions: Vec<String>,
    pub permissions: Vec<String>,
    pub http: Vec<HttpAllowRule>,
}

#[derive(Clone)]
pub struct PluginManager {
    engine: Engine,
    linker: Linker<StreamContext>,
    pub plugin_dir: PathBuf,
    registry: VideoRegistry,
    plugins: Arc<RwLock<HashMap<String, Arc<PluginRuntime>>>>,
    routes: Arc<RwLock<Vec<Arc<PluginRuntime>>>>,
    /// 鉴权提供者 ID
    auth_provider: Option<String>,
    /// VtxFfmpeg 工具链管理器
    pub vtx_ffmpeg: Arc<VtxFfmpegManager>,
    max_buffer_read_bytes: u64,
    max_memory_bytes: usize,
    event_bus: Arc<EventBus>,
}

pub struct PluginManagerConfig {
    pub engine: Engine,
    pub plugin_dir: PathBuf,
    pub registry: VideoRegistry,
    pub linker: Linker<StreamContext>,
    pub auth_provider: Option<String>,
    pub vtx_ffmpeg: Arc<VtxFfmpegManager>,
    pub max_buffer_read_bytes: u64,
    pub max_memory_bytes: usize,
    pub event_bus: Arc<EventBus>,
}

impl PluginManager {
    pub async fn new(config: PluginManagerConfig) -> anyhow::Result<Self> {
        let PluginManagerConfig {
            engine,
            mut plugin_dir,
            registry,
            linker,
            auth_provider,
            vtx_ffmpeg,
            max_buffer_read_bytes,
            max_memory_bytes,
            event_bus,
        } = config;
        if plugin_dir.is_file() {
            warn!(
                "[PluginManager] Configured path '{:?}' is a file, but a directory is expected.",
                plugin_dir
            );
            if let Some(parent) = plugin_dir.parent() {
                warn!(
                    "[PluginManager] Automatically adjusting plugin directory to: {:?}",
                    parent
                );
                plugin_dir = parent.to_path_buf();
            } else {
                return Err(anyhow::anyhow!("Invalid plugin directory path"));
            }
        }

        info!(
            "[PluginManager] Initializing plugin manager at: {:?}",
            plugin_dir
        );

        if !plugin_dir.exists() {
            std::fs::create_dir_all(&plugin_dir)?;
        }

        let manager = Self {
            engine: engine.clone(),
            linker,
            plugin_dir: plugin_dir.clone(),
            registry,
            plugins: Arc::new(RwLock::new(HashMap::new())),
            routes: Arc::new(RwLock::new(Vec::new())),
            auth_provider,
            vtx_ffmpeg,
            max_buffer_read_bytes,
            max_memory_bytes,
            event_bus,
        };

        manager.load_all_plugins().await?;

        // 确保配置的 auth_provider 确实已加载，防止单点故障导致的系统裸奔或 500 错误
        if let Some(auth_id) = &manager.auth_provider {
            let plugins = manager.plugins.read().unwrap();
            if !plugins.contains_key(auth_id) {
                error!(
                    "[Fatal] Configured auth_provider '{}' not found in loaded plugins!",
                    auth_id
                );
                // 必须阻止启动，迫使管理员检查配置或插件文件
                return Err(anyhow::anyhow!(
                    "Critical: Configured auth_provider '{}' is missing. Startup aborted.",
                    auth_id
                ));
            }
            info!("[Auth] Verified auth_provider '{}' is active.", auth_id);
        }

        watcher::spawn_watcher(manager.clone(), tokio::runtime::Handle::current());

        Ok(manager)
    }

    async fn load_all_plugins(&self) -> anyhow::Result<()> {
        info!("[PluginManager] Scanning plugins in: {:?}", self.plugin_dir);
        let entries = std::fs::read_dir(&self.plugin_dir).map_err(|e| {
            anyhow::anyhow!(
                "Failed to read plugin directory '{:?}': {}",
                self.plugin_dir,
                e
            )
        })?;

        let mut loaded_count = 0;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "vtx") {
                match self.load_one(&path).await {
                    Ok(_) => loaded_count += 1,
                    Err(e) => error!("[PluginManager] Failed to load {}: {}", path.display(), e),
                }
            }
        }

        if loaded_count == 0 {
            warn!("[PluginManager] No .vtx plugins found in directory.");
        } else {
            info!(
                "[PluginManager] Loaded {} plugins successfully.",
                loaded_count
            );
        }

        Ok(())
    }

    pub async fn load_one(&self, path: &Path) -> anyhow::Result<()> {
        let load_result = loader::load_and_migrate(
            &self.engine,
            &self.registry,
            &self.linker,
            path,
            self.vtx_ffmpeg.clone(),
            self.event_bus.clone(),
        )
        .await?;
        let instance_pre = self.linker.instantiate_pre(&load_result.component)?;

        let runtime = Arc::new(PluginRuntime {
            id: load_result.plugin_id.clone(),
            manifest: load_result.manifest.clone(),
            policy: load_result.policy.clone(),
            vtx_meta: load_result.vtx_meta.clone(),
            instance_pre,
            component: load_result.component,
            source_path: path.to_path_buf(),
        });

        self.register_plugin(runtime)
    }

    fn register_plugin(&self, runtime: Arc<PluginRuntime>) -> anyhow::Result<()> {
        let mut plugins_lock = self.plugins.write().unwrap();
        let mut routes_lock = self.routes.write().unwrap();

        let new_entrypoint = &runtime.manifest.entrypoint;
        let new_id = &runtime.id;

        for existing in routes_lock.iter() {
            if existing.manifest.entrypoint == *new_entrypoint && existing.id != *new_id {
                return Err(anyhow::anyhow!(
                    "Route conflict: '{}' is already owned by plugin '{}'. Installation of '{}' aborted.",
                    new_entrypoint,
                    existing.id,
                    new_id
                ));
            }
        }

        // 原子替换：如果是 Modify 事件触发的重载，这里会直接覆盖旧的 Arc
        plugins_lock.insert(new_id.clone(), runtime.clone());

        routes_lock.retain(|p| p.id != *new_id);
        routes_lock.push(runtime.clone());
        routes_lock.sort_by(|a, b| {
            b.manifest
                .entrypoint
                .len()
                .cmp(&a.manifest.entrypoint.len())
        });

        info!(
            "[Register] Plugin '{}' registered at route '{}'",
            new_id, new_entrypoint
        );

        let topics = runtime.policy.subscriptions.clone();
        if !topics.is_empty() {
            let bus = self.event_bus.clone();
            let runtime = runtime.clone();
            let engine = self.engine.clone();
            let registry = self.registry.clone();
            let vtx_ffmpeg = self.vtx_ffmpeg.clone();
            let max_buffer = self.max_buffer_read_bytes;
            let max_memory = self.max_memory_bytes;

            tokio::spawn(async move {
                let Some(mut rx) = bus
                    .register_plugin(&runtime.id, &topics, &runtime.policy.subscriptions)
                    .await
                else {
                    return;
                };
                while let Some(event) = rx.recv().await {
                    if let Err(e) = PluginExecutor::dispatch_event_with(
                        EventDispatchContext {
                            engine: engine.clone(),
                            registry: registry.clone(),
                            vtx_ffmpeg: vtx_ffmpeg.clone(),
                            event_bus: bus.clone(),
                            max_memory_bytes: max_memory,
                            max_buffer_read_bytes: max_buffer,
                        },
                        runtime.clone(),
                        event,
                    )
                    .await
                    {
                        tracing::error!("[EventBus] Dispatch failed for '{}': {}", runtime.id, e);
                    }
                }
            });
        }

        Ok(())
    }

    pub fn match_route(&self, path: &str) -> Option<(Arc<PluginRuntime>, String)> {
        let routes = self.routes.read().unwrap();
        for plugin in routes.iter() {
            let prefix = &plugin.manifest.entrypoint;
            if path.starts_with(prefix) {
                let rest = &path[prefix.len()..];
                if rest.is_empty() || rest.starts_with('/') {
                    return Some((plugin.clone(), rest.to_string()));
                }
            }
        }
        None
    }

    pub fn uninstall_by_path(&self, path: &Path) {
        let target_id = {
            let plugins = self.plugins.read().unwrap();
            plugins
                .values()
                .find(|p| p.source_path == path)
                .map(|p| p.id.clone())
        };

        if let Some(id) = target_id {
            info!(
                "[Watcher] Detected removal of '{}', uninstalling plugin '{}'...",
                path.display(),
                id
            );
            if let Err(e) = self.uninstall(&id, true) {
                error!("[Watcher] Failed to uninstall plugin '{}': {}", id, e);
            }
        }
    }

    pub fn uninstall(&self, plugin_id: &str, keep_data: bool) -> anyhow::Result<()> {
        // 禁止卸载核心鉴权插件。即使文件被删除，内存中也必须保留该插件。
        if let Some(auth_id) = &self.auth_provider {
            if auth_id == plugin_id {
                warn!(
                    "[Protection] Uninstall blocked for auth_provider '{}'. \
                     System requires this plugin to remain active. \
                     Use file replacement (Atomic Move/Copy) to update it.",
                    plugin_id
                );
                return Err(anyhow::anyhow!(
                    "Operation denied: Cannot uninstall the active auth_provider."
                ));
            }
        }

        {
            let mut plugins_lock = self.plugins.write().unwrap();
            if plugins_lock.remove(plugin_id).is_none() {
                return Err(anyhow::anyhow!("Plugin not found: {}", plugin_id));
            }
            let mut routes_lock = self.routes.write().unwrap();
            routes_lock.retain(|p| p.id != plugin_id);
        }

        if !keep_data {
            self.registry.nuke_plugin(plugin_id)?;
            self.registry.release_installation(plugin_id)?;
        }

        let bus = self.event_bus.clone();
        let plugin_id_owned = plugin_id.to_string();
        let plugin_id_log = plugin_id_owned.clone();
        tokio::spawn(async move {
            bus.unregister_plugin(&plugin_id_owned).await;
        });

        info!("[Uninstall] Plugin '{}' uninstalled.", plugin_id_log);
        Ok(())
    }

    /// 获取所有已加载插件的状态列表
    pub fn list_plugins(&self) -> Vec<PluginStatus> {
        let plugins = self.plugins.read().unwrap();
        plugins
            .values()
            .map(|p| PluginStatus {
                id: p.id.clone(),
                name: p.manifest.name.clone(),
                version: p.manifest.version.clone(),
                entrypoint: p.manifest.entrypoint.clone(),
                source_path: p.source_path.to_string_lossy().to_string(),
                vtx_meta: p.vtx_meta.clone(),
            })
            .collect()
    }

    pub async fn verify_identity(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<UserContext, u16> {
        let wit_headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // 性能优化：O(1) 精确查找
        if let Some(provider_id) = &self.auth_provider {
            let runtime = {
                let plugins = self.plugins.read().unwrap();
                plugins.get(provider_id).cloned()
            };
            if let Some(runtime) = runtime {
                match self.invoke_authenticate(&runtime, &wit_headers).await {
                    Ok(user) => return Ok(user),
                    Err(code) => return Err(code),
                }
            } else {
                // 理论上由 new() 和 uninstall() 的保护机制，此处不应到达
                // 但为了防御性编程，保留此错误分支
                error!(
                    "[Auth] Critical: auth_provider '{}' missing at runtime!",
                    provider_id
                );
                return Err(500);
            }
        } else {
            // 默认模式：责任链遍历
            let plugins: Vec<Arc<PluginRuntime>> =
                { self.plugins.read().unwrap().values().cloned().collect() };

            for plugin_runtime in plugins {
                match self
                    .invoke_authenticate(&plugin_runtime, &wit_headers)
                    .await
                {
                    Ok(user) => return Ok(user),
                    Err(code) => {
                        // 401/403 表示该插件无法处理或拒绝，继续尝试下一个
                        if code == 401 || code == 403 {
                            continue;
                        }
                        return Err(code);
                    }
                }
            }
        }
        Err(401)
    }

    async fn invoke_authenticate(
        &self,
        runtime: &PluginRuntime,
        headers: &[(String, String)],
    ) -> Result<UserContext, u16> {
        let limits = wasmtime::StoreLimitsBuilder::new()
            .instances(1)
            .memory_size(10 * 1024 * 1024)
            .build();

        let ctx = StreamContext::new_secure(StreamContextConfig {
            registry: self.registry.clone(),
            vtx_ffmpeg: self.vtx_ffmpeg.clone(),
            limiter: limits,
            policy: SecurityPolicy::Restricted,
            plugin_id: Some(runtime.id.clone()),
            max_buffer_read_bytes: self.max_buffer_read_bytes,
            current_user: None,
            event_bus: self.event_bus.clone(),
            permissions: runtime.policy.permissions.iter().cloned().collect(),
            http_allowlist: runtime.policy.http.clone(),
        });
        let mut store = wasmtime::Store::new(&self.engine, ctx);
        store.limiter(|s| &mut s.limiter);

        let instance = runtime
            .instance_pre
            .instantiate_async(&mut store)
            .await
            .map_err(|e| {
                error!("[Auth] Instantiation failed: {}", e);
                500u16
            })?;

        let plugin = Plugin::new(&mut store, &instance).map_err(|_| 500u16)?;

        plugin
            .call_authenticate(&mut store, headers)
            .await
            .map_err(|e| {
                error!("[Auth] Call failed: {}", e);
                500u16
            })?
    }
}
