pub mod loader;
pub mod watcher;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tracing::{error, info, warn};
use wasmtime::component::{Component, InstancePre, Linker};
use wasmtime::Engine;

use crate::runtime::context::{SecurityPolicy, StreamContext};
use crate::runtime::host_impl::api::auth_types::UserContext;
use crate::runtime::host_impl::api::types::Manifest;
use crate::runtime::host_impl::Plugin;
use crate::storage::VideoRegistry;

pub struct PluginRuntime {
    pub id: String,
    pub manifest: Manifest,
    pub instance_pre: InstancePre<StreamContext>,
    #[allow(dead_code)]
    pub component: Component,
    pub source_path: PathBuf,
}

#[derive(Clone)]
pub struct PluginManager {
    engine: Engine,
    linker: Linker<StreamContext>,
    pub plugin_dir: PathBuf,
    registry: VideoRegistry,
    plugins: Arc<RwLock<HashMap<String, Arc<PluginRuntime>>>>,
    routes: Arc<RwLock<Vec<Arc<PluginRuntime>>>>,
}

impl PluginManager {
    pub fn new(
        engine: Engine,
        mut plugin_dir: PathBuf,
        registry: VideoRegistry,
        linker: Linker<StreamContext>,
    ) -> anyhow::Result<Self> {
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
        };

        manager.load_all_plugins()?;

        watcher::spawn_watcher(manager.clone());

        Ok(manager)
    }

    fn load_all_plugins(&self) -> anyhow::Result<()> {
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
            if path.extension().map_or(false, |e| e == "vtx") {
                match self.load_one(&path) {
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

    pub fn load_one(&self, path: &Path) -> anyhow::Result<()> {
        let load_result =
            loader::load_and_migrate(&self.engine, &self.registry, &self.linker, path)?;
        let instance_pre = self.linker.instantiate_pre(&load_result.component)?;

        let runtime = Arc::new(PluginRuntime {
            id: load_result.plugin_id.clone(),
            manifest: load_result.manifest.clone(),
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

        info!("[Uninstall] Plugin '{}' uninstalled.", plugin_id);
        Ok(())
    }

    pub fn verify_identity(&self, headers: &axum::http::HeaderMap) -> Result<UserContext, u16> {
        let wit_headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        let plugins: Vec<Arc<PluginRuntime>> =
            { self.plugins.read().unwrap().values().cloned().collect() };

        for plugin_runtime in plugins {
            match self.invoke_authenticate(&plugin_runtime, &wit_headers) {
                Ok(user) => return Ok(user),
                Err(code) => {
                    if code == 401 || code == 403 {
                        continue;
                    }
                }
            }
        }
        Err(401)
    }

    fn invoke_authenticate(
        &self,
        runtime: &PluginRuntime,
        headers: &[(String, String)],
    ) -> Result<UserContext, u16> {
        let limits = wasmtime::StoreLimitsBuilder::new()
            .instances(1)
            .memory_size(10 * 1024 * 1024)
            .build();

        let ctx =
            StreamContext::new_secure(self.registry.clone(), limits, SecurityPolicy::Restricted);
        let mut store = wasmtime::Store::new(&self.engine, ctx);
        store.limiter(|s| &mut s.limiter);

        let instance = runtime.instance_pre.instantiate(&mut store).map_err(|e| {
            error!("[Auth] Instantiation failed: {}", e);
            500u16
        })?;

        let plugin = Plugin::new(&mut store, &instance).map_err(|_| 500u16)?;

        plugin
            .call_authenticate(&mut store, headers)
            .map_err(|e| {
                error!("[Auth] Call failed: {}", e);
                500u16
            })?
            .map_err(|code| code)
    }
}
