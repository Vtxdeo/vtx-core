use std::sync::{Arc, RwLock};
use std::path::{Path, PathBuf};
use std::time::Duration;

use wasmtime::{Engine, component::Component};
use notify::{Watcher, RecursiveMode, Event};

use tracing::{info, error, warn, debug};

use crate::registry::VideoRegistry;
use crate::state::StreamContext;
use crate::host::Plugin;
use crate::host::api::auth_types::UserContext;

#[derive(Clone)]
pub struct PluginManager {
    engine: Engine,
    component: Arc<RwLock<Component>>,
    wasm_path: PathBuf,
    registry: VideoRegistry,
    current_id: Arc<RwLock<String>>,
}

impl PluginManager {
    /// 初始化插件管理器，并执行插件加载与数据库迁移
    pub fn new(engine: Engine, wasm_path: PathBuf, registry: VideoRegistry) -> anyhow::Result<Self> {
        info!("[PluginManager] Initializing plugin...");
        let component = Component::from_file(&engine, &wasm_path)?;

        let plugin_id = Self::load_and_migrate(&engine, &component, &registry, &wasm_path)?;

        info!("[PluginManager] Plugin loaded: {}", plugin_id);

        let manager = Self {
            engine: engine.clone(),
            component: Arc::new(RwLock::new(component)),
            wasm_path: wasm_path.clone(),
            registry,
            current_id: Arc::new(RwLock::new(plugin_id)),
        };

        manager.start_watcher();
        Ok(manager)
    }

    pub fn get_component(&self) -> Component {
        self.component.read().unwrap().clone()
    }

    pub fn get_plugin_id(&self) -> String {
        self.current_id.read().unwrap().clone()
    }

    /// 调用插件进行身份验证
    pub fn verify_identity(&self, headers: &axum::http::HeaderMap) -> Result<UserContext, u16> {
        let wit_headers: Vec<(String, String)> = headers.iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        let engine = self.engine.clone();
        let registry = self.registry.clone();

        let mut linker = wasmtime::component::Linker::new(&engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker).ok();
        crate::host::api::stream_io::add_to_linker(&mut linker, |ctx: &mut StreamContext| ctx).ok();
        crate::host::api::sql::add_to_linker(&mut linker, |ctx: &mut StreamContext| ctx).ok();

        let ctx = StreamContext::new_secure(
            registry,
            wasmtime::StoreLimitsBuilder::new().instances(5).build()
        );
        let mut store = wasmtime::Store::new(&engine, ctx);
        let component = self.component.read().unwrap().clone();

        let (plugin, _) = Plugin::instantiate(&mut store, &component, &linker)
            .map_err(|e| {
                error!("Plugin instantiation failed during authentication: {}", e);
                500u16
            })?;

        plugin.call_authenticate(&mut store, &wit_headers)
            .map_err(|e| {
                error!("Authentication call failed: {}", e);
                500u16
            })?
            .map_err(|code| code)
    }

    /// 加载插件并执行数据库迁移流程
    fn load_and_migrate(
        engine: &Engine,
        component: &Component,
        registry: &VideoRegistry,
        wasm_path: &Path,
    ) -> anyhow::Result<String> {
        let mut linker = wasmtime::component::Linker::new(engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)?;
        crate::host::api::stream_io::add_to_linker(&mut linker, |ctx: &mut StreamContext| ctx)?;
        crate::host::api::sql::add_to_linker(&mut linker, |ctx: &mut StreamContext| ctx)?;

        let ctx = StreamContext::new_secure(
            registry.clone(),
            wasmtime::StoreLimitsBuilder::new().build()
        );
        let mut store = wasmtime::Store::new(engine, ctx);

        let (plugin, _) = Plugin::instantiate(&mut store, component, &linker)?;
        let manifest = plugin.call_get_manifest(&mut store)?;
        let plugin_id = manifest.id;

        if !registry.verify_installation(&plugin_id, wasm_path)? {
            return Err(anyhow::anyhow!(
                "Plugin ID '{}' is already registered with a different path. Installation rejected.",
                plugin_id
            ));
        }

        info!("[Plugin] Detected plugin: {} (v{}) - {}", plugin_id, manifest.version, manifest.name);

        let declared_resources = plugin.call_get_resources(&mut store)?;
        let migrations = plugin.call_get_migrations(&mut store)?;
        let current_ver = registry.get_plugin_version(&plugin_id);

        if migrations.len() > current_ver {
            info!("[PluginDB] Migrating plugin database: {} (v{} -> v{})", plugin_id, current_ver, migrations.len());
            let conn = registry.get_conn()?;
            for (idx, sql) in migrations.iter().enumerate().skip(current_ver) {
                debug!("[PluginDB] Executing migration #{}: {}", idx + 1, sql);
                if let Err(e) = conn.execute(sql, []) {
                    return Err(anyhow::anyhow!("Migration failed: {}", e));
                }
            }

            for table_name in declared_resources {
                registry.register_resource(&plugin_id, "TABLE", &table_name);
                info!("[Audit] Registered resource: {}", table_name);
            }

            registry.set_plugin_version(&plugin_id, migrations.len());
            info!("[PluginDB] Migration complete");
        } else {
            info!("[PluginDB] Database is up to date");
        }

        Ok(plugin_id)
    }

    /// 启动热更新监听器，自动重载插件文件变更
    fn start_watcher(&self) {
        let path = self.wasm_path.clone();
        let engine = self.engine.clone();
        let component_lock = self.component.clone();
        let current_id_lock = self.current_id.clone();
        let registry = self.registry.clone();

        std::thread::spawn(move || {
            let (tx, rx) = std::sync::mpsc::channel();
            let mut watcher = notify::recommended_watcher(tx).unwrap();
            let parent = path.parent().unwrap_or(Path::new("."));

            if let Err(e) = watcher.watch(parent, RecursiveMode::NonRecursive) {
                error!("[HotReload] Failed to watch path: {}", e);
                return;
            }

            info!("[HotReload] Watching plugin file: {:?}", path);

            for res in rx {
                match res {
                    Ok(Event { kind, paths, .. }) => {
                        if paths.iter().any(|p| p.file_name() == path.file_name()) {
                            if !path.exists() {
                                warn!("[HotReload] Plugin file missing. Skipping reload.");
                                continue;
                            }

                            if kind.is_modify() || kind.is_create() {
                                info!("[HotReload] Change detected. Reloading plugin...");
                                std::thread::sleep(Duration::from_millis(200));

                                match Component::from_file(&engine, &path) {
                                    Ok(new_component) => {
                                        match Self::load_and_migrate(&engine, &new_component, &registry, &path) {
                                            Ok(new_id) => {
                                                *component_lock.write().unwrap() = new_component;
                                                *current_id_lock.write().unwrap() = new_id.clone();
                                                info!("[HotReload] Reload complete: {}", new_id);
                                            }
                                            Err(e) => {
                                                error!("[HotReload] Migration failed during reload: {}", e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("[HotReload] Failed to compile plugin: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {}
                }
            }
        });
    }

    /// 卸载插件文件，可选择是否保留数据库数据
    pub fn uninstall(&self, keep_data: bool) -> anyhow::Result<()> {
        let plugin_id = self.get_plugin_id();
        warn!("[Uninstall] Uninstalling plugin: {} (keep data: {})", plugin_id, keep_data);

        let disabled_path = self.wasm_path.with_extension("wasm.disabled");

        if self.wasm_path.exists() {
            std::fs::rename(&self.wasm_path, &disabled_path)?;
            info!("[Uninstall] Plugin file renamed to disable future loading");
        }

        if !keep_data {
            self.registry.nuke_plugin(&plugin_id)?;
            info!("[Uninstall] Plugin database entries removed");
            self.registry.release_installation(&plugin_id)?;
            info!("[Uninstall] Plugin ID lock released");
        }

        Ok(())
    }
}
