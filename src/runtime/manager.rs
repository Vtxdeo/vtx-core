use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use notify::{Event, RecursiveMode, Watcher};
use wasmtime::component::{Component, InstancePre, Linker};
use wasmtime::Engine;

use tracing::{debug, error, info, warn};

use crate::runtime::context::StreamContext;
use crate::runtime::host_impl::api::auth_types::UserContext;
use crate::runtime::host_impl::Plugin;
use crate::storage::registry::VideoRegistry;

#[derive(Clone)]
pub struct PluginManager {
    engine: Engine,
    instance_pre: Arc<RwLock<InstancePre<StreamContext>>>,
    component: Arc<RwLock<Component>>,
    linker: Linker<StreamContext>,
    wasm_path: PathBuf,
    registry: VideoRegistry,
    current_id: Arc<RwLock<String>>,
}

impl PluginManager {
    /// 初始化插件管理器，并执行插件加载与数据库迁移
    pub fn new(
        engine: Engine,
        wasm_path: PathBuf,
        registry: VideoRegistry,
        linker: Linker<StreamContext>,
    ) -> anyhow::Result<Self> {
        info!("[PluginManager] Initializing plugin...");
        
        if !wasm_path.exists() {
            return Err(anyhow::anyhow!(
                "Plugin binary not found at: {:?}.\nHint: Please ensure the WASM module is compiled (e.g., 'cargo build --target wasm32-wasip1 --release').",
                wasm_path
            ));
        }

        let component = Component::from_file(&engine, &wasm_path)?;

        // 执行一次性加载任务（迁移、校验）
        let plugin_id = Self::load_and_migrate(&engine, &component, &registry, &wasm_path)?;

        // 生成 InstancePre
        let instance_pre = linker.instantiate_pre(&component)?;

        info!("[PluginManager] Plugin loaded: {}", plugin_id);

        let manager = Self {
            engine: engine.clone(),
            instance_pre: Arc::new(RwLock::new(instance_pre)),
            component: Arc::new(RwLock::new(component)),
            linker,
            wasm_path: wasm_path.clone(),
            registry,
            current_id: Arc::new(RwLock::new(plugin_id)),
        };

        manager.start_watcher();
        Ok(manager)
    }

    pub fn get_instance_pre(&self) -> InstancePre<StreamContext> {
        self.instance_pre.read().unwrap().clone()
    }
    
    #[allow(dead_code)]
    pub fn get_component(&self) -> Component {
        self.component.read().unwrap().clone()
    }

    pub fn get_plugin_id(&self) -> String {
        self.current_id.read().unwrap().clone()
    }

    /// 调用插件进行身份验证
    ///
    /// 前置条件：HTTP Header 已被收集。
    /// 资源消耗：每次调用会实例化一个新的 Wasm 实例，注意性能影响。
    pub fn verify_identity(&self, headers: &axum::http::HeaderMap) -> Result<UserContext, u16> {
        let wit_headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        let engine = self.engine.clone();
        let registry = self.registry.clone();

        let mut linker = wasmtime::component::Linker::new(&engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker).ok();
        crate::runtime::host_impl::api::stream_io::add_to_linker(
            &mut linker,
            |ctx: &mut StreamContext| ctx,
        )
        .ok();
        crate::runtime::host_impl::api::sql::add_to_linker(
            &mut linker,
            |ctx: &mut StreamContext| ctx,
        )
        .ok();

        let ctx = StreamContext::new_secure(
            registry,
            wasmtime::StoreLimitsBuilder::new().instances(5).build(),
        );
        let mut store = wasmtime::Store::new(&engine, ctx);
        let component = self.component.read().unwrap().clone();

        let (plugin, _) = Plugin::instantiate(&mut store, &component, &linker).map_err(|e| {
            error!("Plugin instantiation failed during authentication: {}", e);
            500u16
        })?;

        plugin
            .call_authenticate(&mut store, &wit_headers)
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
        crate::runtime::host_impl::api::stream_io::add_to_linker(
            &mut linker,
            |ctx: &mut StreamContext| ctx,
        )?;
        crate::runtime::host_impl::api::sql::add_to_linker(
            &mut linker,
            |ctx: &mut StreamContext| ctx,
        )?;

        let ctx = StreamContext::new_secure(
            registry.clone(),
            wasmtime::StoreLimitsBuilder::new().build(),
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

        info!(
            "[Plugin] Detected plugin: {} (v{}) - {}",
            plugin_id, manifest.version, manifest.name
        );

        let declared_resources = plugin.call_get_resources(&mut store)?;
        let migrations = plugin.call_get_migrations(&mut store)?;
        let current_ver = registry.get_plugin_version(&plugin_id);

        if migrations.len() > current_ver {
            info!(
                "[PluginDB] Migrating plugin database: {} (v{} -> v{})",
                plugin_id,
                current_ver,
                migrations.len()
            );
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
        let instance_pre_lock = self.instance_pre.clone();
        let current_id_lock = self.current_id.clone();

        let registry = self.registry.clone();
        let linker = self.linker.clone();

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
                                        match Self::load_and_migrate(
                                            &engine,
                                            &new_component,
                                            &registry,
                                            &path,
                                        ) {
                                            Ok(new_id) => {
                                                match linker.instantiate_pre(&new_component) {
                                                    Ok(new_pre) => {
                                                        *component_lock.write().unwrap() =
                                                            new_component;
                                                        *instance_pre_lock.write().unwrap() =
                                                            new_pre;
                                                        *current_id_lock.write().unwrap() =
                                                            new_id.clone();
                                                        info!("[HotReload] Reload complete: {}", new_id);
                                                    }
                                                    Err(e) => {
                                                        error!(
                                                            "[HotReload] Link failed during reload: {}",
                                                            e
                                                        );
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                error!(
                                                    "[HotReload] Migration failed during reload: {}",
                                                    e
                                                );
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
        warn!(
            "[Uninstall] Uninstalling plugin: {} (keep data: {})",
            plugin_id, keep_data
        );

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
