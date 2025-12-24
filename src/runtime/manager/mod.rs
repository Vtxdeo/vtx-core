pub mod loader;
pub mod watcher;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tracing::{error, info};
use wasmtime::component::{Component, InstancePre, Linker};
use wasmtime::Engine;

use crate::runtime::context::{SecurityPolicy, StreamContext};
use crate::runtime::host_impl::api::auth_types::UserContext;
use crate::runtime::host_impl::Plugin;
use crate::storage::registry::VideoRegistry;

/// 插件管理器：负责插件加载、验证调用、热更新与卸载管理
#[derive(Clone)]
pub struct PluginManager {
    engine: Engine,
    instance_pre: Arc<RwLock<InstancePre<StreamContext>>>,
    component: Arc<RwLock<Component>>,
    current_id: Arc<RwLock<String>>,
    linker: Linker<StreamContext>,
    wasm_path: PathBuf,
    registry: VideoRegistry,
}

impl PluginManager {
    /// 初始化插件管理器并加载插件
    pub fn new(
        engine: Engine,
        wasm_path: PathBuf,
        registry: VideoRegistry,
        linker: Linker<StreamContext>,
    ) -> anyhow::Result<Self> {
        info!("[PluginManager] Initializing plugin manager");

        if !wasm_path.exists() {
            return Err(anyhow::anyhow!(
                "Plugin binary not found at path: {:?}",
                wasm_path
            ));
        }

        // 使用 Root 策略加载插件，完成初始化与数据库迁移等操作
        let load_result = loader::load_and_migrate(&engine, &registry, &linker, &wasm_path)?;

        let instance_pre = linker.instantiate_pre(&load_result.component)?;

        info!(
            "[PluginManager] Plugin loaded successfully. ID: {}",
            load_result.plugin_id
        );

        let manager = Self {
            engine: engine.clone(),
            instance_pre: Arc::new(RwLock::new(instance_pre)),
            component: Arc::new(RwLock::new(load_result.component)),
            current_id: Arc::new(RwLock::new(load_result.plugin_id)),
            linker,
            wasm_path,
            registry,
        };

        manager.start_watcher();

        Ok(manager)
    }

    /// 返回预编译实例（用于高性能运行）
    pub fn get_instance_pre(&self) -> InstancePre<StreamContext> {
        self.instance_pre.read().unwrap().clone()
    }

    /// 返回插件组件（原始组件对象）
    #[allow(dead_code)]
    pub fn get_component(&self) -> Component {
        self.component.read().unwrap().clone()
    }

    /// 返回当前插件的唯一 ID
    pub fn get_plugin_id(&self) -> String {
        self.current_id.read().unwrap().clone()
    }

    /// 使用插件进行身份验证逻辑执行
    pub fn verify_identity(&self, headers: &axum::http::HeaderMap) -> Result<UserContext, u16> {
        let wit_headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // 获取预编译实例，避免重复构建
        let instance_pre = self.get_instance_pre();

        // 设置内存资源限制
        let limits = wasmtime::StoreLimitsBuilder::new()
            .instances(1)
            .memory_size(10 * 1024 * 1024) // 限制最大 10MB 内存
            .build();

        // 初始化 Store 并应用限制与安全策略（禁止文件访问和 DB 写）
        let ctx =
            StreamContext::new_secure(self.registry.clone(), limits, SecurityPolicy::Restricted);

        let mut store = wasmtime::Store::new(&self.engine, ctx);
        store.limiter(|s| &mut s.limiter);

        // 实例化插件
        let instance = instance_pre.instantiate(&mut store).map_err(|e| {
            error!("[Auth] Failed to instantiate plugin: {}", e);
            500u16
        })?;

        // 绑定插件接口
        let plugin = Plugin::new(&mut store, &instance).map_err(|e| {
            error!("[Auth] Failed to bind plugin interface: {}", e);
            500u16
        })?;

        // 执行插件身份认证逻辑
        plugin
            .call_authenticate(&mut store, &wit_headers)
            .map_err(|e| {
                error!("[Auth] Plugin authentication invocation failed: {}", e);
                500u16
            })?
            .map_err(|code| code)
    }

    /// 启动热更新文件监听器，监控插件文件变更
    fn start_watcher(&self) {
        let ctx = watcher::HotReloadContext {
            engine: self.engine.clone(),
            linker: self.linker.clone(),
            registry: self.registry.clone(),
            wasm_path: self.wasm_path.clone(),
            component: self.component.clone(),
            instance_pre: self.instance_pre.clone(),
            current_id: self.current_id.clone(),
        };

        watcher::spawn_watcher(ctx);
    }

    /// 卸载插件，根据 `keep_data` 决定是否保留注册表中的相关数据
    pub fn uninstall(&self, keep_data: bool) -> anyhow::Result<()> {
        let plugin_id = self.get_plugin_id();
        let disabled_path = self.wasm_path.with_extension("wasm.disabled");

        if self.wasm_path.exists() {
            std::fs::rename(&self.wasm_path, &disabled_path)?;
        }

        if !keep_data {
            self.registry.nuke_plugin(&plugin_id)?;
            self.registry.release_installation(&plugin_id)?;
        }

        Ok(())
    }
}
