use notify::{Event, RecursiveMode, Watcher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{error, info, warn};
use wasmtime::{
    component::{Component, InstancePre, Linker},
    Engine,
};

use super::loader;
use crate::runtime::context::StreamContext;
use crate::storage::registry::VideoRegistry;

/// 热更新上下文，包含插件引擎、链接器、注册表及共享状态
pub struct HotReloadContext {
    pub engine: Engine,
    pub linker: Linker<StreamContext>,
    pub registry: VideoRegistry,
    pub wasm_path: PathBuf,
    // 共享状态，用于支持热更新替换
    pub component: Arc<RwLock<Component>>,
    pub instance_pre: Arc<RwLock<InstancePre<StreamContext>>>,
    pub current_id: Arc<RwLock<String>>,
}

/// 启动监听线程，监控插件文件变更
pub fn spawn_watcher(ctx: HotReloadContext) {
    std::thread::spawn(move || {
        let path = ctx.wasm_path.clone();
        let (tx, rx) = std::sync::mpsc::channel();

        let mut watcher = match notify::recommended_watcher(tx) {
            Ok(w) => w,
            Err(e) => {
                error!("[HotReload] Failed to create file watcher: {}", e);
                return;
            }
        };

        let parent = path.parent().unwrap_or(Path::new("."));
        if let Err(e) = watcher.watch(parent, RecursiveMode::NonRecursive) {
            error!("[HotReload] Failed to watch directory: {}", e);
            return;
        }

        info!("[HotReload] Watching plugin file: {:?}", path);

        // 监听文件事件
        for res in rx {
            match res {
                Ok(Event { kind, paths, .. }) => {
                    // 判断是否目标插件文件发生变动
                    if paths.iter().any(|p| p.file_name() == path.file_name()) {
                        handle_change(&ctx, &path, &kind);
                    }
                }
                Err(e) => error!("[HotReload] File watch error: {}", e),
            }
        }
    });
}

/// 处理插件文件变更事件，执行热更新
fn handle_change(ctx: &HotReloadContext, path: &Path, kind: &notify::EventKind) {
    if !path.exists() {
        warn!("[HotReload] Plugin file not found. Skipping reload.");
        return;
    }

    if kind.is_modify() || kind.is_create() {
        info!("[HotReload] Change detected. Attempting to reload plugin...");

        // 等待写入完成，避免读取到未完整写入的文件
        std::thread::sleep(Duration::from_millis(200));

        // 加载新插件并执行迁移
        match loader::load_and_migrate(&ctx.engine, &ctx.registry, &ctx.linker, path) {
            Ok(result) => {
                match ctx.linker.instantiate_pre(&result.component) {
                    Ok(new_pre) => {
                        // 更新热更新上下文的共享状态
                        *ctx.component.write().unwrap() = result.component;
                        *ctx.instance_pre.write().unwrap() = new_pre;
                        *ctx.current_id.write().unwrap() = result.plugin_id.clone();

                        info!(
                            "[HotReload] Plugin reloaded successfully: {}",
                            result.plugin_id
                        );
                    }
                    Err(e) => error!("[HotReload] Failed to link plugin instance: {}", e),
                }
            }
            Err(e) => error!("[HotReload] Failed to load/migrate plugin: {}", e),
        }
    }
}
