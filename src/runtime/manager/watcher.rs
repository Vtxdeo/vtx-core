use notify::{Event, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

use super::PluginManager;

/// 启动监听线程，监控插件目录变更
///
/// 该实现采用了基于时间窗口的防抖动机制（Debounce）：
/// 1. 聚合短时间内的多次 Modify 事件，避免重复重载。
/// 2. 只有当文件在指定时间窗口内没有新的变更时，才触发加载逻辑。
/// 3. Remove 事件拥有最高优先级，会立即取消挂起的重载任务。
pub fn spawn_watcher(manager: PluginManager) {
    std::thread::spawn(move || {
        let (tx, rx) = std::sync::mpsc::channel();

        let mut watcher = match notify::recommended_watcher(tx) {
            Ok(w) => w,
            Err(e) => {
                error!("[HotReload] Failed to create file watcher: {}", e);
                return;
            }
        };

        if let Err(e) = watcher.watch(&manager.plugin_dir, RecursiveMode::NonRecursive) {
            error!(
                "[HotReload] Failed to watch plugin directory {:?}: {}",
                manager.plugin_dir, e
            );
            return;
        }

        info!(
            "[HotReload] Watching plugin directory: {:?}",
            manager.plugin_dir
        );

        // 防抖动时间窗口：500ms
        let debounce_duration = Duration::from_millis(500);

        let mut pending_events: HashMap<PathBuf, Instant> = HashMap::new();

        loop {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(res) => match res {
                    Ok(event) => handle_fs_event(&mut pending_events, &manager, event),
                    Err(e) => error!("[HotReload] File watch error: {}", e),
                },
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    error!("[HotReload] Watcher channel disconnected. Stopping thread.");
                    break;
                }
            }

            // 检查是否有挂起的任务达到了防抖时间阈值
            process_pending_events(&manager, &mut pending_events, debounce_duration);
        }
    });
}

/// 处理文件系统原始事件，更新挂起队列
fn handle_fs_event(
    pending_events: &mut HashMap<PathBuf, Instant>,
    manager: &PluginManager,
    event: Event,
) {
    for path in event.paths {
        if path.extension().map_or(false, |e| e == "vtx") {
            if event.kind.is_remove() {
                if pending_events.remove(&path).is_some() {
                    info!(
                        "[HotReload] Pending reload cancelled for removed file: {:?}",
                        path
                    );
                }
                manager.uninstall_by_path(&path);
            } else if event.kind.is_create() || event.kind.is_modify() {
                pending_events.insert(path, Instant::now());
            }
        }
    }
}

/// 扫描挂起队列，执行满足条件的重载任务
fn process_pending_events(
    manager: &PluginManager,
    pending_events: &mut HashMap<PathBuf, Instant>,
    debounce: Duration,
) {
    if pending_events.is_empty() {
        return;
    }

    let now = Instant::now();

    let paths_to_reload: Vec<PathBuf> = pending_events
        .iter()
        .filter(|(_, &last_update)| now.duration_since(last_update) > debounce)
        .map(|(path, _)| path.clone())
        .collect();

    for path in paths_to_reload {
        pending_events.remove(&path);

        info!(
            "[HotReload] Change stabilized. Reloading plugin: {:?}",
            path
        );
        if let Err(e) = manager.load_one(&path) {
            error!("[HotReload] Failed to reload plugin {:?}: {}", path, e);
        }
    }
}
