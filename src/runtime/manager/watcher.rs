use std::collections::HashMap;
use std::time::Duration;
use tokio::runtime::Handle;
use tracing::{error, info};

use futures_util::StreamExt;
use super::{is_vtx_uri, PluginManager};

/// Starts a polling watcher that scans the VFS plugin root on an interval.
pub fn spawn_watcher(manager: PluginManager, handle: Handle) {
    handle.spawn(async move {
        info!("[HotReload] Polling plugin root: {}", manager.plugin_root);

        let mut known: HashMap<String, ObjectSignature> = HashMap::new();
        let mut ticker = tokio::time::interval(Duration::from_millis(2000));

        loop {
            ticker.tick().await;
            if let Err(e) = scan_once(&manager, &mut known).await {
                error!("[HotReload] Polling error: {}", e);
            }
        }
    });
}

#[derive(Clone, Debug, PartialEq)]
struct ObjectSignature {
    size: u64,
    last_modified: Option<i64>,
    etag: Option<String>,
}

async fn scan_once(
    manager: &PluginManager,
    known: &mut HashMap<String, ObjectSignature>,
) -> anyhow::Result<()> {
    let mut entries = manager.vfs.list_objects(&manager.plugin_root).await?;
    let mut current: HashMap<String, ObjectSignature> = HashMap::new();
    let mut changed: Vec<String> = Vec::new();

    while let Some(item) = entries.next().await {
        match item {
            Ok(obj) => {
                if !is_vtx_uri(&obj.uri) {
                    continue;
                }
                let sig = ObjectSignature {
                    size: obj.size,
                    last_modified: obj.last_modified,
                    etag: obj.etag.clone(),
                };
                if known.get(&obj.uri).map(|prev| prev != &sig).unwrap_or(true) {
                    changed.push(obj.uri.clone());
                }
                current.insert(obj.uri, sig);
            }
            Err(e) => {
                error!("[HotReload] Failed to list object: {}", e);
            }
        }
    }

    let mut removed = Vec::new();
    for uri in known.keys() {
        if !current.contains_key(uri) {
            removed.push(uri.clone());
        }
    }
    *known = current;

    for uri in removed {
        manager.uninstall_by_uri(&uri);
    }

    for uri in changed {
        let manager = manager.clone();
        tokio::spawn(async move {
            if let Err(e) = manager.load_one(&uri).await {
                error!("[HotReload] Failed to reload plugin {}: {}", uri, e);
            }
        });
    }

    Ok(())
}
