use crate::runtime::{
    context::{SecurityPolicy, StreamContext},
    host_impl::Plugin,
};
use crate::storage::VideoRegistry;
use anyhow::Context;
use std::path::Path;
use tracing::{debug, error, info};
use wasmtime::{
    component::{Component, Linker},
    Engine,
};

/// 表示插件加载的结果，包括插件 ID 和已编译的组件
pub struct LoadResult {
    pub plugin_id: String,
    pub component: Component,
}

/// 加载并迁移插件：
/// - 仅支持 `.vtx` 包
/// - 解包 `.vtx` -> component bytes（通过 vtx-format）
/// - 编译 component -> 实例化 -> 获取元数据 -> 校验路径 -> 执行迁移 -> 注册资源
pub fn load_and_migrate(
    engine: &Engine,
    registry: &VideoRegistry,
    linker: &Linker<StreamContext>,
    vtx_path: &Path,
) -> anyhow::Result<LoadResult> {
    enforce_vtx_only(vtx_path)?;

    let component = load_component_from_vtx(engine, vtx_path)?;

    // Root 权限允许迁移
    let ctx = StreamContext::new_secure(
        registry.clone(),
        wasmtime::StoreLimitsBuilder::new().build(),
        SecurityPolicy::Root,
    );
    let mut store = wasmtime::Store::new(engine, ctx);

    let (plugin, _) = Plugin::instantiate(&mut store, &component, linker)?;

    let manifest = plugin.call_get_manifest(&mut store)?;
    let plugin_id = manifest.id;

    if !registry.verify_installation(&plugin_id, vtx_path)? {
        return Err(anyhow::anyhow!(
            "Plugin ID '{}' is already registered with a different path. Installation aborted.",
            plugin_id
        ));
    }

    info!(
        "[plugin/init] Plugin loaded: {} (v{}) - {}",
        plugin_id, manifest.version, manifest.name
    );

    let declared_resources = plugin.call_get_resources(&mut store)?;
    let migrations = plugin.call_get_migrations(&mut store)?;
    let current_ver = registry.get_plugin_version(&plugin_id);

    if migrations.len() > current_ver {
        info!(
            "[plugin/migration] Starting DB migration: {} (v{} -> v{})",
            plugin_id,
            current_ver,
            migrations.len()
        );

        let conn = registry.get_conn()?;
        for (idx, sql) in migrations.iter().enumerate().skip(current_ver) {
            debug!(
                "[plugin/migration] Executing migration #{} for {}",
                idx + 1,
                plugin_id
            );
            if let Err(e) = conn.execute(sql, []) {
                error!(
                    "[plugin/migration] Migration failed at step {}: {}",
                    idx + 1,
                    e
                );
                return Err(anyhow::anyhow!("Migration failed: {}", e));
            }
        }

        for table_name in declared_resources {
            registry.register_resource(&plugin_id, "TABLE", &table_name);
            info!("[plugin/resource] Registered table resource: {}", table_name);
        }

        registry.set_plugin_version(&plugin_id, migrations.len());
        info!(
            "[plugin/migration] Migration complete for plugin: {}",
            plugin_id
        );
    } else {
        info!(
            "[plugin/migration] No migration needed. Plugin '{}' database is up to date.",
            plugin_id
        );
    }

    Ok(LoadResult {
        plugin_id,
        component,
    })
}

fn enforce_vtx_only(path: &Path) -> anyhow::Result<()> {
    let ext_ok = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.eq_ignore_ascii_case("vtx"))
        .unwrap_or(false);

    if !ext_ok {
        return Err(anyhow::anyhow!(
            "Only .vtx plugin is allowed, got: {}",
            path.display()
        ));
    }
    Ok(())
}

fn load_component_from_vtx(engine: &Engine, path: &Path) -> anyhow::Result<Component> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read plugin package: {}", path.display()))?;

    let (version, component_bytes) = vtx_format::decode(&bytes)
        .with_context(|| format!("invalid vtx package: {}", path.display()))?;

    Component::new(engine, component_bytes).with_context(|| {
        format!(
            "failed to compile component from vtx (version {}): {}",
            version,
            path.display()
        )
    })
}
