use crate::runtime::{
    context::{SecurityPolicy, StreamContext},
    host_impl::Plugin,
};
use crate::storage::VideoRegistry;
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

/// 加载并迁移插件：编译 -> 实例化 -> 获取元数据 -> 校验路径 -> 执行迁移 -> 注册资源
pub fn load_and_migrate(
    engine: &Engine,
    registry: &VideoRegistry,
    linker: &Linker<StreamContext>,
    wasm_path: &Path,
) -> anyhow::Result<LoadResult> {
    // 编译插件组件
    let component = Component::from_file(engine, wasm_path)?;

    // 创建临时 Store 实例以提取插件元数据
    // 使用 Root 权限以允许插件执行迁移逻辑
    let ctx = StreamContext::new_secure(
        registry.clone(),
        wasmtime::StoreLimitsBuilder::new().build(),
        SecurityPolicy::Root,
    );
    let mut store = wasmtime::Store::new(engine, ctx);

    let (plugin, _) = Plugin::instantiate(&mut store, &component, linker)?;

    // 提取插件元信息
    let manifest = plugin.call_get_manifest(&mut store)?;
    let plugin_id = manifest.id;

    // 校验插件 ID 与路径的一致性，防止路径冲突
    if !registry.verify_installation(&plugin_id, wasm_path)? {
        return Err(anyhow::anyhow!(
            "Plugin ID '{}' is already registered with a different path. Installation aborted.",
            plugin_id
        ));
    }

    info!(
        "[plugin/init] Plugin loaded: {} (v{}) - {}",
        plugin_id, manifest.version, manifest.name
    );

    // 处理数据库迁移
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

        // 注册资源（如表名）到系统注册表
        for table_name in declared_resources {
            registry.register_resource(&plugin_id, "TABLE", &table_name);
            info!(
                "[plugin/resource] Registered table resource: {}",
                table_name
            );
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
