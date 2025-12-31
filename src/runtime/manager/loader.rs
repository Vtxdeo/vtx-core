use crate::runtime::{
    context::{SecurityPolicy, StreamContext},
    ffmpeg::VtxFfmpegManager,
    host_impl::Plugin,
};
use crate::storage::VideoRegistry;
use anyhow::Context;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, error, info};
use wasmtime::{
    component::{Component, Linker},
    Engine,
};

/// 表示插件加载的结果，包括插件 ID、Manifest 和已编译的组件
pub struct LoadResult {
    pub plugin_id: String,
    pub manifest: crate::runtime::host_impl::api::types::Manifest,
    pub component: Component,
}

/// 加载并迁移插件：
/// - 仅支持 `.vtx` 包
/// - 解包 `.vtx` -> component bytes（通过 vtx-format）
/// - 编译 component -> 实例化 -> 获取元数据 -> 校验路径 -> 执行迁移 -> 注册资源
///
/// IO 复杂度：涉及文件读取、Wasm 编译及多次数据库交互，需注意 I/O 延迟。
pub fn load_and_migrate(
    engine: &Engine,
    registry: &VideoRegistry,
    linker: &Linker<StreamContext>,
    vtx_path: &Path,
    vtx_ffmpeg: Arc<VtxFfmpegManager>,
) -> anyhow::Result<LoadResult> {
    enforce_vtx_only(vtx_path)?;

    let component = load_component_from_vtx(engine, vtx_path)?;

    // Root 权限允许迁移
    let ctx = StreamContext::new_secure(
        registry.clone(),
        vtx_ffmpeg,
        wasmtime::StoreLimitsBuilder::new().build(),
        SecurityPolicy::Root,
        None,
    );
    let mut store = wasmtime::Store::new(engine, ctx);

    let (plugin, _) = Plugin::instantiate(&mut store, &component, linker)?;

    let manifest = plugin.call_get_manifest(&mut store)?;
    let plugin_id = manifest.id.clone();

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

        // 获取可变连接以启动事务
        let mut conn = registry.get_conn()?;

        // 开启数据库事务，确保迁移操作的原子性
        // 若中途失败，所有已执行的 SQL 将自动回滚，防止数据库处于损坏状态
        let tx = conn
            .transaction()
            .context("Failed to start database transaction")?;

        for (idx, sql) in migrations.iter().enumerate().skip(current_ver) {
            debug!(
                "[plugin/migration] Executing migration #{} for {}",
                idx + 1,
                plugin_id
            );

            // 使用事务句柄执行 SQL
            if let Err(e) = tx.execute(sql, []) {
                error!(
                    "[plugin/migration] Migration failed at step {}: {}. Rolling back transaction.",
                    idx + 1,
                    e
                );
                // 此时直接返回错误，Transaction Drop 时会自动 Rollback
                return Err(anyhow::anyhow!("Migration failed: {}", e));
            }
        }

        // 提交事务
        tx.commit()
            .context("Failed to commit migration transaction")?;

        // 迁移成功后注册资源表
        for table_name in declared_resources {
            registry.register_resource(&plugin_id, "TABLE", &table_name);
            info!(
                "[plugin/resource] Registered table resource: {}",
                table_name
            );
        }

        // 更新版本号
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
        manifest,
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
