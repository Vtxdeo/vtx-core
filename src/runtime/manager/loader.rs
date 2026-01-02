use crate::runtime::{
    context::{SecurityPolicy, StreamContext},
    ffmpeg::VtxFfmpegManager,
    host_impl::Plugin,
    manager::migration_policy,
};
use crate::storage::VideoRegistry;
use anyhow::Context;
use configparser::ini::Ini;
use serde_json;
use std::path::Path;
use std::sync::Arc;
use toml;
use tracing::{debug, error, info};
use wasmtime::{
    component::{Component, Linker},
    Engine,
};

/// 表示插件加载的结果，包括插件 ID、Manifest 和已编译的组件
pub struct LoadResult {
    pub plugin_id: String,
    pub manifest: crate::runtime::host_impl::api::types::Manifest,
    pub policy: super::PluginPolicy,
    pub component: Component,
}

/// 加载并迁移插件：
/// - 仅支持 `.vtx` 包
/// - 解包 `.vtx` -> component bytes（通过 vtx-format）
/// - 编译 component -> 实例化 -> 获取元数据 -> 校验路径 -> 执行迁移 -> 注册资源
///
/// IO 复杂度：涉及文件读取、Wasm 编译及多次数据库交互，需注意 I/O 延迟。
pub async fn load_and_migrate(
    engine: &Engine,
    registry: &VideoRegistry,
    linker: &Linker<StreamContext>,
    vtx_path: &Path,
    vtx_ffmpeg: Arc<VtxFfmpegManager>,
    event_bus: Arc<crate::runtime::bus::EventBus>,
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
        0,
        None,
        event_bus,
        std::collections::HashSet::new(),
    );
    let mut store = wasmtime::Store::new(engine, ctx);

    let instance = linker.instantiate_async(&mut store, &component).await?;
    let plugin = Plugin::new(&mut store, &instance)?;

    let manifest = plugin.call_get_manifest(&mut store).await?;
    let plugin_id = manifest.id.clone();
    let policy = parse_manifest_policy(&manifest.description)
        .map_err(|e| anyhow::anyhow!("Invalid manifest policy: {}", e))?;

    if !registry.verify_installation(&plugin_id, vtx_path)? {
        return Err(anyhow::anyhow!(
            "Plugin ID '{}' is already registered with a different path. Installation aborted.",
            plugin_id
        ));
    }

    info!(
        "[plugin/init] Plugin loaded: {} (v{}) - {}",
        &plugin_id, manifest.version, manifest.name
    );

    let declared_resources = plugin.call_get_resources(&mut store).await?;
    let normalized_resources =
        migration_policy::normalize_declared_resources(&plugin_id, declared_resources)
            .map_err(|e| anyhow::anyhow!("Invalid resource declaration: {}", e))?;
    let declared_set = normalized_resources
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let migrations = plugin.call_get_migrations(&mut store).await?;
    let mut rewritten_migrations = Vec::with_capacity(migrations.len());
    for sql in migrations {
        let rewritten =
            migration_policy::validate_and_rewrite_migration(&plugin_id, &declared_set, &sql)
                .map_err(|e| anyhow::anyhow!("Migration rejected: {}", e))?;
        rewritten_migrations.push(rewritten);
    }
    let current_ver = registry.get_plugin_version(&plugin_id);

    if rewritten_migrations.len() > current_ver {
        info!(
            "[plugin/migration] Starting DB migration: {} (v{} -> v{})",
            &plugin_id,
            current_ver,
            rewritten_migrations.len()
        );

        // 获取可变连接以启动事务
        let mut conn = registry.get_conn()?;

        // 开启数据库事务，确保迁移操作的原子性
        // 若中途失败，所有已执行的 SQL 将自动回滚，防止数据库处于损坏状态
        let tx = conn
            .transaction()
            .context("Failed to start database transaction")?;

        for (idx, sql) in rewritten_migrations.iter().enumerate().skip(current_ver) {
            debug!(
                "[plugin/migration] Executing migration #{} for {}",
                idx + 1,
                &plugin_id
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
        for table_name in normalized_resources {
            registry.register_resource(&plugin_id, "TABLE", &table_name);
            info!(
                "[plugin/resource] Registered table resource: {}",
                table_name
            );
        }

        // 更新版本号
        registry.set_plugin_version(&plugin_id, rewritten_migrations.len());
        info!(
            "[plugin/migration] Migration complete for plugin: {}",
            &plugin_id
        );
    } else {
        info!(
            "[plugin/migration] No migration needed. Plugin '{}' database is up to date.",
            &plugin_id
        );
    }

    Ok(LoadResult {
        plugin_id,
        manifest,
        policy,
        component,
    })
}

fn parse_manifest_policy(raw: &str) -> Result<super::PluginPolicy, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(super::PluginPolicy::default());
    }

    if let Ok(policy) = parse_policy_toml(raw) {
        return Ok(policy);
    }

    if let Ok(policy) = parse_policy_ini(raw) {
        return Ok(policy);
    }

    Ok(super::PluginPolicy::default())
}

fn parse_policy_toml(raw: &str) -> Result<super::PluginPolicy, String> {
    #[derive(serde::Deserialize)]
    struct Subscriptions {
        topics: Option<Vec<String>>,
    }
    #[derive(serde::Deserialize)]
    struct Capabilities {
        permissions: Option<Vec<String>>,
    }
    #[derive(serde::Deserialize)]
    struct Policy {
        subscriptions: Option<Subscriptions>,
        capabilities: Option<Capabilities>,
    }

    let parsed: Policy = toml::from_str(raw).map_err(|e| e.to_string())?;
    Ok(super::PluginPolicy {
        subscriptions: parsed
            .subscriptions
            .and_then(|s| s.topics)
            .unwrap_or_default(),
        permissions: parsed
            .capabilities
            .and_then(|c| c.permissions)
            .unwrap_or_default(),
    })
}

fn parse_policy_ini(raw: &str) -> Result<super::PluginPolicy, String> {
    let mut ini = Ini::new();
    ini.read(raw.to_string()).map_err(|e| e.to_string())?;
    let mut subscriptions = Vec::new();
    let mut permissions = Vec::new();

    if let Some(value) = ini.get("subscriptions", "topics") {
        subscriptions = parse_list_value(&value);
    }
    if let Some(value) = ini.get("capabilities", "permissions") {
        permissions = parse_list_value(&value);
    }

    Ok(super::PluginPolicy {
        subscriptions,
        permissions,
    })
}

fn parse_list_value(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.starts_with('[') {
        if let Ok(list) = serde_json::from_str::<Vec<String>>(trimmed) {
            return list.into_iter().filter(|s| !s.trim().is_empty()).collect();
        }
    }
    trimmed
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
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
