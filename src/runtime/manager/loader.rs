use crate::runtime::{
    context::{SecurityPolicy, StreamContext, StreamContextConfig},
    ffmpeg::VtxFfmpegManager,
    manager::migration_policy,
    vtx_host_impl::VtxPlugin,
};
use crate::storage::VtxVideoRegistry;
use crate::vtx_vfs::VtxVfsManager;
use anyhow::Context;
use std::sync::Arc;
use tracing::{debug, error, info};
use url::Url;
use wasmtime::{
    component::{Component, Linker},
    Engine,
};

pub struct LoadResult {
    pub plugin_id: String,
    pub manifest: crate::runtime::vtx_host_impl::api::vtx_types::Manifest,
    pub policy: super::PluginPolicy,
    pub vtx_meta: Option<super::VtxPackageMetadata>,
    pub component: Component,
}

pub async fn load_and_migrate(
    engine: &Engine,
    registry: &VtxVideoRegistry,
    linker: &Linker<StreamContext>,
    vtx_uri: &str,
    vtx_ffmpeg: Arc<VtxFfmpegManager>,
    vfs: Arc<VtxVfsManager>,
    event_bus: Arc<crate::runtime::bus::EventBus>,
) -> anyhow::Result<LoadResult> {
    enforce_vtx_only(vtx_uri)?;

    let (component, vtx_meta) = load_component_from_vtx(engine, &vfs, vtx_uri).await?;

    let ctx = StreamContext::new_secure(StreamContextConfig {
        registry: registry.clone(),
        vtx_ffmpeg,
        vfs,
        limiter: wasmtime::StoreLimitsBuilder::new().build(),
        policy: SecurityPolicy::Root,
        plugin_id: None,
        max_buffer_read_bytes: 0,
        current_user: None,
        event_bus,
        permissions: std::collections::HashSet::new(),
        http_allowlist: Vec::new(),
    });
    let mut store = wasmtime::Store::new(engine, ctx);

    let instance = linker.instantiate_async(&mut store, &component).await?;
    let plugin = VtxPlugin::new(&mut store, &instance)?;

    let manifest = plugin.call_get_manifest(&mut store).await?;
    let plugin_id = manifest.id.clone();
    let capabilities = plugin.call_get_capabilities(&mut store).await?;
    let policy = super::PluginPolicy {
        subscriptions: capabilities.subscriptions,
        permissions: capabilities.permissions,
        http: capabilities.http.unwrap_or_default(),
    };

    if !registry.verify_installation(&plugin_id, vtx_uri)? {
        return Err(anyhow::anyhow!(
            "Plugin ID '{}' is already registered with a different path. Installation aborted.",
            plugin_id
        ));
    }

    if let Some(meta) = vtx_meta.as_ref() {
        if let Err(e) = registry.set_plugin_metadata(&plugin_id, meta) {
            tracing::warn!(
                "[plugin/meta] Failed to persist metadata for {}: {}",
                plugin_id,
                e
            );
        }
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

        let mut conn = registry
            .pool
            .get()
            .context("Failed to get database connection")?;
        let tx = conn
            .transaction()
            .context("Failed to start database transaction")?;

        for (idx, sql) in rewritten_migrations.iter().enumerate().skip(current_ver) {
            debug!(
                "[plugin/migration] Executing migration #{} for {}",
                idx + 1,
                &plugin_id
            );

            if let Err(e) = tx.execute(sql, []) {
                error!(
                    "[plugin/migration] Migration failed at step {}: {}. Rolling back transaction.",
                    idx + 1,
                    e
                );

                return Err(anyhow::anyhow!("Migration failed: {}", e));
            }
        }

        tx.commit()
            .context("Failed to commit migration transaction")?;

        registry.set_plugin_version(&plugin_id, rewritten_migrations.len());

        for table_name in normalized_resources {
            registry.register_resource(&plugin_id, "TABLE", &table_name);
            info!(
                "[plugin/resource] Registered table resource: {}",
                table_name
            );
        }

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
        vtx_meta,
        component,
    })
}

fn enforce_vtx_only(uri: &str) -> anyhow::Result<()> {
    let url = Url::parse(uri).context("Invalid plugin URI")?;
    if !super::is_vtx_path(url.path()) {
        return Err(anyhow::anyhow!("Only .vtx plugin is allowed, got: {}", uri));
    }
    Ok(())
}

async fn load_component_from_vtx(
    engine: &Engine,
    vfs: &VtxVfsManager,
    uri: &str,
) -> anyhow::Result<(Component, Option<super::VtxPackageMetadata>)> {
    let meta = vfs
        .head(uri)
        .await
        .with_context(|| format!("failed to read plugin package metadata: {}", uri))?;
    let bytes = vfs
        .read_range(uri, 0, meta.size)
        .await
        .with_context(|| format!("failed to read plugin package: {}", uri))?;

    let decoded = vtx_format::decode_with_metadata(&bytes)
        .with_context(|| format!("invalid vtx package: {}", uri))?;

    let version = decoded.version;
    let vtx_meta = decoded.metadata.and_then(parse_vtx_metadata_json);
    let component_bytes = decoded.component;

    Component::new(engine, component_bytes)
        .with_context(|| {
            format!(
                "failed to compile component from vtx (version {}): {}",
                version, uri
            )
        })
        .map(|c| (c, vtx_meta))
}

fn parse_vtx_metadata_json(bytes: &[u8]) -> Option<super::VtxPackageMetadata> {
    #[derive(serde::Deserialize)]
    struct Tool {
        name: Option<String>,
        version: Option<String>,
    }

    #[derive(serde::Deserialize)]
    struct Meta {
        author: Option<String>,
        authors: Option<Vec<super::VtxAuthor>>,
        description: Option<String>,
        license: Option<String>,
        homepage: Option<String>,
        repository: Option<String>,
        keywords: Option<Vec<String>>,
        version: Option<String>,
        sdk_version: Option<String>,
        package: Option<String>,
        language: Option<String>,
        tool: Option<Tool>,
    }

    let text = std::str::from_utf8(bytes).ok()?;
    let parsed: Meta = serde_json::from_str(text).ok()?;
    Some(super::VtxPackageMetadata {
        author: parsed.author,
        authors: parsed.authors,
        description: parsed.description,
        license: parsed.license,
        homepage: parsed.homepage,
        repository: parsed.repository,
        keywords: parsed.keywords,
        version: parsed.version,
        sdk_version: parsed.sdk_version,
        package: parsed.package,
        language: parsed.language,
        tool_name: parsed.tool.as_ref().and_then(|t| t.name.clone()),
        tool_version: parsed.tool.as_ref().and_then(|t| t.version.clone()),
    })
}
