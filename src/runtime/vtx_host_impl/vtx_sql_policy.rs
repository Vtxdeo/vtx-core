use crate::runtime::context::{SecurityPolicy, StreamContext};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::Connection;
use std::collections::HashSet;
use std::sync::Arc;

pub struct AuthorizerGuard<'a> {
    conn: &'a Connection,
    active: bool,
}

impl Drop for AuthorizerGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self
                .conn
                .authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
        }
    }
}

pub fn enforce_sql_policy<'a>(
    ctx: &StreamContext,
    conn: &'a Connection,
) -> Result<AuthorizerGuard<'a>, String> {
    if ctx.policy == SecurityPolicy::Root {
        let _ = conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
        return Ok(AuthorizerGuard {
            conn,
            active: false,
        });
    }

    let plugin_id = ctx
        .plugin_id
        .as_ref()
        .ok_or("Permission Denied".to_string())?;

    let allowed_tables = ctx
        .registry
        .list_plugin_resources(plugin_id, "TABLE")
        .map_err(|e| e.to_string())?;
    let allowed_set: HashSet<String> = allowed_tables
        .into_iter()
        .map(|t| t.to_ascii_lowercase())
        .collect();

    let allow_write = match ctx.policy {
        SecurityPolicy::Restricted => false,
        SecurityPolicy::Plugin => ctx.permissions.contains("sql:write"),
        SecurityPolicy::Root => true,
    };

    let config = Arc::new(SqlAuthConfig {
        allow_write,
        allowed_tables: allowed_set,
    });

    let hook_config = Arc::clone(&config);
    let _ = conn.authorizer(Some(move |auth_ctx: AuthContext<'_>| {
        authorize_action(auth_ctx, &hook_config)
    }));

    Ok(AuthorizerGuard { conn, active: true })
}

struct SqlAuthConfig {
    allow_write: bool,
    allowed_tables: HashSet<String>,
}

fn authorize_action(ctx: AuthContext<'_>, config: &SqlAuthConfig) -> Authorization {
    match ctx.action {
        AuthAction::Read { table_name, .. } => {
            authorize_table(table_name, ctx.database_name, config)
        }
        AuthAction::Insert { table_name } => authorize_write_table(table_name, ctx, config),
        AuthAction::Update { table_name, .. } => authorize_write_table(table_name, ctx, config),
        AuthAction::Delete { table_name } => authorize_write_table(table_name, ctx, config),
        AuthAction::Select => Authorization::Allow,
        AuthAction::Function { .. } => Authorization::Allow,
        AuthAction::Recursive => Authorization::Allow,
        AuthAction::Transaction { .. } | AuthAction::Savepoint { .. } => Authorization::Deny,
        _ => Authorization::Deny,
    }
}

fn authorize_write_table(
    table_name: &str,
    ctx: AuthContext<'_>,
    config: &SqlAuthConfig,
) -> Authorization {
    if !config.allow_write {
        return Authorization::Deny;
    }
    authorize_table(table_name, ctx.database_name, config)
}

fn authorize_table(
    table_name: &str,
    database_name: Option<&str>,
    config: &SqlAuthConfig,
) -> Authorization {
    if !is_allowed_database(database_name) {
        return Authorization::Deny;
    }

    let name = table_name.to_ascii_lowercase();
    if name.starts_with("sys_") || name.contains('.') {
        return Authorization::Deny;
    }
    if !config.allowed_tables.contains(&name) {
        return Authorization::Deny;
    }
    Authorization::Allow
}

fn is_allowed_database(database_name: Option<&str>) -> bool {
    matches!(database_name, None | Some("main") | Some("temp"))
}
