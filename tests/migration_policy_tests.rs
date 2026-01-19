use std::collections::HashSet;
use vtx_core::runtime::manager::migration_policy;

fn declared_set(plugin_id: &str, names: &[&str]) -> HashSet<String> {
    let declared = names.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let normalized =
        migration_policy::normalize_declared_resources(plugin_id, declared).expect("normalize");
    normalized.into_iter().collect()
}

#[test]
fn normalize_declared_resources_dedup_and_prefix() {
    let normalized = migration_policy::normalize_declared_resources(
        "p1",
        vec![
            "items".to_string(),
            "items".to_string(),
            "vtx_plugin_p1_logs".to_string(),
        ],
    )
    .expect("normalize");

    assert_eq!(
        normalized,
        vec!["vtx_plugin_p1_items", "vtx_plugin_p1_logs"]
    );
}

#[test]
fn validate_rewrite_create_table() {
    let declared = declared_set("p1", &["items"]);
    let rewritten = migration_policy::validate_and_rewrite_migration(
        "p1",
        &declared,
        "CREATE TABLE items (id INTEGER)",
    )
    .expect("rewrite");

    assert_eq!(rewritten, "CREATE TABLE vtx_plugin_p1_items (id INTEGER)");
}

#[test]
fn validate_rewrite_create_table_with_quotes() {
    let declared = declared_set("p1", &["items"]);
    let rewritten = migration_policy::validate_and_rewrite_migration(
        "p1",
        &declared,
        "CREATE TABLE \"items\" (id INTEGER)",
    )
    .expect("rewrite");

    assert_eq!(
        rewritten,
        "CREATE TABLE \"vtx_plugin_p1_items\" (id INTEGER)"
    );
}

#[test]
fn validate_rewrite_create_index() {
    let declared = declared_set("p1", &["items"]);
    let rewritten = migration_policy::validate_and_rewrite_migration(
        "p1",
        &declared,
        "CREATE INDEX idx_items ON items(id)",
    )
    .expect("rewrite");

    assert_eq!(
        rewritten,
        "CREATE INDEX vtx_plugin_p1_idx_items ON vtx_plugin_p1_items(id)"
    );
}

#[test]
fn validate_rewrite_create_unique_index() {
    let declared = declared_set("p1", &["items"]);
    let rewritten = migration_policy::validate_and_rewrite_migration(
        "p1",
        &declared,
        "CREATE UNIQUE INDEX idx_items ON items(id)",
    )
    .expect("rewrite");

    assert_eq!(
        rewritten,
        "CREATE UNIQUE INDEX vtx_plugin_p1_idx_items ON vtx_plugin_p1_items(id)"
    );
}

#[test]
fn validate_rewrite_alter_table() {
    let declared = declared_set("p1", &["items"]);
    let rewritten = migration_policy::validate_and_rewrite_migration(
        "p1",
        &declared,
        "ALTER TABLE items ADD COLUMN name TEXT",
    )
    .expect("rewrite");

    assert_eq!(
        rewritten,
        "ALTER TABLE vtx_plugin_p1_items ADD COLUMN name TEXT"
    );
}

#[test]
fn validate_rewrite_drop_index() {
    let declared = declared_set("p1", &["items"]);
    let rewritten =
        migration_policy::validate_and_rewrite_migration("p1", &declared, "DROP INDEX idx_items")
            .expect("rewrite");

    assert_eq!(rewritten, "DROP INDEX vtx_plugin_p1_idx_items");
}

#[test]
fn validate_rejects_undeclared_table() {
    let declared = declared_set("p1", &["items"]);
    assert!(migration_policy::validate_and_rewrite_migration(
        "p1",
        &declared,
        "CREATE TABLE other (id INTEGER)",
    )
    .is_err());
}

#[test]
fn validate_rejects_multiple_statements() {
    let declared = declared_set("p1", &["items"]);
    assert!(migration_policy::validate_and_rewrite_migration(
        "p1",
        &declared,
        "CREATE TABLE items (id INTEGER); DROP TABLE items;",
    )
    .is_err());
}

#[test]
fn migration_policy_e2e_executes_rewritten_sql() {
    let declared = declared_set("p1", &["items"]);
    let migrations = [
        "CREATE TABLE items (id INTEGER);",
        "CREATE INDEX idx_items ON items(id);",
    ];
    let rewritten = migrations
        .iter()
        .map(|sql| {
            migration_policy::validate_and_rewrite_migration("p1", &declared, sql).expect("rewrite")
        })
        .collect::<Vec<_>>();

    let conn = rusqlite::Connection::open_in_memory().expect("conn");
    for sql in rewritten {
        conn.execute(&sql, []).expect("execute");
    }

    let table_exists: bool = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
            ["vtx_plugin_p1_items"],
            |row| row.get::<_, String>(0),
        )
        .map(|_| true)
        .unwrap_or(false);
    assert!(table_exists);

    let index_exists: bool = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?1",
            ["vtx_plugin_p1_idx_items"],
            |row| row.get::<_, String>(0),
        )
        .map(|_| true)
        .unwrap_or(false);
    assert!(index_exists);
}
