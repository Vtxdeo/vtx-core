use rusqlite::params;
use tempfile::tempdir;
use vtx_core::runtime::manager::VtxPackageMetadata;
use vtx_core::storage::VideoRegistry;

fn make_registry() -> VideoRegistry {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    VideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry")
}

#[test]
fn plugin_version_roundtrip() {
    let registry = make_registry();
    assert_eq!(registry.get_plugin_version("p1"), 0);
    registry.set_plugin_version("p1", 2);
    assert_eq!(registry.get_plugin_version("p1"), 2);
}

#[test]
fn register_and_list_resources() {
    let registry = make_registry();
    registry.register_resource("p1", "TABLE", "vtx_table");
    registry.register_resource("p1", "TABLE", "vtx_table");
    registry.register_resource("p1", "KV", "vtx_kv");

    let mut tables = registry
        .list_plugin_resources("p1", "TABLE")
        .expect("list");
    tables.sort();
    assert_eq!(tables, vec!["vtx_table".to_string()]);
}

#[test]
fn verify_installation_lock_and_release() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");

    let plugin_a = temp_dir.path().join("plugin-a.vtx");
    let plugin_b = temp_dir.path().join("plugin-b.vtx");
    std::fs::write(&plugin_a, "x").expect("write");
    std::fs::write(&plugin_b, "x").expect("write");

    assert!(registry
        .verify_installation("plugin", &plugin_a)
        .expect("verify"));
    assert!(registry
        .verify_installation("plugin", &plugin_a)
        .expect("verify"));
    assert!(!registry
        .verify_installation("plugin", &plugin_b)
        .expect("verify"));

    registry.release_installation("plugin").expect("release");
    assert!(registry
        .verify_installation("plugin", &plugin_b)
        .expect("verify"));
}

#[test]
fn set_metadata_and_nuke_plugin() {
    let registry = make_registry();
    let meta = VtxPackageMetadata {
        author: Some("me".to_string()),
        sdk_version: Some("1".to_string()),
        package: Some("pkg".to_string()),
        language: Some("rust".to_string()),
        tool_name: Some("tool".to_string()),
        tool_version: Some("1".to_string()),
    };

    registry
        .set_plugin_metadata("plugin", &meta)
        .expect("metadata");
    registry.set_plugin_version("plugin", 3);
    registry.register_resource("plugin", "TABLE", "plugin_table");

    let conn = registry.get_conn().expect("conn");
    conn.execute("CREATE TABLE plugin_table (id TEXT)", [])
        .expect("create table");

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sys_plugin_metadata WHERE plugin_id = ?1",
            params!["plugin"],
            |row| row.get(0),
        )
        .expect("count");
    assert_eq!(count, 1);

    drop(conn);
    registry.nuke_plugin("plugin").expect("nuke");

    let resources = registry
        .list_plugin_resources("plugin", "TABLE")
        .expect("list");
    assert!(resources.is_empty());
    assert_eq!(registry.get_plugin_version("plugin"), 0);

    let conn = registry.get_conn().expect("conn");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sys_plugin_metadata WHERE plugin_id = ?1",
            params!["plugin"],
            |row| row.get(0),
        )
        .expect("count");
    assert_eq!(count, 0);
}

#[test]
fn nuke_plugin_skips_invalid_table_names() {
    let registry = make_registry();
    registry.register_resource("plugin", "TABLE", "good_table");
    registry.register_resource("plugin", "TABLE", "bad;table");

    let conn = registry.get_conn().expect("conn");
    conn.execute("CREATE TABLE good_table (id TEXT)", [])
        .expect("create table");

    drop(conn);
    registry.nuke_plugin("plugin").expect("nuke");

    let conn = registry.get_conn().expect("conn");
    let table_exists: bool = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
            ["good_table"],
            |row| row.get::<_, String>(0),
        )
        .map(|_| true)
        .unwrap_or(false);
    assert!(!table_exists);

    drop(conn);
    let resources = registry
        .list_plugin_resources("plugin", "TABLE")
        .expect("list");
    assert!(resources.is_empty());
}
