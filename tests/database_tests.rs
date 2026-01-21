use rusqlite::params;
use tempfile::tempdir;
use vtx_core::storage::VtxVideoRegistry;

#[test]
fn initialize_pool_creates_core_tables() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VtxVideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");
    let conn = registry.get_conn().expect("conn");

    let table_exists = |name: &str| -> bool {
        conn.query_row(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![name],
            |row| row.get::<_, String>(0),
        )
        .map(|_| true)
        .unwrap_or(false)
    };

    assert!(table_exists("videos"));
    assert!(table_exists("sys_jobs"));
    assert!(table_exists("sys_plugin_versions"));
}
