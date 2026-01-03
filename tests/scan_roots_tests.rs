use tempfile::tempdir;
use vtx_core::storage::VideoRegistry;

#[test]
fn add_list_remove_scan_root() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");

    let root_dir = temp_dir.path().join("media");
    std::fs::create_dir_all(&root_dir).expect("create root");

    let resolved = registry.add_scan_root(&root_dir).expect("add");
    let roots = registry.list_scan_roots().expect("list");
    assert_eq!(roots, vec![resolved.clone()]);

    registry.remove_scan_root(&root_dir).expect("remove");
    let roots = registry.list_scan_roots().expect("list");
    assert!(roots.is_empty());
}

#[test]
fn add_scan_root_rejects_file_path() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");

    let file_path = temp_dir.path().join("not-a-dir.txt");
    std::fs::write(&file_path, "x").expect("write file");

    let err = registry
        .add_scan_root(&file_path)
        .expect_err("expected error");
    assert!(err.to_string().contains("directory"));
}
