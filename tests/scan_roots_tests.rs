use tempfile::tempdir;
use url::Url;
use vtx_core::storage::VtxVideoRegistry;

#[test]
fn add_list_remove_scan_root() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VtxVideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");

    let root_dir = temp_dir.path().join("media");
    std::fs::create_dir_all(&root_dir).expect("create root");
    let root_uri = Url::from_file_path(&root_dir)
        .expect("root uri")
        .to_string();

    let resolved = registry.add_scan_root(&root_uri).expect("add");
    let roots = registry.list_scan_roots().expect("list");
    assert_eq!(roots, vec![resolved.clone()]);

    registry.remove_scan_root(&root_uri).expect("remove");
    let roots = registry.list_scan_roots().expect("list");
    assert!(roots.is_empty());
}

#[test]
fn add_scan_root_rejects_file_path() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VtxVideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");

    let file_path = temp_dir.path().join("not-a-dir.txt");
    std::fs::write(&file_path, "x").expect("write file");
    let file_uri = Url::from_file_path(&file_path)
        .expect("file uri")
        .to_string();

    let err = registry
        .add_scan_root(&file_uri)
        .expect_err("expected error");
    assert!(err.to_string().contains("directory"));
}
