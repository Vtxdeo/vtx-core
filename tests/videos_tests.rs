use std::collections::HashSet;
use tempfile::tempdir;
use vtx_core::storage::VtxVideoRegistry;
use vtx_core::vtx_vfs::VtxVfsManager;

#[tokio::test]
async fn scan_directory_registers_new_videos() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VtxVideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");
    let vfs = VtxVfsManager::new().expect("vfs");

    let root = temp_dir.path().join("media");
    std::fs::create_dir_all(&root).expect("create root");

    let video1 = root.join("video1.mp4");
    let video2 = root.join("video2.mkv");
    let not_video = root.join("note.txt");

    std::fs::write(&video1, "x").expect("write");
    std::fs::write(&video2, "x").expect("write");
    std::fs::write(&not_video, "x").expect("write");

    let root_uri = url::Url::from_directory_path(&root)
        .expect("root uri")
        .to_string();
    let new_videos = registry
        .scan_directory(&vfs, &root_uri)
        .await
        .expect("scan");
    assert_eq!(new_videos.len(), 2);
    let names: HashSet<String> = new_videos.into_iter().map(|v| v.filename).collect();
    assert!(names.contains("video1.mp4"));
    assert!(names.contains("video2.mkv"));

    let second_scan = registry
        .scan_directory(&vfs, &root_uri)
        .await
        .expect("scan");
    assert!(second_scan.is_empty());

    let all = registry.list_all().expect("list");
    assert_eq!(all.len(), 2);
}
