use super::VtxVfsManager;

#[test]
fn normalize_uri_strips_dot_segments() {
    let manager = VtxVfsManager::new().unwrap();
    let normalized = manager.normalize_uri("file:///tmp/../var//log/./").unwrap();
    assert_eq!(normalized, "file:///var/log/");
}

#[test]
fn ensure_prefix_uri_appends_slash() {
    let manager = VtxVfsManager::new().unwrap();
    let normalized = manager.ensure_prefix_uri("file:///var/log").unwrap();
    assert_eq!(normalized, "file:///var/log/");
}

#[test]
fn match_allowed_prefix_accepts_matching_root() {
    let manager = VtxVfsManager::new().unwrap();
    let allowed = vec!["file:///var/log".to_string()];
    let matched = manager
        .match_allowed_prefix("file:///var/log/app/a.txt", &allowed)
        .unwrap();
    assert_eq!(matched, "file:///var/log/app/a.txt");
}

#[test]
fn match_allowed_prefix_rejects_without_roots() {
    let manager = VtxVfsManager::new().unwrap();
    let err = manager
        .match_allowed_prefix("file:///var/log/app/a.txt", &[])
        .unwrap_err();
    assert_eq!(err, "Scan roots not configured");
}
