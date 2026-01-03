use serde_json::json;
use vtx_core::runtime::job_registry::{normalize_payload, validate_job_submission};

#[test]
fn normalize_payload_migrates_scan_directory_v0() {
    let payload = json!({ "directory": "C:/media" });
    let (normalized, version) = normalize_payload("scan-directory", &payload, 0).unwrap();
    assert_eq!(version, 1);
    assert_eq!(
        normalized.get("path").and_then(|value| value.as_str()),
        Some("C:/media")
    );
}

#[test]
fn validate_job_submission_rejects_missing_group() {
    let payload = json!({ "path": "C:/media" });
    let groups = vec!["user".to_string()];
    let err = validate_job_submission("scan-directory", &payload, Some(&groups), 1)
        .expect_err("expected permission denied");
    assert!(err.contains("permission denied"));
}

#[test]
fn normalize_payload_rejects_long_path() {
    let payload = json!({ "path": "a".repeat(2049) });
    let err = normalize_payload("scan-directory", &payload, 1).expect_err("expected error");
    assert!(err.contains("too long"));
}
