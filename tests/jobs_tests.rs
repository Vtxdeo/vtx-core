use tempfile::tempdir;
use vtx_core::storage::VtxVideoRegistry;

fn make_registry() -> (tempfile::TempDir, VtxVideoRegistry) {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VtxVideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");
    (temp_dir, registry)
}

#[test]
fn enqueue_and_get_job() {
    let (_temp_dir, registry) = make_registry();
    let job_id = registry.enqueue_job("noop", "{}", 1, 2).expect("enqueue");

    let job = registry.get_job(&job_id).expect("get").expect("job");
    assert_eq!(job.job_type, "noop");
    assert_eq!(job.payload, "{}");
    assert_eq!(job.payload_version, 1);
    assert_eq!(job.status, "queued");
}

#[test]
fn claim_and_complete_job() {
    let (_temp_dir, registry) = make_registry();
    let first = registry.enqueue_job("scan", "{}", 1, 0).expect("enqueue");
    let _second = registry.enqueue_job("scan", "{}", 1, 0).expect("enqueue");

    let claimed = registry
        .claim_next_job("worker-1", 60)
        .expect("claim")
        .expect("job");
    assert_eq!(claimed.id, first);
    assert_eq!(claimed.status, "running");

    registry.update_job_progress(&first, 50).expect("progress");
    registry.complete_job(&first, "done").expect("complete");

    let status = registry.get_job_status(&first).expect("status");
    assert_eq!(status.as_deref(), Some("succeeded"));
    let count = registry
        .count_jobs_by_type_and_status("scan", "succeeded")
        .expect("count");
    assert_eq!(count, 1);
}

#[test]
fn retry_and_cancel_job() {
    let (_temp_dir, registry) = make_registry();
    let job_id = registry.enqueue_job("scan", "{}", 1, 1).expect("enqueue");

    registry.retry_job(&job_id, "transient").expect("retry");
    let status = registry.get_job_status(&job_id).expect("status");
    assert_eq!(status.as_deref(), Some("queued"));

    let rows = registry.cancel_job(&job_id).expect("cancel");
    assert_eq!(rows, 1);
    let status = registry.get_job_status(&job_id).expect("status");
    assert_eq!(status.as_deref(), Some("canceled"));
}

#[test]
fn list_recent_jobs_orders_by_created_at() {
    let (_temp_dir, registry) = make_registry();
    let first = registry.enqueue_job("scan", "{}", 1, 0).expect("enqueue");
    let second = registry.enqueue_job("scan", "{}", 1, 0).expect("enqueue");

    let conn = registry.get_conn().expect("conn");
    conn.execute(
        "UPDATE sys_jobs SET created_at = datetime('now', '-10 seconds') WHERE id = ?1",
        [first.as_str()],
    )
    .expect("update first created_at");
    conn.execute(
        "UPDATE sys_jobs SET created_at = datetime('now', '-1 seconds') WHERE id = ?1",
        [second.as_str()],
    )
    .expect("update second created_at");
    drop(conn);

    let jobs = registry.list_recent_jobs(10).expect("list");
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].id, second);
    assert_eq!(jobs[1].id, first);
}

#[test]
fn renew_lease_updates_expiry() {
    let (_temp_dir, registry) = make_registry();
    let job_id = registry.enqueue_job("scan", "{}", 1, 0).expect("enqueue");

    let _claimed = registry
        .claim_next_job("worker-1", 30)
        .expect("claim")
        .expect("job");

    registry
        .renew_job_lease(&job_id, "worker-1", 120)
        .expect("renew");

    let job = registry.get_job(&job_id).expect("get").expect("job");
    assert_eq!(job.status, "running");
    assert!(job.lease_expires_at.is_some());
}

#[test]
fn requeue_expired_lease_sets_queued() {
    let (_temp_dir, registry) = make_registry();
    let job_id = registry.enqueue_job("scan", "{}", 1, 0).expect("enqueue");

    registry
        .claim_next_job("worker-1", 1)
        .expect("claim")
        .expect("job");

    let conn = registry.get_conn().expect("conn");
    conn.execute(
        "UPDATE sys_jobs SET lease_expires_at = strftime('%s','now') - 10 WHERE id = ?1",
        [job_id.as_str()],
    )
    .expect("update");
    drop(conn);

    let rows = registry.requeue_expired_job_leases().expect("requeue");
    assert_eq!(rows, 1);

    let job = registry.get_job(&job_id).expect("get").expect("job");
    assert_eq!(job.status, "queued");
    assert_eq!(job.error.as_deref(), Some("lease_expired"));
}

#[test]
fn fail_timed_out_jobs_marks_failed() {
    let (_temp_dir, registry) = make_registry();
    let job_id = registry.enqueue_job("scan", "{}", 1, 0).expect("enqueue");

    registry
        .claim_next_job("worker-1", 60)
        .expect("claim")
        .expect("job");

    let conn = registry.get_conn().expect("conn");
    conn.execute(
        "UPDATE sys_jobs SET started_at = datetime('now', '-10 seconds') WHERE id = ?1",
        [job_id.as_str()],
    )
    .expect("update");
    drop(conn);

    let rows = registry.fail_timed_out_jobs(1).expect("timeout");
    assert_eq!(rows, 1);

    let job = registry.get_job(&job_id).expect("get").expect("job");
    assert_eq!(job.status, "failed");
    assert_eq!(job.error.as_deref(), Some("timeout"));
}
