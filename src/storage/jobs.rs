use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct JobRecord {
    pub id: String,
    pub job_type: String,
    pub payload: String,
    pub payload_version: i64,
    pub status: String,
    pub progress: i64,
    pub result: Option<String>,
    pub error: Option<String>,
    pub retries: i64,
    pub max_retries: i64,
    pub created_at: String,
    pub updated_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub worker_id: Option<String>,
    pub lease_expires_at: Option<i64>,
}

pub(crate) fn enqueue_job(
    pool: &Pool<SqliteConnectionManager>,
    job_type: &str,
    payload: &str,
    payload_version: i64,
    max_retries: i64,
) -> anyhow::Result<String> {
    let conn = pool.get()?;
    let job_id = Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO sys_jobs (id, job_type, payload, payload_version, status, progress, retries, max_retries)
         VALUES (?1, ?2, ?3, ?4, 'queued', 0, 0, ?5)",
        params![job_id, job_type, payload, payload_version, max_retries],
    )?;
    Ok(job_id)
}

pub(crate) fn get_job(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
) -> anyhow::Result<Option<JobRecord>> {
    let conn = pool.get()?;
    let mut stmt = conn.prepare_cached(
        "SELECT id, job_type, payload, payload_version, status, progress, result, error, retries, max_retries,
                created_at, updated_at, started_at, finished_at, worker_id, lease_expires_at
         FROM sys_jobs WHERE id = ?1",
    )?;
    let record = stmt.query_row(params![job_id], |row| {
        Ok(JobRecord {
            id: row.get(0)?,
            job_type: row.get(1)?,
            payload: row.get(2)?,
            payload_version: row.get(3)?,
            status: row.get(4)?,
            progress: row.get(5)?,
            result: row.get(6)?,
            error: row.get(7)?,
            retries: row.get(8)?,
            max_retries: row.get(9)?,
            created_at: row.get(10)?,
            updated_at: row.get(11)?,
            started_at: row.get(12)?,
            finished_at: row.get(13)?,
            worker_id: row.get(14)?,
            lease_expires_at: row.get(15)?,
        })
    });
    match record {
        Ok(job) => Ok(Some(job)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn get_job_status(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
) -> anyhow::Result<Option<String>> {
    let conn = pool.get()?;
    let mut stmt = conn.prepare_cached("SELECT status FROM sys_jobs WHERE id = ?1")?;
    let status = stmt.query_row(params![job_id], |row| row.get(0));
    match status {
        Ok(status) => Ok(Some(status)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn set_job_error(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
    error: &str,
) -> anyhow::Result<()> {
    let conn = pool.get()?;
    conn.execute(
        "UPDATE sys_jobs SET error = ?1, updated_at = CURRENT_TIMESTAMP WHERE id = ?2",
        params![error, job_id],
    )?;
    Ok(())
}

pub(crate) fn set_job_result(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
    result: &str,
) -> anyhow::Result<()> {
    let conn = pool.get()?;
    conn.execute(
        "UPDATE sys_jobs SET result = ?1, updated_at = CURRENT_TIMESTAMP WHERE id = ?2",
        params![result, job_id],
    )?;
    Ok(())
}

pub(crate) fn set_job_status_terminal(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
    status: &str,
) -> anyhow::Result<()> {
    let conn = pool.get()?;
    conn.execute(
        "UPDATE sys_jobs
         SET status = ?1, updated_at = CURRENT_TIMESTAMP,
             finished_at = CURRENT_TIMESTAMP, lease_expires_at = NULL
         WHERE id = ?2",
        params![status, job_id],
    )?;
    Ok(())
}

pub(crate) fn list_recent_jobs(
    pool: &Pool<SqliteConnectionManager>,
    limit: i64,
) -> anyhow::Result<Vec<JobRecord>> {
    let conn = pool.get()?;
    let mut stmt = conn.prepare(
        "SELECT id, job_type, payload, payload_version, status, progress, result, error, retries, max_retries,
                created_at, updated_at, started_at, finished_at, worker_id, lease_expires_at
         FROM sys_jobs ORDER BY created_at DESC LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![limit], |row| {
        Ok(JobRecord {
            id: row.get(0)?,
            job_type: row.get(1)?,
            payload: row.get(2)?,
            payload_version: row.get(3)?,
            status: row.get(4)?,
            progress: row.get(5)?,
            result: row.get(6)?,
            error: row.get(7)?,
            retries: row.get(8)?,
            max_retries: row.get(9)?,
            created_at: row.get(10)?,
            updated_at: row.get(11)?,
            started_at: row.get(12)?,
            finished_at: row.get(13)?,
            worker_id: row.get(14)?,
            lease_expires_at: row.get(15)?,
        })
    })?;

    let mut jobs = Vec::new();
    for row in rows {
        jobs.push(row?);
    }
    Ok(jobs)
}

pub(crate) fn claim_next_job(
    pool: &Pool<SqliteConnectionManager>,
    worker_id: &str,
    lease_secs: u64,
) -> anyhow::Result<Option<JobRecord>> {
    let mut conn = pool.get()?;
    let tx = conn.transaction()?;
    let job = {
        let mut stmt = tx.prepare(
            "SELECT id, job_type, payload, payload_version, status, progress, result, error, retries, max_retries,
                    created_at, updated_at, started_at, finished_at, worker_id, lease_expires_at
             FROM sys_jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1",
        )?;
        let record = stmt.query_row([], |row| {
            Ok(JobRecord {
                id: row.get(0)?,
                job_type: row.get(1)?,
                payload: row.get(2)?,
                payload_version: row.get(3)?,
                status: row.get(4)?,
                progress: row.get(5)?,
                result: row.get(6)?,
                error: row.get(7)?,
                retries: row.get(8)?,
                max_retries: row.get(9)?,
                created_at: row.get(10)?,
                updated_at: row.get(11)?,
                started_at: row.get(12)?,
                finished_at: row.get(13)?,
                worker_id: row.get(14)?,
                lease_expires_at: row.get(15)?,
            })
        });
        match record {
            Ok(job) => Some(job),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(e.into()),
        }
    };

    let Some(mut job) = job else {
        tx.commit()?;
        return Ok(None);
    };

    let updated = tx.execute(
        "UPDATE sys_jobs
         SET status = 'running', worker_id = ?1, started_at = CURRENT_TIMESTAMP,
             updated_at = CURRENT_TIMESTAMP, lease_expires_at = strftime('%s','now') + ?2
         WHERE id = ?3 AND status = 'queued'",
        params![worker_id, lease_secs as i64, job.id],
    )?;

    if updated == 0 {
        tx.commit()?;
        return Ok(None);
    }

    tx.commit()?;
    job.status = "running".to_string();
    job.worker_id = Some(worker_id.to_string());
    job.lease_expires_at = None;
    Ok(Some(job))
}

pub(crate) fn update_progress(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
    progress: i64,
) -> anyhow::Result<()> {
    let conn = pool.get()?;
    conn.execute(
        "UPDATE sys_jobs SET progress = ?1, updated_at = CURRENT_TIMESTAMP WHERE id = ?2",
        params![progress, job_id],
    )?;
    Ok(())
}

pub(crate) fn complete_job(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
    result: &str,
) -> anyhow::Result<()> {
    let conn = pool.get()?;
    conn.execute(
        "UPDATE sys_jobs
         SET status = 'succeeded', result = ?1, updated_at = CURRENT_TIMESTAMP,
             finished_at = CURRENT_TIMESTAMP, progress = 100, lease_expires_at = NULL
         WHERE id = ?2 AND status = 'running'",
        params![result, job_id],
    )?;
    Ok(())
}

pub(crate) fn fail_job(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
    error: &str,
) -> anyhow::Result<()> {
    let conn = pool.get()?;
    conn.execute(
        "UPDATE sys_jobs
         SET status = 'failed', error = ?1, updated_at = CURRENT_TIMESTAMP,
             finished_at = CURRENT_TIMESTAMP, lease_expires_at = NULL
         WHERE id = ?2 AND status = 'running'",
        params![error, job_id],
    )?;
    Ok(())
}

pub(crate) fn retry_job(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
    error: &str,
) -> anyhow::Result<()> {
    let conn = pool.get()?;
    conn.execute(
        "UPDATE sys_jobs
         SET status = 'queued', error = ?1, updated_at = CURRENT_TIMESTAMP,
             worker_id = NULL, progress = 0, result = NULL, started_at = NULL,
             finished_at = NULL, lease_expires_at = NULL
         WHERE id = ?2",
        params![error, job_id],
    )?;
    Ok(())
}

pub(crate) fn increment_retries(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
) -> anyhow::Result<()> {
    let conn = pool.get()?;
    conn.execute(
        "UPDATE sys_jobs SET retries = retries + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
        params![job_id],
    )?;
    Ok(())
}

pub(crate) fn cancel_job(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
) -> anyhow::Result<usize> {
    let conn = pool.get()?;
    let rows = conn.execute(
        "UPDATE sys_jobs
         SET status = 'canceled', updated_at = CURRENT_TIMESTAMP,
             finished_at = CURRENT_TIMESTAMP, lease_expires_at = NULL
         WHERE id = ?1 AND status IN ('queued', 'running')",
        params![job_id],
    )?;
    Ok(rows)
}

pub(crate) fn fail_timed_out_jobs(
    pool: &Pool<SqliteConnectionManager>,
    timeout_secs: u64,
) -> anyhow::Result<usize> {
    let conn = pool.get()?;
    let rows = conn.execute(
        "UPDATE sys_jobs
         SET status = 'failed', error = 'timeout', updated_at = CURRENT_TIMESTAMP,
             finished_at = CURRENT_TIMESTAMP, lease_expires_at = NULL
         WHERE status = 'running'
           AND started_at IS NOT NULL
           AND (strftime('%s','now') - strftime('%s', started_at)) > ?1",
        params![timeout_secs as i64],
    )?;
    Ok(rows)
}

pub(crate) fn renew_lease(
    pool: &Pool<SqliteConnectionManager>,
    job_id: &str,
    worker_id: &str,
    lease_secs: u64,
) -> anyhow::Result<()> {
    let conn = pool.get()?;
    conn.execute(
        "UPDATE sys_jobs
         SET lease_expires_at = strftime('%s','now') + ?1,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?2 AND status = 'running' AND worker_id = ?3",
        params![lease_secs as i64, job_id, worker_id],
    )?;
    Ok(())
}

pub(crate) fn requeue_expired_leases(
    pool: &Pool<SqliteConnectionManager>,
) -> anyhow::Result<usize> {
    let conn = pool.get()?;
    let rows = conn.execute(
        "UPDATE sys_jobs
         SET status = 'queued', worker_id = NULL, updated_at = CURRENT_TIMESTAMP,
             started_at = NULL, finished_at = NULL, progress = 0, result = NULL,
             error = 'lease_expired', lease_expires_at = NULL
         WHERE status = 'running'
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at < strftime('%s','now')",
        [],
    )?;
    Ok(rows)
}

pub(crate) fn count_jobs_by_type_and_status(
    pool: &Pool<SqliteConnectionManager>,
    job_type: &str,
    status: &str,
) -> anyhow::Result<usize> {
    let conn = pool.get()?;
    let mut stmt =
        conn.prepare_cached("SELECT COUNT(*) FROM sys_jobs WHERE job_type = ?1 AND status = ?2")?;
    let count: i64 = stmt.query_row(params![job_type, status], |row| row.get(0))?;
    Ok(count as usize)
}
