use super::VideoMeta;
use futures_util::StreamExt;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use std::collections::HashSet;
use tracing::{error, info};
use uuid::Uuid;

use crate::vfs::VfsManager;

pub(crate) enum ScanAbort {
    Canceled,
    TimedOut,
}

pub(crate) enum ScanOutcome {
    Completed(Vec<VideoMeta>),
    Aborted(ScanAbort),
}

pub(crate) async fn scan_directory(
    pool: &Pool<SqliteConnectionManager>,
    vfs: &VfsManager,
    root_uri: &str,
) -> anyhow::Result<Vec<VideoMeta>> {
    match scan_directory_with_abort(pool, vfs, root_uri, || Ok(())).await? {
        ScanOutcome::Completed(videos) => Ok(videos),
        ScanOutcome::Aborted(_) => Ok(Vec::new()),
    }
}

pub(crate) async fn scan_directory_with_abort<F>(
    pool: &Pool<SqliteConnectionManager>,
    vfs: &VfsManager,
    root_uri: &str,
    should_continue: F,
) -> anyhow::Result<ScanOutcome>
where
    F: Fn() -> Result<(), ScanAbort> + Send + Sync,
{
    let conn = pool
        .get()
        .map_err(|e| anyhow::anyhow!("DB Connection failed: {}", e))?;

    let root_uri = vfs.ensure_prefix_uri(root_uri)?;
    info!("[scanner] start scanning directory: {}", root_uri);

    let mut stmt = conn.prepare("SELECT full_path FROM videos")?;
    let existing_paths: HashSet<String> = stmt
        .query_map([], |row| row.get(0))?
        .filter_map(Result::ok)
        .collect();

    drop(stmt);
    drop(conn);

    let mut new_videos = Vec::new();
    let mut stream = vfs.list_objects(&root_uri).await?;

    while let Some(item) = stream.next().await {
        if let Err(abort) = should_continue() {
            return Ok(ScanOutcome::Aborted(abort));
        }
        let obj = match item {
            Ok(value) => value,
            Err(_) => continue,
        };

        let ext = extract_extension(&obj.uri);
        if !matches!(ext.as_deref(), Some("mp4") | Some("mkv") | Some("mov") | Some("avi") | Some("webm")) {
            continue;
        }

        if existing_paths.contains(&obj.uri) {
            continue;
        }

        let filename = match extract_filename(&obj.uri) {
            Some(name) => name,
            None => continue,
        };

        new_videos.push(VideoMeta {
            id: Uuid::new_v4().to_string(),
            filename,
            source_uri: obj.uri,
            created_at: "Just Now".to_string(),
        });
    }

    if !new_videos.is_empty() {
        let mut conn = pool.get()?;
        let tx = conn.transaction()?;

        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO videos (id, filename, full_path, created_at)
                 VALUES (?1, ?2, ?3, datetime('now', 'localtime'))",
            )?;

            for video in &new_videos {
                if let Err(e) = stmt.execute(params![&video.id, &video.filename, &video.source_uri]) {
                    error!("[scanner] insert failed: {} ({})", video.filename, e);
                }
            }
        }
        tx.commit()?;
        info!("[scanner] scan completed: {} new videos registered", new_videos.len());
    }

    Ok(ScanOutcome::Completed(new_videos))
}

pub(crate) fn list_all(pool: &Pool<SqliteConnectionManager>) -> anyhow::Result<Vec<VideoMeta>> {
    let conn = pool.get()?;
    let mut stmt = conn.prepare(
        "SELECT id, filename, full_path, created_at FROM videos ORDER BY created_at DESC",
    )?;

    let video_iter = stmt.query_map([], |row| {
        Ok(VideoMeta {
            id: row.get(0)?,
            filename: row.get(1)?,
            source_uri: row.get::<_, String>(2)?,
            created_at: row.get::<_, String>(3)?,
        })
    })?;

    let mut results = Vec::new();
    for video in video_iter {
        results.push(video?);
    }
    Ok(results)
}

pub(crate) fn get_uri(pool: &Pool<SqliteConnectionManager>, id: &str) -> Option<String> {
    let conn = pool.get().ok()?;
    let mut stmt = conn
        .prepare_cached("SELECT full_path FROM videos WHERE id = ?1")
        .ok()?;
    let path_str: String = stmt.query_row(params![id], |row| row.get(0)).ok()?;
    Some(path_str)
}

fn extract_extension(uri: &str) -> Option<String> {
    let path = url::Url::parse(uri).ok().map(|u| u.path().to_string()).unwrap_or_else(|| uri.to_string());
    std::path::Path::new(&path)
        .extension()
        .map(|ext| ext.to_string_lossy().to_lowercase())
}

fn extract_filename(uri: &str) -> Option<String> {
    let path = url::Url::parse(uri).ok().map(|u| u.path().to_string()).unwrap_or_else(|| uri.to_string());
    std::path::Path::new(&path)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
}
