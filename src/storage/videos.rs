use super::VideoMeta;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rayon::prelude::*;
use rusqlite::params;
use std::collections::HashSet;
use std::path::PathBuf;
use tracing::{error, info, warn};
use uuid::Uuid;
use walkdir::WalkDir;

/// 扫描目录并更新视频库
pub(crate) fn scan_directory(
    pool: &Pool<SqliteConnectionManager>,
    dir_path: &str,
) -> anyhow::Result<Vec<VideoMeta>> {
    let conn = pool
        .get()
        .map_err(|e| anyhow::anyhow!("DB Connection failed: {}", e))?;

    // 路径解析与安全校验
    let root_path = std::fs::canonicalize(dir_path).map_err(|e| {
        error!("[scanner] failed to resolve scan root: {}", e);
        anyhow::anyhow!("Invalid directory path: {}", e)
    })?;

    info!("[scanner] start scanning directory: {:?}", root_path);

    // 预读缓存，用于内存判重
    let mut stmt = conn.prepare("SELECT full_path FROM videos")?;
    let existing_paths: HashSet<String> = stmt
        .query_map([], |row| row.get(0))?
        .filter_map(Result::ok)
        .collect();

    drop(stmt);
    drop(conn); // 释放连接供后续使用

    // 并行遍历文件系统
    let new_videos: Vec<VideoMeta> = WalkDir::new(dir_path)
        .into_iter()
        .par_bridge()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_file())
        .filter_map(|entry| {
            let path = entry.path();
            let ext = path.extension()?.to_string_lossy().to_lowercase();
            if !["mp4", "mkv", "mov", "avi", "webm"].contains(&ext.as_str()) {
                return None;
            }

            let real_path = std::fs::canonicalize(path).ok()?;
            let full_path_str = real_path.to_string_lossy().to_string();

            // 防御软链接逃逸
            if !real_path.starts_with(&root_path) {
                warn!("[scanner] symlink outside root skipped: {:?}", real_path);
                return None;
            }

            if existing_paths.contains(&full_path_str) {
                return None;
            }

            Some(VideoMeta {
                id: Uuid::new_v4().to_string(),
                filename: path.file_name()?.to_string_lossy().to_string(),
                full_path: real_path,
                created_at: "Just Now".to_string(),
            })
        })
        .collect();

    // 批量写入
    if !new_videos.is_empty() {
        let mut conn = pool.get()?;
        let tx = conn.transaction()?;

        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO videos (id, filename, full_path, created_at)
                 VALUES (?1, ?2, ?3, datetime('now', 'localtime'))",
            )?;

            for video in &new_videos {
                let path_str = video.full_path.to_string_lossy().to_string();
                if let Err(e) = stmt.execute(params![&video.id, &video.filename, &path_str]) {
                    error!("[scanner] insert failed: {} ({})", video.filename, e);
                }
            }
        }
        tx.commit()?;
        info!(
            "[scanner] scan completed: {} new videos registered",
            new_videos.len()
        );
    }

    Ok(new_videos)
}

/// 全量列出视频
pub(crate) fn list_all(
    pool: &Pool<SqliteConnectionManager>,
) -> anyhow::Result<Vec<VideoMeta>> {
    let conn = pool.get()?;
    let mut stmt = conn.prepare(
        "SELECT id, filename, full_path, created_at FROM videos ORDER BY created_at DESC",
    )?;

    let video_iter = stmt.query_map([], |row| {
        Ok(VideoMeta {
            id: row.get(0)?,
            filename: row.get(1)?,
            full_path: PathBuf::from(row.get::<_, String>(2)?),
            created_at: row.get::<_, String>(3)?,
        })
    })?;

    let mut results = Vec::new();
    for video in video_iter {
        results.push(video?);
    }
    Ok(results)
}

/// 根据 ID 获取路径
pub(crate) fn get_path(
    pool: &Pool<SqliteConnectionManager>,
    id: &str,
) -> Option<PathBuf> {
    let conn = pool.get().ok()?;
    let mut stmt = conn
        .prepare_cached("SELECT full_path FROM videos WHERE id = ?1")
        .ok()?;
    let path_str: String = stmt.query_row(params![id], |row| row.get(0)).ok()?;
    let path = PathBuf::from(path_str);

    if path.exists() {
        Some(path)
    } else {
        warn!("[Database] File not found on disk: {:?}", path);
        None
    }
}
