use std::path::{Component, Path, PathBuf};

use object_store::path::Path as ObjectPath;
use url::Url;

pub(super) fn split_root(path: &Path) -> (PathBuf, PathBuf) {
    let mut root = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Prefix(prefix) => root.push(prefix.as_os_str()),
            Component::RootDir => root.push(std::path::MAIN_SEPARATOR.to_string()),
            _ => break,
        }
    }
    if root.as_os_str().is_empty() {
        root.push(std::path::MAIN_SEPARATOR.to_string());
    }
    let relative = path.strip_prefix(&root).unwrap_or(path).to_path_buf();
    (root, relative)
}

pub(super) fn object_path_from_relative(path: &Path) -> anyhow::Result<Option<ObjectPath>> {
    let raw = path.to_string_lossy().replace('\\', "/");
    let trimmed = raw.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(ObjectPath::parse(trimmed)?))
}

pub(super) fn object_path_from_url_path(path: &str) -> anyhow::Result<Option<ObjectPath>> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(ObjectPath::from(trimmed)))
}

pub(super) fn normalize_path(path: &str) -> String {
    let trailing = path.ends_with('/');
    let mut parts = Vec::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            parts.pop();
            continue;
        }
        parts.push(part);
    }
    let mut normalized = String::from("/");
    normalized.push_str(&parts.join("/"));
    if trailing && !normalized.ends_with('/') {
        normalized.push('/');
    }
    if normalized.len() > 1 && normalized.ends_with('/') && parts.is_empty() {
        normalized.truncate(1);
    }
    normalized
}

pub(super) fn same_authority(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

pub(super) fn to_range(start: u64, end: u64) -> anyhow::Result<std::ops::Range<u64>> {
    if end < start {
        return Err(anyhow::anyhow!("Invalid range"));
    }
    let start = start;
    let end = end.saturating_add(1);
    Ok(start..end)
}

pub(super) fn file_key(root: &Path) -> String {
    let raw = root.to_string_lossy().replace('\\', "/");
    format!("file://{}", raw.trim_end_matches('/'))
}
