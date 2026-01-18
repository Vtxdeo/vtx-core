use anyhow::Context;
use url::Url;

use super::super::utils::{normalize_path, same_authority};
use super::VtxVfsManager;

impl VtxVfsManager {
    pub fn normalize_uri(&self, uri: &str) -> anyhow::Result<String> {
        let mut url = Url::parse(uri).context("Invalid URI")?;
        let raw_path = url.path();
        let normalized = normalize_path(raw_path);
        url.set_path(&normalized);
        Ok(url.to_string())
    }

    pub fn ensure_prefix_uri(&self, uri: &str) -> anyhow::Result<String> {
        let normalized = self.normalize_uri(uri)?;
        let mut url = Url::parse(&normalized).context("Invalid URI")?;
        if !url.path().ends_with('/') {
            let mut path = url.path().to_string();
            path.push('/');
            url.set_path(&path);
        }
        Ok(url.to_string())
    }

    pub fn match_allowed_prefix(
        &self,
        requested: &str,
        allowed_roots: &[String],
    ) -> Result<String, String> {
        let requested_norm = self
            .normalize_uri(requested)
            .map_err(|_| "Invalid scan path".to_string())?;
        let requested_url =
            Url::parse(&requested_norm).map_err(|_| "Invalid scan path".to_string())?;

        let mut has_root = false;
        for root in allowed_roots {
            let root_norm = match self.ensure_prefix_uri(root) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let root_url = match Url::parse(&root_norm) {
                Ok(value) => value,
                Err(_) => continue,
            };
            has_root = true;
            if same_authority(&requested_url, &root_url) {
                let root_path = root_url.path();
                let requested_path = requested_url.path();
                if requested_path == root_path.trim_end_matches('/')
                    || requested_path.starts_with(root_path)
                {
                    return Ok(requested_norm);
                }
            }
        }

        if !has_root {
            return Err("Scan roots not configured".into());
        }
        Err("Scan path not allowed".into())
    }
}
