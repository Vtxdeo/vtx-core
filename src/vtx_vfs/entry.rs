use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use url::Url;

#[derive(Clone)]
pub(super) struct VtxVfsStoreEntry {
    pub(super) scheme: String,
    pub(super) authority: Option<String>,
    pub(super) store: Arc<dyn ObjectStore + Send + Sync>,
    pub(super) file_root: Option<PathBuf>,
}

impl VtxVfsStoreEntry {
    pub(super) fn to_uri(&self, location: &ObjectPath) -> anyhow::Result<String> {
        match self.scheme.as_str() {
            "file" => {
                let root = self
                    .file_root
                    .as_ref()
                    .context("Missing file root for file store")?;
                let rel = location
                    .as_ref()
                    .replace('/', std::path::MAIN_SEPARATOR_STR);
                let os_path = root.join(rel);
                Ok(Url::from_file_path(&os_path)
                    .map_err(|_| anyhow::anyhow!("Invalid file path"))?
                    .to_string())
            }
            "s3" => {
                let bucket = self
                    .authority
                    .as_ref()
                    .context("Missing bucket for s3 store")?;
                let path = location.as_ref();
                if path.is_empty() {
                    Ok(format!("s3://{}", bucket))
                } else {
                    Ok(format!("s3://{}/{}", bucket, path))
                }
            }
            scheme => Err(anyhow::anyhow!("Unsupported scheme: {}", scheme)),
        }
    }
}
