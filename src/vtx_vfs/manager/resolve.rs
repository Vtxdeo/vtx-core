use std::sync::Arc;

use anyhow::Context;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use url::Url;

use super::super::entry::VfsStoreEntry;
use super::super::utils::{
    file_key, object_path_from_relative, object_path_from_url_path, split_root,
};
use super::{VfsResolved, VtxVfsManager};

impl VtxVfsManager {
    pub(super) fn resolve(&self, uri: &str) -> anyhow::Result<VfsResolved> {
        let url = Url::parse(uri).context("Invalid URI")?;
        match url.scheme() {
            "file" => self.resolve_file(url),
            "s3" => self.resolve_s3(url),
            scheme => Err(anyhow::anyhow!("Unsupported URI scheme: {}", scheme)),
        }
    }

    fn resolve_file(&self, url: Url) -> anyhow::Result<VfsResolved> {
        let os_path = url
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("Invalid file URI"))?;
        let (root, relative) = split_root(&os_path);
        let key = file_key(&root);
        let entry = {
            let mut stores = self.stores.write().unwrap();
            if let Some(entry) = stores.get(&key) {
                entry.clone()
            } else {
                let store = LocalFileSystem::new_with_prefix(root.clone())
                    .context("Failed to create local store")?;
                let entry = VfsStoreEntry {
                    scheme: "file".to_string(),
                    authority: None,
                    store: Arc::new(store),
                    file_root: Some(root.clone()),
                };
                stores.insert(key, entry.clone());
                entry
            }
        };
        let location = object_path_from_relative(&relative)?;
        Ok(VfsResolved { entry, location })
    }

    fn resolve_s3(&self, url: Url) -> anyhow::Result<VfsResolved> {
        let bucket = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("s3 URI requires bucket"))?
            .to_string();
        let key = format!("s3://{}", bucket);
        let entry = {
            let mut stores = self.stores.write().unwrap();
            if let Some(entry) = stores.get(&key) {
                entry.clone()
            } else {
                let store = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket.clone())
                    .build()
                    .context("Failed to create S3 store")?;
                let entry = VfsStoreEntry {
                    scheme: "s3".to_string(),
                    authority: Some(bucket.clone()),
                    store: Arc::new(store),
                    file_root: None,
                };
                stores.insert(key, entry.clone());
                entry
            }
        };

        let location = object_path_from_url_path(url.path())?;
        Ok(VfsResolved { entry, location })
    }
}
