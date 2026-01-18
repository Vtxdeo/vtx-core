use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::{Arc, RwLock};

use anyhow::Context;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectMeta};
use url::Url;

use super::entry::VfsStoreEntry;
use super::utils::{
    file_key, normalize_path, object_path_from_relative, object_path_from_url_path, same_authority,
    split_root, to_range,
};

#[derive(Clone)]
struct VfsResolved {
    entry: VfsStoreEntry,
    location: Option<ObjectPath>,
}

pub struct VfsObject {
    pub uri: String,
    pub size: u64,
    pub last_modified: Option<i64>,
    pub etag: Option<String>,
}

pub struct VfsManager {
    stores: RwLock<HashMap<String, VfsStoreEntry>>,
}

fn _assert_send_sync() {
    fn assert<T: Send + Sync>() {}
    assert::<VfsManager>();
}

impl VfsManager {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            stores: RwLock::new(HashMap::new()),
        })
    }

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

    pub async fn head(&self, uri: &str) -> anyhow::Result<VfsObject> {
        let resolved = self.resolve(uri)?;
        let location = resolved
            .location
            .clone()
            .context("URI must point to an object")?;
        let meta = resolved.entry.store.head(&location).await?;
        self.meta_to_object(&resolved.entry, &meta)
    }

    pub async fn list_objects(
        &self,
        prefix_uri: &str,
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<VfsObject>>> {
        let resolved = self.resolve(prefix_uri)?;
        let store = resolved.entry.store.clone();
        let entry = resolved.entry.clone();
        let prefix = resolved.location.clone();

        let stream = async_stream::try_stream! {
            let mut inner = store.list(prefix.as_ref());
            while let Some(item) = inner.next().await {
                let meta = item?;
                let uri = entry.clone().to_uri(&meta.location)?;
                yield VfsObject {
                    uri,
                    size: meta.size as u64,
                    last_modified: Some(meta.last_modified.timestamp()),
                    etag: meta.e_tag.clone(),
                };
            }
        };

        Ok(Box::pin(stream))
    }

    pub async fn read_range(&self, uri: &str, offset: u64, len: u64) -> anyhow::Result<Bytes> {
        let resolved = self.resolve(uri)?;
        let location = resolved
            .location
            .clone()
            .context("URI must point to an object")?;
        if len == 0 {
            return Ok(Bytes::new());
        }
        let start = offset;
        let end = offset.saturating_add(len).saturating_sub(1);
        let range = to_range(start, end)?;
        let bytes = resolved.entry.store.get_range(&location, range).await?;
        Ok(bytes)
    }

    pub async fn get_stream(
        &self,
        uri: &str,
        range: Option<RangeInclusive<u64>>,
    ) -> anyhow::Result<BoxStream<'static, std::io::Result<Bytes>>> {
        let resolved = self.resolve(uri)?;
        let location = resolved
            .location
            .clone()
            .context("URI must point to an object")?;
        let store = resolved.entry.store.clone();

        let stream = if let Some(range) = range {
            let mut options = GetOptions::default();
            let start = *range.start();
            let end = *range.end();
            options.range = Some(GetRange::Bounded(to_range(start, end)?));
            let result = store.get_opts(&location, options).await?;
            result.into_stream()
        } else {
            let result = store.get(&location).await?;
            result.into_stream()
        };

        let mapped = stream.map(|item| match item {
            Ok(bytes) => Ok(bytes),
            Err(err) => Err(std::io::Error::other(err.to_string())),
        });
        Ok(Box::pin(mapped))
    }

    fn resolve(&self, uri: &str) -> anyhow::Result<VfsResolved> {
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

    fn meta_to_object(
        &self,
        entry: &VfsStoreEntry,
        meta: &ObjectMeta,
    ) -> anyhow::Result<VfsObject> {
        let uri = entry.to_uri(&meta.location)?;
        Ok(VfsObject {
            uri,
            size: meta.size as u64,
            last_modified: Some(meta.last_modified.timestamp()),
            etag: meta.e_tag.clone(),
        })
    }
}
