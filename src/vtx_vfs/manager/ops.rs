use std::ops::RangeInclusive;

use anyhow::Context;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use object_store::{GetOptions, GetRange, ObjectMeta};

use super::super::entry::VtxVfsStoreEntry;
use super::super::utils::to_range;
use super::{VfsObject, VtxVfsManager};

impl VtxVfsManager {
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

    fn meta_to_object(
        &self,
        entry: &VtxVfsStoreEntry,
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
