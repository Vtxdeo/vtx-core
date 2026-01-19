use std::io::{Cursor, Seek, SeekFrom};
use wasmtime::component::Resource;

use crate::common::buffer::{BufferType, RealBuffer};
use crate::runtime::context::{SecurityPolicy, StreamContext};
use futures_util::StreamExt;

use super::api;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

impl api::vtx_vfs::Host for StreamContext {
    async fn create_memory_buffer(&mut self, data: Vec<u8>) -> Resource<RealBuffer> {
        if self.policy == SecurityPolicy::Plugin && !self.has_permission("buffer:create") {
            return self
                .table
                .push(RealBuffer {
                    inner: BufferType::Memory(Cursor::new(Vec::new())),
                    uri_hint: None,
                    mime_override: Some("application/json".to_string()),
                    process_handle: None,
                })
                .expect("Critical: Failed to allocate memory buffer in host table");
        }
        let rb = RealBuffer {
            inner: BufferType::Memory(Cursor::new(data)),
            uri_hint: None,
            mime_override: Some("application/json".to_string()),
            process_handle: None,
        };

        self.table
            .push(rb)
            .expect("Critical: Failed to allocate memory buffer in host table")
    }

    async fn open_uri(&mut self, uri: String) -> Result<Resource<RealBuffer>, String> {
        if self.policy == SecurityPolicy::Restricted {
            tracing::warn!("[Security] Blocked vfs access: {}", uri);
            return Err("Permission Denied".into());
        }
        if self.policy == SecurityPolicy::Plugin && !self.has_permission("file:read") {
            tracing::warn!("[Security] Missing file:read permission: {}", uri);
            return Err("Permission Denied".into());
        }

        let normalized = self.vfs.normalize_uri(&uri).map_err(|e| e.to_string())?;

        let rb = RealBuffer {
            inner: BufferType::Object {
                uri: normalized.clone(),
            },
            uri_hint: Some(normalized),
            mime_override: None,
            process_handle: None,
        };

        self.table
            .push(rb)
            .map_err(|e| format!("Resource Table Error: {}", e))
    }

    async fn head(&mut self, uri: String) -> Result<api::vtx_vfs::VtxObjectMeta, String> {
        if self.policy == SecurityPolicy::Restricted {
            tracing::warn!("[Security] Blocked vfs access: {}", uri);
            return Err("Permission Denied".into());
        }
        if self.policy == SecurityPolicy::Plugin && !self.has_permission("file:read") {
            tracing::warn!("[Security] Missing file:read permission: {}", uri);
            return Err("Permission Denied".into());
        }

        let normalized = self.vfs.normalize_uri(&uri).map_err(|e| e.to_string())?;
        let meta = self
            .vfs
            .head(&normalized)
            .await
            .map_err(|e| e.to_string())?;

        Ok(api::vtx_vfs::VtxObjectMeta {
            uri: meta.uri,
            size: meta.size,
            last_modified: meta.last_modified,
            etag: meta.etag,
        })
    }

    async fn list_objects(
        &mut self,
        prefix_uri: String,
    ) -> Result<Vec<api::vtx_vfs::VtxObjectMeta>, String> {
        if self.policy == SecurityPolicy::Restricted {
            tracing::warn!("[Security] Blocked vfs access: {}", prefix_uri);
            return Err("Permission Denied".into());
        }
        if self.policy == SecurityPolicy::Plugin && !self.has_permission("file:read") {
            tracing::warn!("[Security] Missing file:read permission: {}", prefix_uri);
            return Err("Permission Denied".into());
        }

        let normalized = self
            .vfs
            .ensure_prefix_uri(&prefix_uri)
            .map_err(|e| e.to_string())?;
        let mut stream = self
            .vfs
            .list_objects(&normalized)
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| e.to_string())?;
            out.push(api::vtx_vfs::VtxObjectMeta {
                uri: meta.uri,
                size: meta.size,
                last_modified: meta.last_modified,
                etag: meta.etag,
            });
        }
        Ok(out)
    }

    async fn read_range(&mut self, uri: String, offset: u64, len: u64) -> Result<Vec<u8>, String> {
        if self.policy == SecurityPolicy::Restricted {
            tracing::warn!("[Security] Blocked vfs access: {}", uri);
            return Err("Permission Denied".into());
        }
        if self.policy == SecurityPolicy::Plugin && !self.has_permission("file:read") {
            tracing::warn!("[Security] Missing file:read permission: {}", uri);
            return Err("Permission Denied".into());
        }

        let normalized = self.vfs.normalize_uri(&uri).map_err(|e| e.to_string())?;
        let bytes = self
            .vfs
            .read_range(&normalized, offset, len)
            .await
            .map_err(|e| e.to_string())?;
        Ok(bytes.to_vec())
    }
}

impl api::vtx_vfs::HostBuffer for StreamContext {
    async fn size(&mut self, resource: Resource<RealBuffer>) -> u64 {
        let rb = match self.table.get_mut(&resource) {
            Ok(b) => b,
            Err(_) => return 0,
        };

        match &rb.inner {
            BufferType::Object { uri } => self.vfs.head(uri).await.map(|m| m.size).unwrap_or(0),
            BufferType::Memory(c) => c.get_ref().len() as u64,
            BufferType::Pipe(_) => 0,
        }
    }

    async fn read(
        &mut self,
        resource: Resource<RealBuffer>,
        offset: u64,
        max_bytes: u64,
    ) -> Vec<u8> {
        let rb = match self.table.get_mut(&resource) {
            Ok(b) => b,
            Err(_) => return vec![],
        };

        let limit = std::cmp::min(max_bytes, self.max_buffer_read_bytes);

        match &mut rb.inner {
            BufferType::Pipe(stdout) => {
                let mut buf = vec![0u8; limit as usize];
                match stdout.read(&mut buf).await {
                    Ok(n) => {
                        buf.truncate(n);
                        buf
                    }
                    Err(e) => {
                        tracing::warn!("Pipe read error: {}", e);
                        vec![]
                    }
                }
            }
            BufferType::Memory(c) => {
                let mut chunk = vec![0u8; limit as usize];
                let _ = c.seek(SeekFrom::Start(offset));
                let read_len = std::io::Read::read(c, &mut chunk).unwrap_or(0);
                chunk.truncate(read_len);
                chunk
            }
            BufferType::Object { uri } => match self.vfs.read_range(uri, offset, limit).await {
                Ok(bytes) => bytes.to_vec(),
                Err(e) => {
                    tracing::warn!("VFS read error: {}", e);
                    vec![]
                }
            },
        }
    }

    async fn write(&mut self, resource: Resource<RealBuffer>, data: Vec<u8>) -> u64 {
        if self.policy == SecurityPolicy::Plugin && !self.has_permission("file:write") {
            return 0;
        }

        let rb = match self.table.get_mut(&resource) {
            Ok(b) => b,
            Err(_) => return 0,
        };

        if let Some(child) = &mut rb.process_handle {
            if let Some(stdin) = &mut child.stdin {
                if let Err(e) = stdin.write_all(&data).await {
                    tracing::error!("Failed to write to process stdin: {}", e);
                    return 0;
                }
                if let Err(e) = stdin.flush().await {
                    tracing::error!("Failed to flush process stdin: {}", e);
                    return 0;
                }
                return data.len() as u64;
            }
        }

        match &mut rb.inner {
            BufferType::Memory(c) => {
                let _ = c.seek(SeekFrom::End(0));
                match std::io::Write::write(c, &data) {
                    Ok(n) => n as u64,
                    Err(_) => 0,
                }
            }
            BufferType::Pipe(_) => {
                tracing::error!("Cannot write to a standard output pipe (Read-only)");
                0
            }
            BufferType::Object { .. } => {
                tracing::warn!("VFS objects are read-only in the current host");
                0
            }
        }
    }

    async fn drop(&mut self, resource: Resource<RealBuffer>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}
