use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use wasmtime::component::Resource;

use crate::common::buffer::{BufferType, RealBuffer};
use crate::runtime::context::{SecurityPolicy, StreamContext};

use super::api;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;

/// 内部 Trait：抽象化对文件和内存流的同步读取能力
trait BufferIo {
    fn get_size(&self) -> u64;
    fn read_at(&mut self, offset: u64, dest: &mut [u8]) -> std::io::Result<usize>;
}

impl BufferIo for BufferType {
    fn get_size(&self) -> u64 {
        match self {
            BufferType::File(f) => f.metadata().map(|m| m.len()).unwrap_or(0),
            BufferType::Memory(c) => c.get_ref().len() as u64,
            BufferType::Pipe(_) => 0,
        }
    }

    fn read_at(&mut self, offset: u64, dest: &mut [u8]) -> std::io::Result<usize> {
        match self {
            BufferType::File(f) => {
                f.seek(SeekFrom::Start(offset))?;
                f.read(dest)
            }
            BufferType::Memory(c) => {
                c.seek(SeekFrom::Start(offset))?;
                std::io::Read::read(c, dest)
            }
            BufferType::Pipe(_) => {
                Err(std::io::Error::new(std::io::ErrorKind::Other, "Pipe does not support synchronous random access"))
            }
        }
    }
}

/// 插件侧调用：资源生命周期管理 (Open/Create)
#[async_trait::async_trait]
impl api::stream_io::Host for StreamContext {
    async fn open_file(&mut self, uuid: String) -> Result<Resource<RealBuffer>, String> {
        // [安全拦截] 鉴权模式下禁止访问文件系统
        if self.policy == SecurityPolicy::Restricted {
            tracing::warn!("[Security] Blocked file access: {}", uuid);
            return Err("Permission Denied".into());
        }
        if self.policy == SecurityPolicy::Plugin && !self.has_permission("file:read") {
            tracing::warn!("[Security] Missing file:read permission: {}", uuid);
            return Err("Permission Denied".into());
        }

        let file_path = self
            .registry
            .get_path(&uuid)
            .ok_or_else(|| "UUID not found".to_string())?;

        let file_path_clone = file_path.clone();
        let file = task::spawn_blocking(move || {
            std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_path_clone)
        })
        .await
        .map_err(|e| format!("IO Join Error: {}", e))?
        .map_err(|e| format!("IO Error: {}", e))?;

        let rb = RealBuffer {
            inner: BufferType::File(file),
            path_hint: Some(file_path),
            mime_override: None,
            process_handle: None,
        };

        // 资源表分配失败转换为 String 错误
        self.table
            .push(rb)
            .map_err(|e| format!("Resource Table Error: {}", e))
    }

    async fn create_memory_buffer(&mut self, data: Vec<u8>) -> Resource<RealBuffer> {
        if self.policy == SecurityPolicy::Plugin && !self.has_permission("buffer:create") {
            return self
                .table
                .push(RealBuffer {
                    inner: BufferType::Memory(Cursor::new(Vec::new())),
                    path_hint: None,
                    mime_override: Some("application/json".to_string()),
                    process_handle: None,
                })
                .expect("Critical: Failed to allocate memory buffer in host table");
        }
        let rb = RealBuffer {
            inner: BufferType::Memory(Cursor::new(data)),
            path_hint: None,
            mime_override: Some("application/json".to_string()),
            process_handle: None,
        };

        // 此接口无错误返回值，若分配失败则直接 Panic (触发 Wasm Trap)
        self.table
            .push(rb)
            .expect("Critical: Failed to allocate memory buffer in host table")
    }
}

/// 插件侧调用：缓冲区读写操作实现
#[async_trait::async_trait]
impl api::stream_io::HostBuffer for StreamContext {
    async fn size(&mut self, resource: Resource<RealBuffer>) -> u64 {
        self.table
            .get(&resource)
            .map(|b| b.inner.get_size())
            .unwrap_or(0)
    }

    async fn read(
        &mut self,
        resource: Resource<RealBuffer>,
        offset: u64,
        max_bytes: u64,
    ) -> Vec<u8> {
        let file_clone = {
            let rb = match self.table.get_mut(&resource) {
                Ok(b) => b,
                Err(_) => return vec![],
            };

            match &mut rb.inner {
                BufferType::Pipe(stdout) => {
                    let limit = std::cmp::min(max_bytes, self.max_buffer_read_bytes);
                    let mut buf = vec![0u8; limit as usize];
                    match stdout.read(&mut buf).await {
                        Ok(n) => {
                            buf.truncate(n);
                            return buf;
                        }
                        Err(e) => {
                            tracing::warn!("Pipe read error: {}", e);
                            return vec![];
                        }
                    }
                }
                BufferType::Memory(_) => {
                    let limit = std::cmp::min(max_bytes, self.max_buffer_read_bytes);
                    let mut chunk = vec![0u8; limit as usize];
                    let read_len = rb.inner.read_at(offset, &mut chunk).unwrap_or(0);
                    chunk.truncate(read_len);
                    return chunk;
                }
                BufferType::File(f) => match f.try_clone() {
                    Ok(file) => Some(file),
                    Err(e) => {
                        tracing::error!("Failed to clone file handle: {}", e);
                        None
                    }
                },
            }
        };

        let Some(file) = file_clone else {
            return vec![];
        };

        let limit = std::cmp::min(max_bytes, self.max_buffer_read_bytes);
        task::spawn_blocking(move || {
            let mut file = file;
            if file.seek(SeekFrom::Start(offset)).is_err() {
                return vec![];
            }
            let mut chunk = vec![0u8; limit as usize];
            let read_len = file.read(&mut chunk).unwrap_or_else(|_| 0);
            chunk.truncate(read_len);
            chunk
        })
        .await
        .unwrap_or_default()
    }

    async fn write(&mut self, resource: Resource<RealBuffer>, data: Vec<u8>) -> u64 {
        if self.policy == SecurityPolicy::Plugin && !self.has_permission("file:write") {
            return 0;
        }
        let file_clone = {
            let rb = match self.table.get_mut(&resource) {
                Ok(b) => b,
                Err(_) => return 0,
            };

            // 1. 尝试写入子进程管道 (Async)
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

            // 2. 尝试写入 File/Memory (Sync)
            match &mut rb.inner {
                BufferType::File(f) => match f.try_clone() {
                    Ok(file) => Some(file),
                    Err(e) => {
                        tracing::error!("Failed to clone file handle: {}", e);
                        None
                    }
                },
                BufferType::Memory(c) => {
                    let _ = c.seek(SeekFrom::End(0));
                    return match std::io::Write::write(c, &data) {
                        Ok(n) => n as u64,
                        Err(_) => 0,
                    };
                }
                BufferType::Pipe(_) => {
                    tracing::error!("Cannot write to a standard output pipe (Read-only)");
                    return 0;
                }
            }
        };

        let Some(file) = file_clone else {
            return 0;
        };

        task::spawn_blocking(move || {
            let mut file = file;
            let _ = file.seek(SeekFrom::End(0));
            match file.write(&data) {
                Ok(n) => n as u64,
                Err(_) => 0,
            }
        })
        .await
        .unwrap_or(0)
    }

    fn drop(&mut self, resource: Resource<RealBuffer>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}
