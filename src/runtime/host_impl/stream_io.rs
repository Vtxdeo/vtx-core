use std::io::{Cursor, Read, Seek, SeekFrom};
use wasmtime::component::Resource;

use crate::common::buffer::{BufferType, RealBuffer};
use crate::runtime::context::{SecurityPolicy, StreamContext};

use super::api;

/// 内部 Trait：抽象化对文件和内存流的统一读取能力
trait BufferIo {
    fn get_size(&self) -> u64;
    fn read_at(&mut self, offset: u64, dest: &mut [u8]) -> usize;
}

impl BufferIo for BufferType {
    fn get_size(&self) -> u64 {
        match self {
            BufferType::File(f) => f.metadata().map(|m| m.len()).unwrap_or(0),
            BufferType::Memory(c) => c.get_ref().len() as u64,
            BufferType::Pipe(_) => 0,
        }
    }

    fn read_at(&mut self, offset: u64, dest: &mut [u8]) -> usize {
        match self {
            BufferType::File(f) => f
                .seek(SeekFrom::Start(offset))
                .and_then(|_| f.read(dest))
                .unwrap_or(0),
            BufferType::Memory(c) => c
                .seek(SeekFrom::Start(offset))
                .and_then(|_| c.read(dest))
                .unwrap_or(0),
            BufferType::Pipe(_) => 0,
        }
    }
}

/// 插件侧调用：打开宿主文件资源
impl api::stream_io::Host for StreamContext {
    fn open_file(&mut self, uuid: String) -> Result<Resource<RealBuffer>, String> {
        // [安全拦截] 鉴权模式下禁止访问文件系统
        if self.policy == SecurityPolicy::Restricted {
            tracing::warn!("[Security] Blocked file access: {}", uuid);
            return Err("Permission Denied".into());
        }

        let file_path = self
            .registry
            .get_path(&uuid)
            .ok_or_else(|| "UUID not found".to_string())?;

        let file = std::fs::File::open(&file_path).map_err(|e| format!("IO Error: {}", e))?;

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

    fn create_memory_buffer(&mut self, data: Vec<u8>) -> Resource<RealBuffer> {
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

/// 插件侧调用：缓冲区操作实现
impl api::stream_io::HostBuffer for StreamContext {
    fn size(&mut self, resource: Resource<RealBuffer>) -> u64 {
        self.table
            .get(&resource)
            .map(|b| b.inner.get_size())
            .unwrap_or(0)
    }

    fn read(&mut self, resource: Resource<RealBuffer>, offset: u64, max_bytes: u64) -> Vec<u8> {
        let mut chunk = vec![0u8; max_bytes as usize];

        let read_len = if let Ok(buffer) = self.table.get_mut(&resource) {
            buffer.inner.read_at(offset, &mut chunk)
        } else {
            0
        };

        chunk.truncate(read_len);
        chunk
    }

    fn drop(&mut self, resource: Resource<RealBuffer>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}
