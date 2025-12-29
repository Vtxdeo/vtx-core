use rusqlite::types::ToSql;
use serde_json::{Map, Value};
use std::io::{Cursor, Read, Seek, SeekFrom};
use wasmtime::component::Resource;

use crate::common::buffer::{BufferType, RealBuffer};
use crate::runtime::context::{SecurityPolicy, StreamContext};

include!(concat!(env!("OUT_DIR"), "/host_bindings.rs"));

pub use vtx::api;

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
        }
    }

    fn read_at(&mut self, offset: u64, dest: &mut [u8]) -> usize {
        let result = match self {
            BufferType::File(f) => f.seek(SeekFrom::Start(offset)).and_then(|_| f.read(dest)),
            BufferType::Memory(c) => c.seek(SeekFrom::Start(offset)).and_then(|_| c.read(dest)),
        };
        result.unwrap_or(0)
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
        };

        // 资源表分配失败转换为 String 错误
        self.table
            .push(rb)
            .map_err(|e| format!("Resource Table Error: {}", e))
    }

    // 修正：去除外层 wasmtime::Result
    fn create_memory_buffer(&mut self, data: Vec<u8>) -> Resource<RealBuffer> {
        let rb = RealBuffer {
            inner: BufferType::Memory(Cursor::new(data)),
            path_hint: None,
            mime_override: Some("application/json".to_string()),
        };

        // 此接口无错误返回值，若分配失败则直接 Panic (触发 Wasm Trap)
        self.table
            .push(rb)
            .expect("Critical: Failed to allocate memory buffer in host table")
    }
}

/// 插件侧调用：缓冲区操作实现
impl api::stream_io::HostBuffer for StreamContext {
    // 修正：直接返回 u64
    fn size(&mut self, resource: Resource<RealBuffer>) -> u64 {
        self.table
            .get(&resource)
            .map(|b| b.inner.get_size())
            .unwrap_or(0) // 无效句柄返回 0，或可选择 panic
    }

    // 修正：直接返回 Vec<u8>
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

    // 注意：drop 方法在 bindgen 中通常保留 wasmtime::Result<()> 返回值
    fn drop(&mut self, resource: Resource<RealBuffer>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

/// 插件侧调用：执行 SQL 语句
impl api::sql::Host for StreamContext {
    // 修正：直接返回 Result<u64, String>
    fn execute(
        &mut self,
        statement: String,
        params: Vec<api::sql::DbValue>,
    ) -> Result<u64, String> {
        // [安全拦截] 鉴权模式下禁止写入数据库
        if self.policy == SecurityPolicy::Restricted {
            return Err("Permission Denied".into());
        }

        let conn = self.registry.get_conn().map_err(|e| e.to_string())?;

        let sql_params = convert_params(&params);
        let param_refs: Vec<&dyn ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();

        conn.execute(&statement, param_refs.as_slice())
            .map(|rows| rows as u64)
            .map_err(|e| format!("SQL Error: {}", e))
    }

    fn query_json(
        &mut self,
        statement: String,
        params: Vec<api::sql::DbValue>,
    ) -> Result<String, String> {
        // 查询操作在所有模式下均允许
        let conn = self.registry.get_conn().map_err(|e| e.to_string())?;

        let sql_params = convert_params(&params);
        let param_refs: Vec<&dyn ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();

        let mut stmt = conn.prepare(&statement).map_err(|e| e.to_string())?;

        let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = stmt
            .query(param_refs.as_slice())
            .map_err(|e| e.to_string())?;

        let mut rows_json = Vec::new();
        while let Ok(Some(row)) = rows.next() {
            let mut obj = Map::new();
            for (i, col_name) in col_names.iter().enumerate() {
                let json_val = match row.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => Value::Null,
                    rusqlite::types::ValueRef::Integer(n) => Value::Number(n.into()),
                    rusqlite::types::ValueRef::Real(f) => serde_json::Number::from_f64(f)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                    rusqlite::types::ValueRef::Text(t) => {
                        Value::String(String::from_utf8_lossy(t).into_owned())
                    }
                    rusqlite::types::ValueRef::Blob(_) => Value::String("<BLOB>".into()),
                };
                obj.insert(col_name.clone(), json_val);
            }
            rows_json.push(Value::Object(obj));
        }

        serde_json::to_string(&rows_json).map_err(|e| e.to_string())
    }
}

/// 工具函数：将插件传入的参数类型转换为 rusqlite 支持的 ToSql trait 对象
fn convert_params(params: &[api::sql::DbValue]) -> Vec<Box<dyn ToSql>> {
    params
        .iter()
        .map(|p| match p {
            api::sql::DbValue::Text(s) => Box::new(s.clone()) as Box<dyn ToSql>,
            api::sql::DbValue::Integer(i) => Box::new(*i),
            api::sql::DbValue::Real(f) => Box::new(*f),
            api::sql::DbValue::NullVal => Box::new(rusqlite::types::Null),
        })
        .collect()
}
