use rusqlite::types::ToSql;
use std::io::{Cursor, Read, Seek, SeekFrom};
use wasmtime::component::Resource;
use serde_json::{Map, Value};

use crate::common::buffer::{BufferType, RealBuffer};
use crate::runtime::context::StreamContext;

// 生成 WIT 绑定
wasmtime::component::bindgen!({
    path: "../vtx-sdk/wit/vtx.wit",
    world: "plugin",
    with: {
        "vtx:api/stream-io/buffer": crate::common::buffer::RealBuffer,
    }
});

pub use vtx::api;

/// 内部辅助 Trait：统一处理文件与内存流的 IO 操作
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

/// 实现插件与宿主之间的 Stream IO 文件读写能力
impl vtx::api::stream_io::Host for StreamContext {
    fn open_file(&mut self, uuid: String) -> Result<Resource<RealBuffer>, String> {
        let file_path = self.registry.get_path(&uuid).ok_or_else(|| "UUID not found".to_string())?;
        let file = std::fs::File::open(&file_path).map_err(|e| format!("IO Error: {}", e))?;

        let rb = RealBuffer {
            inner: BufferType::File(file),
            path_hint: Some(file_path),
            mime_override: None,
        };
        // ResourceTable::push 返回的是 ResourceTableError，需转为 String
        self.table.push(rb).map_err(|e| format!("Resource Table Error: {}", e))
    }

    fn create_memory_buffer(&mut self, data: Vec<u8>) -> Resource<RealBuffer> {
        let rb = RealBuffer {
            inner: BufferType::Memory(Cursor::new(data)),
            path_hint: None,
            mime_override: Some("application/json".to_string()),
        };
        self.table.push(rb).expect("Critical: Failed to allocate buffer resource")
    }
}

/// 实现 buffer 的读取、大小获取与销毁
impl vtx::api::stream_io::HostBuffer for StreamContext {
    fn size(&mut self, resource: Resource<RealBuffer>) -> u64 {
        self.table.get(&resource)
            .map(|b| b.inner.get_size())
            .unwrap_or(0)
    }

    fn read(&mut self, resource: Resource<RealBuffer>, offset: u64, max_bytes: u64) -> Vec<u8> {
        let mut chunk = vec![0u8; max_bytes as usize];
        // 获取可变引用进行读取
        let read_len = if let Ok(buffer) = self.table.get_mut(&resource) {
            buffer.inner.read_at(offset, &mut chunk)
        } else {
            0
        };

        chunk.truncate(read_len);
        chunk
    }

    fn drop(&mut self, resource: Resource<RealBuffer>) -> wasmtime::Result<()> {
        // 关键修复：显式映射错误类型，确保符合函数签名的 Result 类型
        self.table.delete(resource)
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!(e))
    }
}

/// 实现 SQL 执行接口
impl vtx::api::sql::Host for StreamContext {
    fn execute(
        &mut self,
        statement: String,
        params: Vec<vtx::api::sql::DbValue>,
    ) -> Result<u64, String> {
        let conn = self.registry.get_conn().map_err(|e| e.to_string())?;
        let sql_params = convert_params(&params);
        let param_refs: Vec<&dyn ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();

        conn.execute(&statement, param_refs.as_slice())
            .map(|rows| rows as u64)
            .map_err(|e| format!("SQL Execution Error: {}", e))
    }

    fn query_json(
        &mut self,
        statement: String,
        params: Vec<vtx::api::sql::DbValue>,
    ) -> Result<String, String> {
        let conn = self.registry.get_conn().map_err(|e| e.to_string())?;
        let sql_params = convert_params(&params);
        let param_refs: Vec<&dyn ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();

        let mut stmt = conn.prepare(&statement).map_err(|e| e.to_string())?;
        let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

        let mut rows_json = Vec::new();
        let mut rows = stmt.query(param_refs.as_slice()).map_err(|e| e.to_string())?;

        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let mut obj = Map::new();
            for (i, col_name) in col_names.iter().enumerate() {
                let json_val = match row.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => Value::Null,
                    rusqlite::types::ValueRef::Integer(n) => Value::Number(n.into()),
                    rusqlite::types::ValueRef::Real(f) => serde_json::Number::from_f64(f)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                    rusqlite::types::ValueRef::Text(t) => Value::String(String::from_utf8_lossy(t).into_owned()),
                    rusqlite::types::ValueRef::Blob(_) => Value::String("<BLOB>".into()),
                };
                obj.insert(col_name.clone(), json_val);
            }
            rows_json.push(Value::Object(obj));
        }

        serde_json::to_string(&rows_json).map_err(|e| e.to_string())
    }
}

/// 辅助函数：转换 Wasm 类型到 SQLite 参数
fn convert_params(params: &[vtx::api::sql::DbValue]) -> Vec<Box<dyn ToSql>> {
    params.iter().map(|p| match p {
        api::sql::DbValue::Text(s) => Box::new(s.clone()) as Box<dyn ToSql>,
        api::sql::DbValue::Integer(i) => Box::new(*i),
        api::sql::DbValue::Real(f) => Box::new(*f),
        api::sql::DbValue::NullVal => Box::new(rusqlite::types::Null),
    }).collect()
}